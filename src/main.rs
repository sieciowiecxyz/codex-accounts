use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{Local, TimeZone};
use clap::{Args, Parser, Subcommand};
use rayon::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use tempfile::Builder;
use tungstenite::{client, Message, WebSocket};

const CACHE_TTL_SECS: i64 = 45;
const PROBE_TIMEOUT_SECS: u64 = 8;
const MAX_CONCURRENCY: usize = 4;

#[derive(Parser, Debug)]
#[command(name = "codex-accounts")]
#[command(about = "Show usage across Codex auth files and switch accounts quickly")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    List(ListArgs),
    Use(UseArgs),
    UseBest(UseBestArgs),
    ImportNew,
    Doctor,
}

#[derive(Args, Debug, Clone)]
struct ListArgs {
    #[arg(long)]
    json: bool,
    #[arg(long)]
    refresh: bool,
}

#[derive(Args, Debug)]
struct UseArgs {
    selector: String,
}

#[derive(Args, Debug)]
struct UseBestArgs {
    #[arg(long)]
    dry_run: bool,
    #[arg(long, default_value_t = 0.0)]
    min_primary_left: f64,
}

#[derive(Debug, Clone)]
struct AppContext {
    codex_root: PathBuf,
    accounts_root: PathBuf,
    tmp_root: PathBuf,
    cache_path: PathBuf,
}

#[derive(Debug, Clone)]
struct AuthFile {
    path: PathBuf,
    bytes: Vec<u8>,
    is_current: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedProbeResults {
    generated_at: i64,
    results: Vec<AccountProbeResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountProbeResult {
    auth_file: String,
    auth_path: PathBuf,
    is_current: bool,
    account_label: String,
    email: Option<String>,
    plan_type: Option<String>,
    primary: Option<WindowSummary>,
    secondary: Option<WindowSummary>,
    credits: Option<CreditsSummary>,
    status: ProbeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WindowSummary {
    used_percent: f64,
    left_percent: f64,
    window_duration_mins: Option<u64>,
    resets_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreditsSummary {
    has_credits: bool,
    unlimited: bool,
    balance: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ProbeStatus {
    Ok,
    Error(String),
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    codex_root: PathBuf,
    accounts_root: PathBuf,
    codex_path: Option<PathBuf>,
    codex_version: Option<String>,
    auth_files_found: usize,
    cache_path: PathBuf,
}

#[derive(Debug, Serialize)]
struct RpcRequest<'a, T> {
    id: u64,
    method: &'a str,
    params: T,
}

#[derive(Debug, Deserialize)]
struct AccountReadResult {
    account: Option<AccountInfo>,
    #[allow(dead_code)]
    #[serde(rename = "requiresOpenaiAuth")]
    requires_openai_auth: bool,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum AccountInfo {
    #[serde(rename = "apiKey")]
    ApiKey,
    #[serde(rename = "chatgpt")]
    Chatgpt {
        email: String,
        #[serde(rename = "planType")]
        plan_type: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
struct RateLimitReadResult {
    #[serde(rename = "rateLimits")]
    rate_limits: RateLimitSnapshot,
    #[allow(dead_code)]
    #[serde(rename = "rateLimitsByLimitId")]
    rate_limits_by_limit_id: Option<BTreeMap<String, RateLimitSnapshot>>,
}

#[derive(Debug, Clone, Deserialize)]
struct RateLimitSnapshot {
    #[allow(dead_code)]
    #[serde(rename = "limitId")]
    limit_id: Option<String>,
    #[allow(dead_code)]
    #[serde(rename = "limitName")]
    limit_name: Option<String>,
    primary: Option<RateLimitWindow>,
    secondary: Option<RateLimitWindow>,
    credits: Option<CreditsSnapshot>,
    #[allow(dead_code)]
    #[serde(rename = "planType")]
    plan_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RateLimitWindow {
    #[serde(rename = "usedPercent")]
    used_percent: f64,
    #[serde(rename = "windowDurationMins")]
    window_duration_mins: Option<u64>,
    #[serde(rename = "resetsAt")]
    resets_at: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct CreditsSnapshot {
    #[serde(rename = "hasCredits")]
    has_credits: bool,
    unlimited: bool,
    balance: Option<String>,
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn spawn(app_server_url: &str, codex_home: &Path) -> Result<Self> {
        let child = Command::new("codex")
            .arg("app-server")
            .arg("--listen")
            .arg(app_server_url)
            .env("CODEX_HOME", codex_home)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("failed to start codex app-server for {}", codex_home.display()))?;
        Ok(Self { child })
    }

    fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>> {
        self.child.try_wait().context("failed to inspect codex app-server status")
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let ctx = app_context()?;

    match cli.command.unwrap_or(Commands::List(ListArgs { json: false, refresh: false })) {
        Commands::List(args) => {
            let results = load_or_probe(&ctx, args.refresh)?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&results)?);
            } else {
                print_table(&results);
            }
        }
        Commands::Use(args) => {
            let results = load_or_probe(&ctx, false)?;
            let selected = resolve_selector(&results, &args.selector)?;
            switch_to(&ctx, selected)?;
            clear_cache(&ctx)?;
            println!(
                "Switched to {} ({})",
                selected.auth_file,
                selected.account_label
            );
        }
        Commands::UseBest(args) => {
            let results = probe_accounts(&ctx)?;
            save_cache(&ctx, &results)?;
            let selected = select_best(&results, args.min_primary_left)?;
            if args.dry_run {
                println!(
                    "{} -> {} | 5h left {} | weekly left {}",
                    selected.auth_file,
                    selected.account_label,
                    format_left(selected.primary.as_ref()),
                    format_left(selected.secondary.as_ref())
                );
            } else {
                switch_to(&ctx, selected)?;
                clear_cache(&ctx)?;
                println!(
                    "Switched to best account: {} ({})",
                    selected.auth_file,
                    selected.account_label
                );
            }
        }
        Commands::Doctor => {
            let auth_files = discover_auth_files(&ctx)?;
            let report = DoctorReport {
                codex_root: ctx.codex_root.clone(),
                accounts_root: ctx.accounts_root.clone(),
                codex_path: find_codex_path(),
                codex_version: codex_version(),
                auth_files_found: auth_files.len(),
                cache_path: ctx.cache_path.clone(),
            };
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::ImportNew => {
            let stored_path = import_new_account(&ctx)?;
            clear_cache(&ctx)?;
            println!("Imported account into {}", stored_path.display());
        }
    }

    Ok(())
}

fn app_context() -> Result<AppContext> {
    let codex_root = match std::env::var_os("CODEX_HOME") {
        Some(value) => PathBuf::from(value),
        None => dirs::home_dir()
            .map(|home| home.join(".codex"))
            .ok_or_else(|| anyhow!("failed to determine home directory"))?,
    };
    let accounts_root = codex_root.join("accounts");
    let tmp_root = codex_root.join("tmp").join("codex-accounts");
    let cache_path = tmp_root.join("cache.json");
    fs::create_dir_all(&accounts_root)
        .with_context(|| format!("failed to create {}", accounts_root.display()))?;
    fs::create_dir_all(&tmp_root)
        .with_context(|| format!("failed to create {}", tmp_root.display()))?;
    Ok(AppContext {
        codex_root,
        accounts_root,
        tmp_root,
        cache_path,
    })
}

fn load_or_probe(ctx: &AppContext, refresh: bool) -> Result<Vec<AccountProbeResult>> {
    if !refresh {
        if let Some(cached) = load_cache(ctx)? {
            return Ok(cached);
        }
    }

    let results = probe_accounts(ctx)?;
    save_cache(ctx, &results)?;
    Ok(results)
}

fn load_cache(ctx: &AppContext) -> Result<Option<Vec<AccountProbeResult>>> {
    if !ctx.cache_path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(&ctx.cache_path)
        .with_context(|| format!("failed to read {}", ctx.cache_path.display()))?;
    let cache: CachedProbeResults = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse {}", ctx.cache_path.display()))?;
    let age = chrono::Utc::now().timestamp() - cache.generated_at;
    if age <= CACHE_TTL_SECS {
        Ok(Some(cache.results))
    } else {
        Ok(None)
    }
}

fn save_cache(ctx: &AppContext, results: &[AccountProbeResult]) -> Result<()> {
    let payload = CachedProbeResults {
        generated_at: chrono::Utc::now().timestamp(),
        results: results.to_vec(),
    };
    let text = serde_json::to_string_pretty(&payload)?;
    fs::write(&ctx.cache_path, text)
        .with_context(|| format!("failed to write {}", ctx.cache_path.display()))?;
    Ok(())
}

fn clear_cache(ctx: &AppContext) -> Result<()> {
    if ctx.cache_path.exists() {
        fs::remove_file(&ctx.cache_path)
            .with_context(|| format!("failed to remove {}", ctx.cache_path.display()))?;
    }
    Ok(())
}

fn discover_auth_files(ctx: &AppContext) -> Result<Vec<AuthFile>> {
    let current_auth_bytes = fs::read(ctx.codex_root.join("auth.json")).ok();
    let mut files = Vec::new();
    collect_auth_files_from_dir(
        &ctx.accounts_root,
        &current_auth_bytes,
        &mut files,
        |file_name| file_name.ends_with(".json"),
    )?;
    collect_auth_files_from_dir(
        &ctx.codex_root,
        &current_auth_bytes,
        &mut files,
        |file_name| file_name.starts_with("auth") && file_name.ends_with(".json"),
    )?;
    files.sort_by(|a, b| {
        auth_source_rank(&a.path, &ctx.accounts_root)
            .cmp(&auth_source_rank(&b.path, &ctx.accounts_root))
            .then_with(|| a.path.file_name().cmp(&b.path.file_name()))
    });

    let mut deduped: Vec<AuthFile> = Vec::new();
    'outer: for file in files {
        for existing in &mut deduped {
            if existing.bytes == file.bytes {
                existing.is_current |= file.is_current;
                continue 'outer;
            }
        }
        deduped.push(file);
    }

    Ok(deduped)
}

fn collect_auth_files_from_dir<F>(
    dir: &Path,
    current_auth_bytes: &Option<Vec<u8>>,
    sink: &mut Vec<AuthFile>,
    predicate: F,
) -> Result<()>
where
    F: Fn(&str) -> bool,
{
    if !dir.exists() {
        return Ok(());
    }

    let entries = fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !predicate(file_name) {
            continue;
        }
        let bytes =
            fs::read(&path).with_context(|| format!("failed to read auth file {}", path.display()))?;
        let is_current = current_auth_bytes
            .as_ref()
            .map(|current| current == &bytes)
            .unwrap_or_else(|| file_name == "auth.json");
        sink.push(AuthFile {
            path,
            bytes,
            is_current,
        });
    }

    Ok(())
}

fn auth_source_rank(path: &Path, accounts_root: &Path) -> u8 {
    if path.starts_with(accounts_root) {
        0
    } else if auth_file_name(path) == "auth.json" {
        2
    } else {
        1
    }
}

fn probe_accounts(ctx: &AppContext) -> Result<Vec<AccountProbeResult>> {
    let auth_files = discover_auth_files(ctx)?;
    if auth_files.is_empty() {
        bail!("no auth*.json files found in {}", ctx.codex_root.display());
    }

    let concurrency = auth_files.len().min(MAX_CONCURRENCY).max(1);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(concurrency)
        .build()
        .context("failed to build probe thread pool")?;

    let mut results = pool.install(|| {
        auth_files
            .par_iter()
            .map(|auth_file| probe_single_auth(ctx, auth_file))
            .collect::<Vec<_>>()
    });

    results.sort_by(compare_results);
    Ok(results)
}

fn probe_single_auth(ctx: &AppContext, auth_file: &AuthFile) -> AccountProbeResult {
    match probe_single_auth_inner(ctx, auth_file) {
        Ok(result) => result,
        Err(err) => AccountProbeResult {
            auth_file: auth_file_name(&auth_file.path),
            auth_path: auth_file.path.clone(),
            is_current: auth_file.is_current,
            account_label: "<unavailable>".to_string(),
            email: None,
            plan_type: None,
            primary: None,
            secondary: None,
            credits: None,
            status: ProbeStatus::Error(err.to_string()),
        },
    }
}

fn probe_single_auth_inner(ctx: &AppContext, auth_file: &AuthFile) -> Result<AccountProbeResult> {
    let temp_dir = Builder::new()
        .prefix("probe-")
        .tempdir_in(&ctx.tmp_root)
        .with_context(|| format!("failed to create temp dir in {}", ctx.tmp_root.display()))?;
    fs::write(temp_dir.path().join("auth.json"), &auth_file.bytes).with_context(|| {
        format!(
            "failed to write temp auth for {} into {}",
            auth_file.path.display(),
            temp_dir.path().display()
        )
    })?;

    let config_path = ctx.codex_root.join("config.toml");
    if config_path.exists() {
        let _ = fs::copy(&config_path, temp_dir.path().join("config.toml"));
    }

    let port = reserve_port()?;
    let app_server_url = format!("ws://127.0.0.1:{port}");
    let mut child = ChildGuard::spawn(&app_server_url, temp_dir.path())?;
    let mut websocket = connect_when_ready(port, &mut child)?;

    let _: Value = rpc_request(
        &mut websocket,
        1,
        "initialize",
        json!({
            "clientInfo": {
                "name": "codex-accounts",
                "title": null,
                "version": env!("CARGO_PKG_VERSION"),
            },
            "capabilities": null
        }),
    )?;
    let account: AccountReadResult = rpc_request(&mut websocket, 2, "account/read", json!({}))?;
    let rate_limits: RateLimitReadResult =
        rpc_request(&mut websocket, 3, "account/rateLimits/read", Value::Null)?;

    let (account_label, email, plan_type) = match account.account {
        Some(AccountInfo::ApiKey) => ("apiKey".to_string(), None, None),
        Some(AccountInfo::Chatgpt { email, plan_type }) => {
            (email.clone(), Some(email), plan_type)
        }
        None => ("<unknown>".to_string(), None, None),
    };

    let _ = websocket.close(None);

    Ok(AccountProbeResult {
        auth_file: auth_file_name(&auth_file.path),
        auth_path: auth_file.path.clone(),
        is_current: auth_file.is_current,
        account_label,
        email,
        plan_type,
        primary: rate_limits.rate_limits.primary.map(window_summary),
        secondary: rate_limits.rate_limits.secondary.map(window_summary),
        credits: rate_limits.rate_limits.credits.map(|credits| CreditsSummary {
            has_credits: credits.has_credits,
            unlimited: credits.unlimited,
            balance: credits.balance,
        }),
        status: ProbeStatus::Ok,
    })
}

fn reserve_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).context("failed to reserve localhost port")?;
    let port = listener
        .local_addr()
        .context("failed to inspect reserved localhost port")?
        .port();
    drop(listener);
    Ok(port)
}

fn connect_when_ready(port: u16, child: &mut ChildGuard) -> Result<WebSocket<TcpStream>> {
    let started = Instant::now();
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let url = format!("ws://127.0.0.1:{port}");
    let mut last_error: Option<anyhow::Error> = None;

    while started.elapsed() < Duration::from_secs(PROBE_TIMEOUT_SECS) {
        if let Some(status) = child.try_wait()? {
            bail!("codex app-server exited early with status {status}");
        }

        match TcpStream::connect_timeout(&address, Duration::from_millis(200)) {
            Ok(stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(PROBE_TIMEOUT_SECS)))
                    .context("failed to set websocket read timeout")?;
                stream
                    .set_write_timeout(Some(Duration::from_secs(PROBE_TIMEOUT_SECS)))
                    .context("failed to set websocket write timeout")?;
                match client(url.as_str(), stream) {
                    Ok((websocket, _)) => return Ok(websocket),
                    Err(err) => last_error = Some(anyhow!(err)),
                }
            }
            Err(err) => last_error = Some(anyhow!(err)),
        }

        thread::sleep(Duration::from_millis(100));
    }

    Err(last_error.unwrap_or_else(|| anyhow!("timed out waiting for codex app-server")))
}

fn rpc_request<P, R>(websocket: &mut WebSocket<TcpStream>, id: u64, method: &str, params: P) -> Result<R>
where
    P: Serialize,
    R: DeserializeOwned,
{
    let request = RpcRequest { id, method, params };
    websocket
        .send(Message::Text(serde_json::to_string(&request)?))
        .with_context(|| format!("failed to send RPC request {method}"))?;

    loop {
        let message = websocket.read().with_context(|| format!("failed to read RPC response for {method}"))?;
        match message {
            Message::Text(text) => {
                let value: Value = serde_json::from_str(&text)
                    .with_context(|| format!("invalid JSON response while waiting for {method}"))?;
                match value.get("id").and_then(Value::as_u64) {
                    Some(response_id) if response_id == id => {
                        if let Some(result) = value.get("result") {
                            return serde_json::from_value(result.clone())
                                .with_context(|| format!("invalid RPC payload for {method}"));
                        }
                        if let Some(error) = value.get("error") {
                            bail!("RPC {method} failed: {error}");
                        }
                    }
                    _ => {}
                }
            }
            Message::Ping(data) => {
                websocket.send(Message::Pong(data))?;
            }
            Message::Close(frame) => {
                bail!("websocket closed while waiting for {method}: {:?}", frame);
            }
            _ => {}
        }
    }
}

fn window_summary(window: RateLimitWindow) -> WindowSummary {
    let left_percent = (100.0 - window.used_percent).max(0.0);
    WindowSummary {
        used_percent: window.used_percent,
        left_percent,
        window_duration_mins: window.window_duration_mins,
        resets_at: window.resets_at,
    }
}

fn compare_results(left: &AccountProbeResult, right: &AccountProbeResult) -> Ordering {
    compare_status(left, right)
        .then_with(|| compare_window(left.primary.as_ref(), right.primary.as_ref()))
        .then_with(|| compare_window(left.secondary.as_ref(), right.secondary.as_ref()))
        .then_with(|| left.auth_file.cmp(&right.auth_file))
}

fn compare_status(left: &AccountProbeResult, right: &AccountProbeResult) -> Ordering {
    match (&left.status, &right.status) {
        (ProbeStatus::Ok, ProbeStatus::Error(_)) => Ordering::Less,
        (ProbeStatus::Error(_), ProbeStatus::Ok) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

fn compare_window(left: Option<&WindowSummary>, right: Option<&WindowSummary>) -> Ordering {
    let left_value = left.map(|item| item.left_percent).unwrap_or(-1.0);
    let right_value = right.map(|item| item.left_percent).unwrap_or(-1.0);
    right_value
        .partial_cmp(&left_value)
        .unwrap_or(Ordering::Equal)
}

fn print_table(results: &[AccountProbeResult]) {
    let headers = [
        "auth",
        "account",
        "plan",
        "5h left",
        "5h reset",
        "weekly left",
        "weekly reset",
        "credits",
        "status",
    ];

    let mut rows = Vec::with_capacity(results.len());
    for result in results {
        rows.push(vec![
            if result.is_current {
                format!("{} *", result.auth_file)
            } else {
                result.auth_file.clone()
            },
            result.account_label.clone(),
            result.plan_type.clone().unwrap_or_else(|| "-".to_string()),
            format_left(result.primary.as_ref()),
            format_reset(result.primary.as_ref()),
            format_left(result.secondary.as_ref()),
            format_reset(result.secondary.as_ref()),
            format_credits(result.credits.as_ref()),
            format_status(&result.status),
        ]);
    }

    let mut widths = headers.map(str::len);
    for row in &rows {
        for (idx, value) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(value.len());
        }
    }

    println!(
        "{:<w0$}  {:<w1$}  {:<w2$}  {:>w3$}  {:<w4$}  {:>w5$}  {:<w6$}  {:<w7$}  {:<w8$}",
        headers[0],
        headers[1],
        headers[2],
        headers[3],
        headers[4],
        headers[5],
        headers[6],
        headers[7],
        headers[8],
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
        w3 = widths[3],
        w4 = widths[4],
        w5 = widths[5],
        w6 = widths[6],
        w7 = widths[7],
        w8 = widths[8],
    );

    for row in rows {
        println!(
            "{:<w0$}  {:<w1$}  {:<w2$}  {:>w3$}  {:<w4$}  {:>w5$}  {:<w6$}  {:<w7$}  {:<w8$}",
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            w0 = widths[0],
            w1 = widths[1],
            w2 = widths[2],
            w3 = widths[3],
            w4 = widths[4],
            w5 = widths[5],
            w6 = widths[6],
            w7 = widths[7],
            w8 = widths[8],
        );
    }
}

fn format_left(window: Option<&WindowSummary>) -> String {
    match window {
        Some(window) => format!("{:.0}%", window.left_percent),
        None => "-".to_string(),
    }
}

fn format_reset(window: Option<&WindowSummary>) -> String {
    let Some(window) = window else {
        return "-".to_string();
    };
    let Some(timestamp) = window.resets_at else {
        return "-".to_string();
    };
    match Local.timestamp_opt(timestamp, 0).single() {
        Some(time) => time.format("%d %b %H:%M").to_string(),
        None => "-".to_string(),
    }
}

fn format_credits(credits: Option<&CreditsSummary>) -> String {
    match credits {
        Some(credits) if credits.unlimited => "unlimited".to_string(),
        Some(credits) if credits.has_credits => credits.balance.clone().unwrap_or_else(|| "yes".to_string()),
        Some(_) => "-".to_string(),
        None => "-".to_string(),
    }
}

fn format_status(status: &ProbeStatus) -> String {
    match status {
        ProbeStatus::Ok => "ok".to_string(),
        ProbeStatus::Error(err) => truncate(err, 48),
    }
}

fn truncate(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        text.to_string()
    } else {
        format!("{}...", &text[..max_len.saturating_sub(3)])
    }
}

fn resolve_selector<'a>(results: &'a [AccountProbeResult], selector: &str) -> Result<&'a AccountProbeResult> {
    let selector_lower = selector.to_lowercase();

    let exact_matches: Vec<_> = results
        .iter()
        .filter(|result| {
            result.auth_file.eq_ignore_ascii_case(selector)
                || auth_stem(&result.auth_file).eq_ignore_ascii_case(selector)
                || result
                    .email
                    .as_deref()
                    .map(|email| email.eq_ignore_ascii_case(selector))
                    .unwrap_or(false)
        })
        .collect();
    if exact_matches.len() == 1 {
        return Ok(exact_matches[0]);
    }
    if exact_matches.len() > 1 {
        bail!("selector is ambiguous; use a file name");
    }

    let partial_matches: Vec<_> = results
        .iter()
        .filter(|result| {
            result.auth_file.to_lowercase().contains(&selector_lower)
                || auth_stem(&result.auth_file).to_lowercase().contains(&selector_lower)
                || result
                    .email
                    .as_deref()
                    .map(|email| email.to_lowercase().contains(&selector_lower))
                    .unwrap_or(false)
        })
        .collect();

    match partial_matches.len() {
        1 => Ok(partial_matches[0]),
        0 => bail!("no auth file matched selector {selector}"),
        _ => bail!("selector is ambiguous; use a more specific file name"),
    }
}

fn select_best(results: &[AccountProbeResult], min_primary_left: f64) -> Result<&AccountProbeResult> {
    results
        .iter()
        .filter(|result| matches!(result.status, ProbeStatus::Ok))
        .filter(|result| result.primary.as_ref().map(|item| item.left_percent).unwrap_or(0.0) >= min_primary_left)
        .max_by(|left, right| {
            left.primary
                .as_ref()
                .map(|item| item.left_percent)
                .unwrap_or(0.0)
                .partial_cmp(
                    &right
                        .primary
                        .as_ref()
                        .map(|item| item.left_percent)
                        .unwrap_or(0.0),
                )
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    left.secondary
                        .as_ref()
                        .map(|item| item.left_percent)
                        .unwrap_or(0.0)
                        .partial_cmp(
                            &right
                                .secondary
                                .as_ref()
                                .map(|item| item.left_percent)
                                .unwrap_or(0.0),
                        )
                        .unwrap_or(Ordering::Equal)
                })
        })
        .ok_or_else(|| anyhow!("no account satisfied the requested minimum primary limit"))
}

fn switch_to(ctx: &AppContext, selected: &AccountProbeResult) -> Result<()> {
    let bytes = fs::read(&selected.auth_path)
        .with_context(|| format!("failed to read {}", selected.auth_path.display()))?;
    let destination = ctx.codex_root.join("auth.json");
    let temp_path = ctx.codex_root.join("auth.json.tmp");

    let mut temp_file = fs::File::create(&temp_path)
        .with_context(|| format!("failed to create {}", temp_path.display()))?;
    temp_file
        .write_all(&bytes)
        .with_context(|| format!("failed to write {}", temp_path.display()))?;
    temp_file
        .sync_all()
        .with_context(|| format!("failed to flush {}", temp_path.display()))?;
    fs::rename(&temp_path, &destination).with_context(|| {
        format!(
            "failed to replace {} with {}",
            destination.display(),
            selected.auth_path.display()
        )
    })?;
    Ok(())
}

fn import_new_account(ctx: &AppContext) -> Result<PathBuf> {
    let auth_path = ctx.codex_root.join("auth.json");
    let backup_path = ctx.tmp_root.join("auth.import-backup.json");

    if backup_path.exists() {
        fs::remove_file(&backup_path)
            .with_context(|| format!("failed to remove stale backup {}", backup_path.display()))?;
    }

    if auth_path.exists() {
        fs::copy(&auth_path, &backup_path).with_context(|| {
            format!(
                "failed to back up {} into {}",
                auth_path.display(),
                backup_path.display()
            )
        })?;
        fs::remove_file(&auth_path)
            .with_context(|| format!("failed to remove {}", auth_path.display()))?;
    }

    let status = Command::new("codex")
        .current_dir(&ctx.codex_root)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .context("failed to launch interactive codex login")?;

    if !auth_path.exists() {
        restore_auth_backup(&backup_path, &auth_path)?;
        bail!(
            "Codex exited with status {} and did not leave a new auth.json",
            status
        );
    }

    let bytes =
        fs::read(&auth_path).with_context(|| format!("failed to read {}", auth_path.display()))?;
    let imported = AuthFile {
        path: auth_path.clone(),
        bytes: bytes.clone(),
        is_current: true,
    };
    let probe = probe_single_auth_inner(ctx, &imported)?;
    let stored_path = store_account_auth(ctx, &probe, &bytes)?;

    if backup_path.exists() {
        fs::remove_file(&backup_path)
            .with_context(|| format!("failed to remove {}", backup_path.display()))?;
    }

    Ok(stored_path)
}

fn restore_auth_backup(backup_path: &Path, auth_path: &Path) -> Result<()> {
    if backup_path.exists() {
        fs::rename(backup_path, auth_path).with_context(|| {
            format!(
                "failed to restore {} back to {}",
                backup_path.display(),
                auth_path.display()
            )
        })?;
    }
    Ok(())
}

fn store_account_auth(ctx: &AppContext, probe: &AccountProbeResult, bytes: &[u8]) -> Result<PathBuf> {
    let existing = discover_auth_files(ctx)?;
    if let Some(found) = existing
        .iter()
        .find(|entry| entry.path.starts_with(&ctx.accounts_root) && entry.bytes == bytes)
    {
        return Ok(found.path.clone());
    }

    let label = probe.email.as_deref().unwrap_or(&probe.account_label);
    let base_name = sanitize_account_name(label);
    let mut candidate = ctx.accounts_root.join(format!("{base_name}.json"));
    let mut suffix = 2u32;

    while candidate.exists() {
        let existing_bytes = fs::read(&candidate)
            .with_context(|| format!("failed to read {}", candidate.display()))?;
        if existing_bytes == bytes {
            return Ok(candidate);
        }
        candidate = ctx.accounts_root.join(format!("{base_name}-{suffix}.json"));
        suffix += 1;
    }

    fs::write(&candidate, bytes)
        .with_context(|| format!("failed to write {}", candidate.display()))?;
    Ok(candidate)
}

fn sanitize_account_name(input: &str) -> String {
    let mut out = String::new();
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
            out.push(ch.to_ascii_lowercase());
        } else if ch == '@' {
            out.push_str("_at_");
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_').trim_matches('.');
    if trimmed.is_empty() {
        "account".to_string()
    } else {
        trimmed.to_string()
    }
}

fn auth_file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("<unknown>")
        .to_string()
}

fn auth_stem(file_name: &str) -> &str {
    file_name.strip_suffix(".json").unwrap_or(file_name)
}

fn find_codex_path() -> Option<PathBuf> {
    let output = Command::new("which").arg("codex").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(PathBuf::from(text))
    }
}

fn codex_version() -> Option<String> {
    let output = Command::new("codex").arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}
