use std::cmp::Ordering;
use std::fs;
use std::io::{BufRead, IsTerminal, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{Local, TimeZone};
use clap::{Args, Parser, Subcommand};
use console::{pad_str, Alignment, Style, Term};
use dialoguer::{theme::ColorfulTheme, Select};
use rayon::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use tempfile::Builder;
use tungstenite::{client, Message, WebSocket};

const CACHE_TTL_SECS: i64 = 45;
const PROBE_TIMEOUT_SECS: u64 = 30;
const MAX_CONCURRENCY: usize = 4;
const USE_BEST_LOCK_TTL_SECS: u64 = 300;
const FIVE_HOUR_WINDOW_MINS: u64 = 5 * 60;
const WEEKLY_WINDOW_MINS: u64 = 7 * 24 * 60;
const ANSI_RESET: &str = "\x1b[0m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_YELLOW: &str = "\x1b[33m";
const ANSI_RED: &str = "\x1b[31m";
const ANSI_BOLD_GREEN: &str = "\x1b[1;32m";
const ANSI_BOLD_CYAN: &str = "\x1b[1;36m";

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
    Use(SelectorArgs),
    /// Interactively select an account and switch after confirmation.
    Switch(SwitchArgs),
    UseBest(UseBestArgs),
    ImportNew,
    Remove(SelectorArgs),
}

#[derive(Args, Debug, Clone)]
struct ListArgs {
    #[arg(long)]
    json: bool,
    #[arg(long)]
    refresh: bool,
}

#[derive(Args, Debug)]
struct SelectorArgs {
    selector: String,
}

#[derive(Args, Debug, Clone)]
struct SwitchArgs {
    /// Refresh usage before showing the account list.
    #[arg(long)]
    refresh: bool,
}

#[derive(Args, Debug)]
struct UseBestArgs {
    #[arg(long)]
    dry_run: bool,
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
    five_hour: Option<WindowSummary>,
    weekly: Option<WindowSummary>,
    reset_credits: Option<u64>,
    status: ProbeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WindowSummary {
    left_percent: f64,
    resets_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UseBestLockState {
    created_at: i64,
    expires_at: i64,
    pid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ProbeStatus {
    Ok,
    Error(String),
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
    #[serde(rename = "rateLimitResetCredits")]
    reset_credits: Option<RateLimitResetCredits>,
}

#[derive(Debug, Clone, Deserialize)]
struct RateLimitResetCredits {
    #[serde(rename = "availableCount")]
    available_count: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct RateLimitSnapshot {
    primary: Option<RateLimitWindow>,
    secondary: Option<RateLimitWindow>,
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
            .with_context(|| {
                format!(
                    "failed to start codex app-server for {}",
                    codex_home.display()
                )
            })?;
        Ok(Self { child })
    }

    fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>> {
        self.child
            .try_wait()
            .context("failed to inspect codex app-server status")
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

    match cli.command.unwrap_or(Commands::List(ListArgs {
        json: false,
        refresh: false,
    })) {
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
                selected.auth_file, selected.account_label
            );
        }
        Commands::Switch(args) => {
            run_interactive_switch(&ctx, &args)?;
        }
        Commands::Remove(args) => {
            let results = load_or_probe(&ctx, false)?;
            let selected = resolve_selector(&results, &args.selector)?;
            remove_selected(selected)?;
            clear_cache(&ctx)?;
            println!(
                "Removed account: {} ({})",
                selected.auth_file, selected.account_label
            );
        }
        Commands::UseBest(args) => {
            let results = probe_accounts(&ctx)?;
            save_cache(&ctx, &results)?;
            let selected = select_best_candidate(&ctx, &results, !args.dry_run)?;
            if args.dry_run {
                print_use_best_preview(selected);
            } else {
                switch_to(&ctx, selected)?;
                clear_cache(&ctx)?;
                println!(
                    "Switched to best account: {} ({})",
                    selected.auth_file, selected.account_label
                );
            }
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
    let cache_path = tmp_root.join("cache-v4.json");
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
        let bytes = fs::read(&path)
            .with_context(|| format!("failed to read auth file {}", path.display()))?;
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

    let concurrency = auth_files.len().clamp(1, MAX_CONCURRENCY);
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
            five_hour: None,
            weekly: None,
            reset_credits: None,
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
    let (account_label, email, plan_type) = match account.account {
        Some(AccountInfo::ApiKey) => ("apiKey".to_string(), None, None),
        Some(AccountInfo::Chatgpt { email, plan_type }) => (email.clone(), Some(email), plan_type),
        None => ("<unknown>".to_string(), None, None),
    };

    let (five_hour, weekly, reset_credits) = match rpc_request::<_, RateLimitReadResult>(
        &mut websocket,
        3,
        "account/rateLimits/read",
        Value::Null,
    ) {
        Ok(rate_limits) => {
            let (five_hour, weekly) = split_rate_limit_windows(rate_limits.rate_limits);
            (
                five_hour,
                weekly,
                rate_limits
                    .reset_credits
                    .map(|credits| credits.available_count),
            )
        }
        Err(err) => {
            let _ = websocket.close(None);
            return Ok(AccountProbeResult {
                auth_file: auth_file_name(&auth_file.path),
                auth_path: auth_file.path.clone(),
                is_current: auth_file.is_current,
                account_label,
                email,
                plan_type,
                five_hour: None,
                weekly: None,
                reset_credits: None,
                status: ProbeStatus::Error(err.to_string()),
            });
        }
    };

    let _ = websocket.close(None);

    Ok(AccountProbeResult {
        auth_file: auth_file_name(&auth_file.path),
        auth_path: auth_file.path.clone(),
        is_current: auth_file.is_current,
        account_label,
        email,
        plan_type,
        five_hour,
        weekly,
        reset_credits,
        status: ProbeStatus::Ok,
    })
}

fn reserve_port() -> Result<u16> {
    let listener =
        TcpListener::bind(("127.0.0.1", 0)).context("failed to reserve localhost port")?;
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

fn rpc_request<P, R>(
    websocket: &mut WebSocket<TcpStream>,
    id: u64,
    method: &str,
    params: P,
) -> Result<R>
where
    P: Serialize,
    R: DeserializeOwned,
{
    let request = RpcRequest { id, method, params };
    websocket
        .send(Message::Text(serde_json::to_string(&request)?))
        .with_context(|| format!("failed to send RPC request {method}"))?;

    loop {
        let message = websocket
            .read()
            .with_context(|| format!("failed to read RPC response for {method}"))?;
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
    WindowSummary {
        left_percent: (100.0 - window.used_percent).max(0.0),
        resets_at: window.resets_at,
    }
}

fn split_rate_limit_windows(
    snapshot: RateLimitSnapshot,
) -> (Option<WindowSummary>, Option<WindowSummary>) {
    let windows = [snapshot.primary, snapshot.secondary];
    let mut five_hour_idx = window_index_for_duration(&windows, FIVE_HOUR_WINDOW_MINS, None);
    let mut weekly_idx = window_index_for_duration(&windows, WEEKLY_WINDOW_MINS, five_hour_idx);

    // Older app-server versions did not always report the duration. In that protocol,
    // primary is the short (5h) window and secondary is the weekly window.
    if five_hour_idx.is_none() && window_has_unknown_duration(&windows[0]) && weekly_idx != Some(0)
    {
        five_hour_idx = Some(0);
    }
    if weekly_idx.is_none() && window_has_unknown_duration(&windows[1]) && five_hour_idx != Some(1)
    {
        weekly_idx = Some(1);
    }

    // If an exact match occupies the opposite protocol slot, the remaining
    // durationless legacy window can still be assigned without relabeling an
    // explicitly described unknown duration.
    if five_hour_idx.is_none() {
        five_hour_idx = windows
            .iter()
            .enumerate()
            .find(|(idx, window)| window_has_unknown_duration(window) && weekly_idx != Some(*idx))
            .map(|(idx, _)| idx);
    }
    if weekly_idx.is_none() {
        weekly_idx = windows
            .iter()
            .enumerate()
            .find(|(idx, window)| {
                window_has_unknown_duration(window) && five_hour_idx != Some(*idx)
            })
            .map(|(idx, _)| idx);
    }

    let summary_at =
        |idx: Option<usize>| idx.and_then(|idx| windows[idx].clone()).map(window_summary);
    (summary_at(five_hour_idx), summary_at(weekly_idx))
}

fn window_has_unknown_duration(window: &Option<RateLimitWindow>) -> bool {
    matches!(window, Some(window) if window.window_duration_mins.is_none())
}

fn window_index_for_duration(
    windows: &[Option<RateLimitWindow>; 2],
    duration_mins: u64,
    excluded_idx: Option<usize>,
) -> Option<usize> {
    windows.iter().enumerate().find_map(|(idx, window)| {
        (excluded_idx != Some(idx)
            && window
                .as_ref()
                .and_then(|window| window.window_duration_mins)
                == Some(duration_mins))
        .then_some(idx)
    })
}

fn compare_results(left: &AccountProbeResult, right: &AccountProbeResult) -> Ordering {
    compare_status(left, right)
        .then_with(|| compare_window(left.weekly.as_ref(), right.weekly.as_ref()))
        .then_with(|| compare_window(left.five_hour.as_ref(), right.five_hour.as_ref()))
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

const TABLE_HEADERS: [&str; 9] = [
    "auth",
    "account",
    "plan",
    "5h left",
    "5h reset",
    "weekly left",
    "weekly reset",
    "resets",
    "status",
];

struct AccountTable {
    rows: Vec<Vec<String>>,
    widths: [usize; TABLE_HEADERS.len()],
}

impl AccountTable {
    fn new(results: &[AccountProbeResult]) -> Self {
        let rows = results
            .iter()
            .map(|result| {
                vec![
                    if result.is_current {
                        format!("{} *", result.auth_file)
                    } else {
                        result.auth_file.clone()
                    },
                    result.account_label.clone(),
                    result.plan_type.clone().unwrap_or_else(|| "-".to_string()),
                    format_left(result.five_hour.as_ref()),
                    format_reset(result.five_hour.as_ref()),
                    format_left(result.weekly.as_ref()),
                    format_reset(result.weekly.as_ref()),
                    format_reset_credits(result.reset_credits),
                    format_status(&result.status),
                ]
            })
            .collect::<Vec<_>>();

        let mut widths = TABLE_HEADERS.map(str::len);
        for row in &rows {
            for (idx, value) in row.iter().enumerate() {
                widths[idx] = widths[idx].max(value.len());
            }
        }

        Self { rows, widths }
    }

    fn header(&self, color_enabled: bool) -> String {
        format!(
            "{}  {}  {}  {}  {}  {}  {}  {}  {}",
            render_header(TABLE_HEADERS[0], self.widths[0], false, color_enabled),
            render_header(TABLE_HEADERS[1], self.widths[1], false, color_enabled),
            render_header(TABLE_HEADERS[2], self.widths[2], false, color_enabled),
            render_header(TABLE_HEADERS[3], self.widths[3], true, color_enabled),
            render_header(TABLE_HEADERS[4], self.widths[4], false, color_enabled),
            render_header(TABLE_HEADERS[5], self.widths[5], true, color_enabled),
            render_header(TABLE_HEADERS[6], self.widths[6], false, color_enabled),
            render_header(TABLE_HEADERS[7], self.widths[7], true, color_enabled),
            render_header(TABLE_HEADERS[8], self.widths[8], false, color_enabled),
        )
    }

    fn row(&self, idx: usize, result: &AccountProbeResult, color_enabled: bool) -> String {
        let row = &self.rows[idx];
        format!(
            "{}  {}  {}  {}  {}  {}  {}  {}  {}",
            render_auth_cell(&row[0], self.widths[0], result.is_current, color_enabled),
            render_plain_cell(&row[1], self.widths[1], false, color_enabled, None),
            render_plain_cell(&row[2], self.widths[2], false, color_enabled, None),
            render_plain_cell(
                &row[3],
                self.widths[3],
                true,
                color_enabled,
                Some(color_for_percent(five_hour_left_percent(result))),
            ),
            render_plain_cell(&row[4], self.widths[4], false, color_enabled, None),
            render_plain_cell(
                &row[5],
                self.widths[5],
                true,
                color_enabled,
                Some(color_for_percent(weekly_left_percent(result))),
            ),
            render_plain_cell(&row[6], self.widths[6], false, color_enabled, None),
            render_plain_cell(
                &row[7],
                self.widths[7],
                true,
                color_enabled,
                Some(color_for_reset_credits(result.reset_credits)),
            ),
            render_plain_cell(
                &row[8],
                self.widths[8],
                false,
                color_enabled,
                Some(status_color(&result.status))
            ),
        )
    }

    fn interactive(&self, width: usize) -> Result<(String, Vec<String>)> {
        let columns: &[usize] = if width >= 140 {
            &[0, 1, 2, 3, 4, 5, 6, 7, 8]
        } else if width >= 105 {
            &[0, 1, 2, 3, 4, 5, 6, 8]
        } else {
            &[0, 1, 2, 3, 5, 8]
        };
        let mut widths = self.widths;
        for &idx in columns {
            widths[idx] = widths[idx].min(interactive_max_column_width(idx));
        }

        let separators_width = columns.len().saturating_sub(1) * 2;
        while columns.iter().map(|idx| widths[*idx]).sum::<usize>() + separators_width > width {
            let mut reduced = false;
            for idx in [8, 1, 0] {
                if columns.contains(&idx) && widths[idx] > interactive_min_column_width(idx) {
                    widths[idx] -= 1;
                    reduced = true;
                    break;
                }
            }
            if !reduced {
                bail!("terminal is too narrow for interactive account selection");
            }
        }

        let header = render_interactive_columns(&TABLE_HEADERS, columns, &widths);
        let rows = self
            .rows
            .iter()
            .map(|row| fit_menu_line(&render_interactive_columns(row, columns, &widths), width))
            .collect();
        Ok((fit_menu_line(&header, width), rows))
    }
}

fn interactive_max_column_width(idx: usize) -> usize {
    match idx {
        0 => 28,
        1 => 26,
        8 => 24,
        _ => usize::MAX,
    }
}

fn interactive_min_column_width(idx: usize) -> usize {
    match idx {
        0 | 1 => 12,
        8 => 5,
        _ => TABLE_HEADERS[idx].len(),
    }
}

fn render_interactive_columns<T>(
    values: &[T],
    columns: &[usize],
    widths: &[usize; TABLE_HEADERS.len()],
) -> String
where
    T: AsRef<str>,
{
    columns
        .iter()
        .map(|idx| {
            let alignment = if matches!(*idx, 3 | 5 | 7) {
                Alignment::Right
            } else {
                Alignment::Left
            };
            pad_str(values[*idx].as_ref(), widths[*idx], alignment, Some("...")).into_owned()
        })
        .collect::<Vec<_>>()
        .join("  ")
}

fn print_table(results: &[AccountProbeResult]) {
    let color_enabled = stdout_is_tty();
    let table = AccountTable::new(results);
    println!("{}", table.header(color_enabled));
    for (idx, result) in results.iter().enumerate() {
        println!("{}", table.row(idx, result, color_enabled));
    }
    println!();
    println!(
        "Total left: 5h {} | weekly {}",
        format_total_percent(total_five_hour_left_percent(results)),
        format_total_percent(total_weekly_left_percent(results)),
    );
}

fn run_interactive_switch(ctx: &AppContext, args: &SwitchArgs) -> Result<()> {
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        bail!(
            "switch requires an interactive terminal; use `codex-accounts use <selector>` in scripts"
        );
    }

    let results = load_or_probe(ctx, args.refresh)?;
    let Some(selected_idx) = select_account_interactively(&results)? else {
        println!("Anulowano.");
        return Ok(());
    };
    let selected = &results[selected_idx];

    let confirmed = {
        let stdin = std::io::stdin();
        let stderr = std::io::stderr();
        let mut reader = stdin.lock();
        let mut writer = stderr.lock();
        confirm_account_switch(&mut reader, &mut writer, &selected.auth_file)?
    };
    if !confirmed {
        println!("Anulowano.");
        return Ok(());
    }

    switch_to(ctx, selected)?;
    clear_cache(ctx)?;
    println!(
        "Switched to {} ({})",
        selected.auth_file, selected.account_label
    );
    Ok(())
}

fn select_account_interactively(results: &[AccountProbeResult]) -> Result<Option<usize>> {
    if results.is_empty() {
        bail!("no accounts are available to select");
    }

    let term = Term::stderr();
    let (terminal_rows, terminal_cols) = term.size();
    let menu_width = usize::from(terminal_cols).saturating_sub(2);
    if menu_width < 20 {
        bail!("terminal is too narrow for interactive account selection");
    }

    let table = AccountTable::new(results);
    let (header, items) = table.interactive(menu_width)?;
    let default_idx = results
        .iter()
        .position(|result| result.is_current)
        .unwrap_or(0);
    let page_size = usize::from(terminal_rows)
        .saturating_sub(4)
        .clamp(1, results.len());
    let theme = ColorfulTheme {
        active_item_style: Style::new().for_stderr().reverse().bold(),
        ..ColorfulTheme::default()
    };

    term.write_line("Wybierz konto (↑/↓, Enter; Esc/q — anuluj):")
        .context("failed to render interactive account selector")?;
    term.write_line(&format!("  {header}"))
        .context("failed to render interactive account table header")?;

    Select::with_theme(&theme)
        .items(&items)
        .default(default_idx)
        .max_length(page_size)
        .report(false)
        .interact_on_opt(&term)
        .context("interactive account selection failed")
}

fn fit_menu_line(text: &str, width: usize) -> String {
    pad_str(text, width, Alignment::Left, Some("...")).into_owned()
}

fn confirm_account_switch<R, W>(reader: &mut R, writer: &mut W, auth_file: &str) -> Result<bool>
where
    R: BufRead,
    W: Write,
{
    loop {
        write!(writer, "Czy chcesz zmienić konto na: {auth_file} [t/n]: ")
            .context("failed to render account switch confirmation")?;
        writer
            .flush()
            .context("failed to flush account switch confirmation")?;

        let mut answer = String::new();
        let bytes_read = match reader.read_line(&mut answer) {
            Ok(bytes_read) => bytes_read,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => return Ok(false),
            Err(err) => return Err(err).context("failed to read account switch confirmation"),
        };
        if bytes_read == 0 {
            return Ok(false);
        }

        match parse_confirmation(&answer) {
            Some(confirmed) => return Ok(confirmed),
            None => {
                writeln!(writer, "Wpisz t lub n.")
                    .context("failed to render account switch confirmation error")?;
            }
        }
    }
}

fn parse_confirmation(input: &str) -> Option<bool> {
    match input.trim() {
        "t" | "T" => Some(true),
        "n" | "N" => Some(false),
        _ => None,
    }
}

fn print_use_best_preview(selected: &AccountProbeResult) {
    let color_enabled = stdout_is_tty();
    println!(
        "{} -> {} | 5h left {} | weekly left {}",
        paint(&selected.auth_file, ANSI_BOLD_GREEN, color_enabled),
        paint(&selected.account_label, ANSI_BOLD_GREEN, color_enabled),
        paint(
            &format_left(selected.five_hour.as_ref()),
            color_for_percent(five_hour_left_percent(selected)),
            color_enabled,
        ),
        paint(
            &format_left(selected.weekly.as_ref()),
            color_for_percent(weekly_left_percent(selected)),
            color_enabled,
        ),
    );
}

fn render_header(text: &str, width: usize, right_align: bool, color_enabled: bool) -> String {
    let cell = pad_cell(text, width, right_align);
    paint(&cell, ANSI_BOLD_CYAN, color_enabled)
}

fn render_auth_cell(text: &str, width: usize, is_current: bool, color_enabled: bool) -> String {
    let cell = pad_cell(text, width, false);
    if is_current {
        paint(&cell, ANSI_BOLD_GREEN, color_enabled)
    } else {
        cell
    }
}

fn render_plain_cell(
    text: &str,
    width: usize,
    right_align: bool,
    color_enabled: bool,
    style: Option<&str>,
) -> String {
    let cell = pad_cell(text, width, right_align);
    if let Some(style) = style {
        paint(&cell, style, color_enabled)
    } else {
        cell
    }
}

fn pad_cell(text: &str, width: usize, right_align: bool) -> String {
    if right_align {
        format!("{:>width$}", text)
    } else {
        format!("{:<width$}", text)
    }
}

fn paint(text: &str, style: &str, enabled: bool) -> String {
    if enabled {
        format!("{style}{text}{ANSI_RESET}")
    } else {
        text.to_string()
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

fn format_reset_credits(reset_credits: Option<u64>) -> String {
    reset_credits
        .map(|count| count.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn format_status(status: &ProbeStatus) -> String {
    match status {
        ProbeStatus::Ok => "ok".to_string(),
        ProbeStatus::Error(err) => truncate(err, 48),
    }
}

fn status_color(status: &ProbeStatus) -> &'static str {
    match status {
        ProbeStatus::Ok => ANSI_GREEN,
        ProbeStatus::Error(_) => ANSI_RED,
    }
}

fn color_for_percent(left_percent: f64) -> &'static str {
    if left_percent >= 50.0 {
        ANSI_GREEN
    } else if left_percent >= 20.0 {
        ANSI_YELLOW
    } else {
        ANSI_RED
    }
}

fn color_for_reset_credits(reset_credits: Option<u64>) -> &'static str {
    if reset_credits.unwrap_or(0) > 0 {
        ANSI_GREEN
    } else {
        ANSI_RED
    }
}

fn weekly_left_percent(result: &AccountProbeResult) -> f64 {
    result
        .weekly
        .as_ref()
        .map(|item| item.left_percent)
        .unwrap_or(0.0)
}

fn five_hour_left_percent(result: &AccountProbeResult) -> f64 {
    result
        .five_hour
        .as_ref()
        .map(|item| item.left_percent)
        .unwrap_or(0.0)
}

fn total_five_hour_left_percent(results: &[AccountProbeResult]) -> f64 {
    results
        .iter()
        .filter_map(|result| result.five_hour.as_ref())
        .map(|window| window.left_percent)
        .sum()
}

fn total_weekly_left_percent(results: &[AccountProbeResult]) -> f64 {
    results
        .iter()
        .filter_map(|result| result.weekly.as_ref())
        .map(|window| window.left_percent)
        .sum()
}

fn format_total_percent(percent: f64) -> String {
    format!("{percent:.0}%")
}

fn truncate(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        text.to_string()
    } else {
        format!("{}...", &text[..max_len.saturating_sub(3)])
    }
}

fn resolve_selector<'a>(
    results: &'a [AccountProbeResult],
    selector: &str,
) -> Result<&'a AccountProbeResult> {
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
                || auth_stem(&result.auth_file)
                    .to_lowercase()
                    .contains(&selector_lower)
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

fn select_best_candidate<'a>(
    ctx: &AppContext,
    results: &'a [AccountProbeResult],
    claim: bool,
) -> Result<&'a AccountProbeResult> {
    let candidates = rank_best_candidates(results);
    if candidates.is_empty() {
        bail!("no account has a readable weekly limit");
    }

    let fallback = candidates.first().copied().unwrap();
    for candidate in candidates {
        if account_is_cooled_down(ctx, candidate)? {
            continue;
        }
        if claim {
            if claim_account_cooldown(ctx, candidate)? {
                return Ok(candidate);
            }
        } else {
            return Ok(candidate);
        }
    }

    Ok(fallback)
}

fn compare_best_candidate(left: &AccountProbeResult, right: &AccountProbeResult) -> Ordering {
    weekly_left_percent(left)
        .partial_cmp(&weekly_left_percent(right))
        .unwrap_or(Ordering::Equal)
        .then_with(|| {
            five_hour_left_percent(left)
                .partial_cmp(&five_hour_left_percent(right))
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| right.auth_file.cmp(&left.auth_file))
}

fn rank_best_candidates(results: &[AccountProbeResult]) -> Vec<&AccountProbeResult> {
    let mut candidates: Vec<&AccountProbeResult> = results
        .iter()
        .filter(|result| matches!(result.status, ProbeStatus::Ok))
        .filter(|result| result.weekly.is_some())
        .collect();
    candidates.sort_by(|left, right| compare_best_candidate(right, left));
    candidates
}

fn stdout_is_tty() -> bool {
    std::io::stdout().is_terminal()
}

fn account_cooldown_path(ctx: &AppContext, result: &AccountProbeResult) -> PathBuf {
    ctx.tmp_root
        .join("use-best")
        .join(format!("{}.lock", sanitize_account_name(&result.auth_file)))
}

fn claim_account_cooldown(ctx: &AppContext, result: &AccountProbeResult) -> Result<bool> {
    acquire_timed_lock(
        &account_cooldown_path(ctx, result),
        Duration::from_secs(USE_BEST_LOCK_TTL_SECS),
    )
}

fn account_is_cooled_down(ctx: &AppContext, result: &AccountProbeResult) -> Result<bool> {
    let lock_path = account_cooldown_path(ctx, result);
    match read_lock_state(&lock_path)? {
        Some(state) if state.expires_at > chrono::Utc::now().timestamp() => Ok(true),
        _ => Ok(false),
    }
}

fn acquire_timed_lock(lock_path: &Path, ttl: Duration) -> Result<bool> {
    let now = chrono::Utc::now().timestamp();
    let expires_at = now + i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX - now);

    loop {
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create lock directory {}", parent.display()))?;
        }
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(lock_path)
        {
            Ok(mut file) => {
                let state = UseBestLockState {
                    created_at: now,
                    expires_at,
                    pid: std::process::id(),
                };
                let text = serde_json::to_string_pretty(&state)?;
                file.write_all(text.as_bytes()).with_context(|| {
                    format!("failed to write lock file {}", lock_path.display())
                })?;
                file.sync_all().with_context(|| {
                    format!("failed to flush lock file {}", lock_path.display())
                })?;
                return Ok(true);
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                match read_lock_state(lock_path)? {
                    Some(state) if state.expires_at > now => {
                        return Ok(false);
                    }
                    _ => {
                        let _ = fs::remove_file(lock_path);
                    }
                }
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to create lock file {}", lock_path.display())
                });
            }
        }
    }
}

fn read_lock_state(lock_path: &Path) -> Result<Option<UseBestLockState>> {
    let raw = match fs::read_to_string(lock_path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to read lock file {}", lock_path.display()));
        }
    };

    match serde_json::from_str(&raw) {
        Ok(state) => Ok(Some(state)),
        Err(_) => Ok(None),
    }
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

fn remove_selected(selected: &AccountProbeResult) -> Result<()> {
    fs::remove_file(&selected.auth_path)
        .with_context(|| format!("failed to remove {}", selected.auth_path.display()))?;
    Ok(())
}

fn import_new_account(ctx: &AppContext) -> Result<PathBuf> {
    preserve_current_auth(ctx)?;
    let login_home = Builder::new()
        .prefix("import-")
        .tempdir_in(&ctx.tmp_root)
        .with_context(|| format!("failed to create temp dir in {}", ctx.tmp_root.display()))?;
    fs::write(
        login_home.path().join("config.toml"),
        "cli_auth_credentials_store = \"file\"\n",
    )
    .with_context(|| {
        format!(
            "failed to configure isolated Codex home {}",
            login_home.path().display()
        )
    })?;
    let status = Command::new("codex")
        .arg("login")
        .env("CODEX_HOME", login_home.path())
        .current_dir(login_home.path())
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .context("failed to launch interactive codex login")?;
    if !status.success() {
        bail!("Codex login exited with status {status}");
    }

    let auth_path = login_home.path().join("auth.json");
    let bytes = fs::read(&auth_path).with_context(|| {
        format!(
            "Codex login did not leave auth.json in {}",
            login_home.path().display()
        )
    })?;
    let imported = AuthFile {
        path: auth_path.clone(),
        bytes: bytes.clone(),
        is_current: false,
    };
    let probe = probe_single_auth_inner(ctx, &imported)?;
    let stored_path = store_account_auth(ctx, &probe, &bytes)?;
    switch_to(ctx, &probe)?;
    Ok(stored_path)
}

fn preserve_current_auth(ctx: &AppContext) -> Result<()> {
    let current_path = ctx.codex_root.join("auth.json");
    let bytes = match fs::read(&current_path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to read {}", current_path.display()))
        }
    };

    if discover_auth_files(ctx)?
        .iter()
        .any(|entry| entry.path.starts_with(&ctx.accounts_root) && entry.bytes == bytes)
    {
        return Ok(());
    }

    let mut candidate = ctx.accounts_root.join("active.json");
    let mut suffix = 2u32;
    while candidate.exists() {
        candidate = ctx.accounts_root.join(format!("active-{suffix}.json"));
        suffix += 1;
    }
    fs::write(&candidate, bytes)
        .with_context(|| format!("failed to preserve current auth in {}", candidate.display()))?;
    Ok(())
}

fn store_account_auth(
    ctx: &AppContext,
    probe: &AccountProbeResult,
    bytes: &[u8],
) -> Result<PathBuf> {
    let existing = discover_auth_files(ctx)?;
    if let Some(found) = existing
        .iter()
        .find(|entry| entry.path.starts_with(&ctx.accounts_root) && entry.bytes == bytes)
    {
        return Ok(found.path.clone());
    }

    if let Some(email) = &probe.email {
        let base_name = sanitize_account_name(email);
        let mut candidate = ctx.accounts_root.join(format!("{base_name}.json"));
        let mut suffix = 2u32;
        while candidate.exists() {
            candidate = ctx.accounts_root.join(format!("{base_name}-{suffix}.json"));
            suffix += 1;
        }
        fs::write(&candidate, bytes)
            .with_context(|| format!("failed to write {}", candidate.display()))?;
        return Ok(candidate);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ctx(tempdir: &tempfile::TempDir) -> AppContext {
        let codex_root = tempdir.path().join("codex");
        AppContext {
            codex_root: codex_root.clone(),
            accounts_root: codex_root.join("accounts"),
            tmp_root: codex_root.join("tmp"),
            cache_path: codex_root.join("tmp/cache.json"),
        }
    }

    fn sample_window(left_percent: f64) -> WindowSummary {
        WindowSummary {
            left_percent,
            resets_at: None,
        }
    }

    fn sample_result_with_windows(
        auth_file: &str,
        five_hour_left: f64,
        weekly_left: f64,
    ) -> AccountProbeResult {
        AccountProbeResult {
            auth_file: auth_file.to_string(),
            auth_path: PathBuf::from(format!("/tmp/{auth_file}")),
            is_current: false,
            account_label: auth_file.to_string(),
            email: Some(format!("{auth_file}@example.com")),
            plan_type: Some("plus".to_string()),
            five_hour: Some(sample_window(five_hour_left)),
            weekly: Some(sample_window(weekly_left)),
            reset_credits: None,
            status: ProbeStatus::Ok,
        }
    }

    fn sample_result(auth_file: &str, weekly_left: f64) -> AccountProbeResult {
        sample_result_with_windows(auth_file, 50.0, weekly_left)
    }

    fn rate_limit_window(used_percent: f64, duration_mins: Option<u64>) -> RateLimitWindow {
        RateLimitWindow {
            used_percent,
            window_duration_mins: duration_mins,
            resets_at: None,
        }
    }

    #[test]
    fn reads_available_reset_credits() {
        let response: RateLimitReadResult = serde_json::from_str(
            r#"{"rateLimits":{},"rateLimitResetCredits":{"availableCount":2}}"#,
        )
        .unwrap();

        assert_eq!(response.reset_credits.unwrap().available_count, 2);
    }

    #[test]
    fn parses_switch_command_with_refresh() {
        let cli = Cli::try_parse_from(["codex-accounts", "switch", "--refresh"]).unwrap();

        assert!(matches!(
            cli.command,
            Some(Commands::Switch(SwitchArgs { refresh: true }))
        ));
    }

    #[test]
    fn parses_polish_confirmation_answers() {
        assert_eq!(parse_confirmation("t\n"), Some(true));
        assert_eq!(parse_confirmation(" T "), Some(true));
        assert_eq!(parse_confirmation("n\n"), Some(false));
        assert_eq!(parse_confirmation(" N "), Some(false));
        assert_eq!(parse_confirmation("y\n"), None);
        assert_eq!(parse_confirmation("\n"), None);
    }

    #[test]
    fn confirmation_reprompts_until_t_or_n() {
        let mut input = std::io::Cursor::new(b"maybe\nT\n");
        let mut output = Vec::new();

        let confirmed = confirm_account_switch(&mut input, &mut output, "alpha.json").unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(confirmed);
        assert_eq!(output.matches("[t/n]:").count(), 2);
        assert!(output.contains("Wpisz t lub n."));
    }

    #[test]
    fn confirmation_eof_cancels_switch() {
        let mut input = std::io::Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        let confirmed = confirm_account_switch(&mut input, &mut output, "alpha.json").unwrap();

        assert!(!confirmed);
    }

    #[test]
    fn menu_line_is_padded_or_truncated_to_terminal_width() {
        assert_eq!(
            console::measure_text_width(&fit_menu_line("account", 12)),
            12
        );
        assert_eq!(fit_menu_line("a very long account row", 10), "a very ...");
    }

    #[test]
    fn narrow_interactive_table_keeps_both_usage_windows_visible() {
        let results = vec![sample_result_with_windows("alpha.json", 75.0, 60.0)];
        let table = AccountTable::new(&results);

        let (header, rows) = table.interactive(78).unwrap();

        assert!(header.contains("5h left"));
        assert!(header.contains("weekly left"));
        assert!(header.contains("status"));
        assert!(!header.contains("5h reset"));
        assert_eq!(console::measure_text_width(&header), 78);
        assert_eq!(console::measure_text_width(&rows[0]), 78);
        assert!(rows[0].contains("75%"));
        assert!(rows[0].contains("60%"));
    }

    #[test]
    fn sums_available_usage_percentages() {
        let results = vec![
            sample_result_with_windows("alpha.json", 75.0, 60.0),
            sample_result_with_windows("beta.json", 125.0, 140.0),
        ];

        assert_eq!(
            format_total_percent(total_five_hour_left_percent(&results)),
            "200%"
        );
        assert_eq!(
            format_total_percent(total_weekly_left_percent(&results)),
            "200%"
        );
    }

    #[test]
    fn excludes_missing_usage_windows_from_totals() {
        let mut result = sample_result_with_windows("alpha.json", 75.0, 60.0);
        result.weekly = None;
        let results = vec![result];

        assert_eq!(total_five_hour_left_percent(&results), 75.0);
        assert_eq!(total_weekly_left_percent(&results), 0.0);
    }

    #[test]
    fn wide_interactive_table_includes_all_list_columns() {
        let results = vec![sample_result("alpha.json", 60.0)];
        let table = AccountTable::new(&results);

        let (header, _) = table.interactive(160).unwrap();

        for expected in ["5h reset", "weekly reset", "resets", "status"] {
            assert!(header.contains(expected));
        }
    }

    #[test]
    fn maps_primary_5h_and_secondary_weekly_windows() {
        let snapshot = RateLimitSnapshot {
            primary: Some(rate_limit_window(15.0, Some(FIVE_HOUR_WINDOW_MINS))),
            secondary: Some(rate_limit_window(35.0, Some(WEEKLY_WINDOW_MINS))),
        };

        let (five_hour, weekly) = split_rate_limit_windows(snapshot);

        assert_eq!(five_hour.unwrap().left_percent, 85.0);
        assert_eq!(weekly.unwrap().left_percent, 65.0);
    }

    #[test]
    fn maps_windows_by_duration_even_when_protocol_slots_are_swapped() {
        let snapshot = RateLimitSnapshot {
            primary: Some(rate_limit_window(35.0, Some(WEEKLY_WINDOW_MINS))),
            secondary: Some(rate_limit_window(15.0, Some(FIVE_HOUR_WINDOW_MINS))),
        };

        let (five_hour, weekly) = split_rate_limit_windows(snapshot);

        assert_eq!(five_hour.unwrap().left_percent, 85.0);
        assert_eq!(weekly.unwrap().left_percent, 65.0);
    }

    #[test]
    fn falls_back_to_primary_and_secondary_for_legacy_windows_without_duration() {
        let snapshot = RateLimitSnapshot {
            primary: Some(rate_limit_window(15.0, None)),
            secondary: Some(rate_limit_window(35.0, None)),
        };

        let (five_hour, weekly) = split_rate_limit_windows(snapshot);

        assert_eq!(five_hour.unwrap().left_percent, 85.0);
        assert_eq!(weekly.unwrap().left_percent, 65.0);
    }

    #[test]
    fn keeps_single_weekly_window_out_of_5h_column() {
        let snapshot = RateLimitSnapshot {
            primary: Some(rate_limit_window(35.0, Some(WEEKLY_WINDOW_MINS))),
            secondary: None,
        };

        let (five_hour, weekly) = split_rate_limit_windows(snapshot);

        assert!(five_hour.is_none());
        assert_eq!(weekly.unwrap().left_percent, 65.0);
    }

    #[test]
    fn does_not_label_explicit_unknown_primary_duration_as_5h() {
        let snapshot = RateLimitSnapshot {
            primary: Some(rate_limit_window(15.0, Some(60))),
            secondary: Some(rate_limit_window(35.0, Some(WEEKLY_WINDOW_MINS))),
        };

        let (five_hour, weekly) = split_rate_limit_windows(snapshot);

        assert!(five_hour.is_none());
        assert_eq!(weekly.unwrap().left_percent, 65.0);
    }

    #[test]
    fn does_not_label_explicit_unknown_secondary_duration_as_5h() {
        let snapshot = RateLimitSnapshot {
            primary: Some(rate_limit_window(35.0, Some(WEEKLY_WINDOW_MINS))),
            secondary: Some(rate_limit_window(15.0, Some(30 * 24 * 60))),
        };

        let (five_hour, weekly) = split_rate_limit_windows(snapshot);

        assert!(five_hour.is_none());
        assert_eq!(weekly.unwrap().left_percent, 65.0);
    }

    #[test]
    fn maps_remaining_durationless_window_after_exact_weekly_match() {
        let snapshot = RateLimitSnapshot {
            primary: Some(rate_limit_window(35.0, Some(WEEKLY_WINDOW_MINS))),
            secondary: Some(rate_limit_window(15.0, None)),
        };

        let (five_hour, weekly) = split_rate_limit_windows(snapshot);

        assert_eq!(five_hour.unwrap().left_percent, 85.0);
        assert_eq!(weekly.unwrap().left_percent, 65.0);
    }

    fn lock_account(result: &AccountProbeResult, expires_at: i64, tempdir: &tempfile::TempDir) {
        let ctx = sample_ctx(tempdir);
        let lock_path = account_cooldown_path(&ctx, result);
        fs::create_dir_all(lock_path.parent().unwrap()).unwrap();
        let lock = UseBestLockState {
            created_at: expires_at - 300,
            expires_at,
            pid: 1234,
        };
        fs::write(&lock_path, serde_json::to_string_pretty(&lock).unwrap()).unwrap();
    }

    #[test]
    fn select_best_uses_highest_weekly_limit() {
        let tempdir = tempfile::tempdir().unwrap();
        let ctx = sample_ctx(&tempdir);
        let results = vec![
            sample_result("lower.json", 20.0),
            sample_result("higher.json", 60.0),
        ];

        let selected = select_best_candidate(&ctx, &results, false).unwrap();
        assert_eq!(selected.auth_file, "higher.json");
    }

    #[test]
    fn select_best_uses_5h_limit_to_break_weekly_ties() {
        let tempdir = tempfile::tempdir().unwrap();
        let ctx = sample_ctx(&tempdir);
        let results = vec![
            sample_result_with_windows("lower-5h.json", 20.0, 60.0),
            sample_result_with_windows("higher-5h.json", 80.0, 60.0),
        ];

        let selected = select_best_candidate(&ctx, &results, false).unwrap();
        assert_eq!(selected.auth_file, "higher-5h.json");
    }

    #[test]
    fn compare_results_sorts_highest_weekly_limit_first() {
        let mut results = [
            sample_result("lower.json", 20.0),
            sample_result("higher.json", 60.0),
        ];

        results.sort_by(compare_results);
        assert_eq!(results[0].auth_file, "higher.json");
    }

    #[test]
    fn importing_same_email_preserves_existing_auth_file() {
        let tempdir = tempfile::tempdir().unwrap();
        let ctx = sample_ctx(&tempdir);
        fs::create_dir_all(&ctx.accounts_root).unwrap();
        let probe = sample_result("ignored.json", 50.0);

        let first = store_account_auth(&ctx, &probe, b"old auth").unwrap();
        let second = store_account_auth(&ctx, &probe, b"new auth").unwrap();

        assert_ne!(first, second);
        assert_eq!(fs::read(&first).unwrap(), b"old auth");
        assert_eq!(fs::read(&second).unwrap(), b"new auth");
        assert!(second.ends_with("ignored.json_at_example.com-2.json"));
    }

    #[test]
    fn select_best_skips_cooled_down_account() {
        let tempdir = tempfile::tempdir().unwrap();
        let ctx = sample_ctx(&tempdir);
        let results = vec![
            sample_result("best.json", 60.0),
            sample_result("backup.json", 50.0),
        ];
        lock_account(&results[0], chrono::Utc::now().timestamp() + 300, &tempdir);

        let selected = select_best_candidate(&ctx, &results, false).unwrap();
        assert_eq!(selected.auth_file, "backup.json");
    }
}
