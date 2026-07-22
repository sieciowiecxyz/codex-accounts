# codex-accounts

`codex-accounts` is a tiny CLI for people who keep multiple Codex / ChatGPT logins and want two things:

- see which account still has room in the weekly limit
- switch `~/.codex/auth.json` quickly

It reads stored account auth files, shows live usage, and can import a freshly logged-in account into `~/.codex/accounts/`.

## What It Looks Like

```bash
$ codex-accounts list --refresh
auth                                 account                   plan  weekly left  weekly reset  status
alpha.user_at_example.com.json       alpha.user@example.com    team           70%  28 Mar 14:07  ok
beta.user_at_example.com.json        beta.user@example.com     plus            4%  25 Mar 17:45  ok
gamma.user_at_example.com.json *     gamma.user@example.com    team           83%  28 Mar 21:14  ok
```

`*` means: this account matches the currently active `~/.codex/auth.json`.

`use-best` picks the account with the most weekly limit remaining. After a successful run, the chosen account is cooled down for 5 minutes so another terminal will pick something else first. If every candidate is already cooled down, it can fall back to the same account.

When you run the CLI in a real terminal, the human-readable output is colorized. `--json` stays plain.

## Typical Flow

```bash
$ codex-accounts list --refresh
$ codex-accounts use-best --dry-run
alpha.user_at_example.com.json -> alpha.user@example.com | weekly left 70%

$ codex-accounts use-best
Switched to best account: alpha.user_at_example.com.json (alpha.user@example.com)
```

## Import A New Account

```bash
$ codex-accounts import-new
```

What happens:

1. `codex login` runs in an isolated temporary `CODEX_HOME`
2. you log into a new account
3. the new auth is stored as `~/.codex/accounts/<email>.json`
4. the new auth becomes active only after it was stored successfully

Example stored names:

```text
~/.codex/accounts/
├── alpha.user_at_example.com.json
├── beta.user_at_example.com.json
└── gamma.user_at_example.com.json
```

## Quick Commands

```bash
codex-accounts list
codex-accounts list --refresh
codex-accounts list --json
codex-accounts use alpha.user_at_example.com
codex-accounts use-best --dry-run
codex-accounts use-best
codex-accounts import-new
codex-accounts remove alpha.user_at_example.com
```

## Install

From this repo:

```bash
cargo install --path . --force --locked
```

If `cargo` installs into `~/.cargo/bin` and that is not in your `PATH`, either add it:

```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

or symlink the binary into a directory already in `PATH`.

If you already installed `codex-accounts` before, rerun the same command with `--force` to update the system binary in place.

## Quick Start

```bash
cargo install --path . --force --locked
codex-accounts list --refresh
codex-accounts use-best --dry-run
```
