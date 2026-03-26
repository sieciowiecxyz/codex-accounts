# codex-accounts

`codex-accounts` is a tiny CLI for people who keep multiple Codex / ChatGPT logins and want two things:

- see which account still has room in the `5h` and `weekly` limits
- switch `~/.codex/auth.json` quickly

It reads stored account auth files, shows live usage, and can import a freshly logged-in account into `~/.codex/accounts/`.

## What It Looks Like

```bash
$ codex-accounts list --refresh
auth                                 account                   plan  5h left  5h reset      weekly left  weekly reset  credits  status
alpha.user_at_example.com.json       alpha.user@example.com    team     100%  22 Mar 06:56          70%  28 Mar 14:07  -        ok
beta.user_at_example.com.json        beta.user@example.com     plus      98%  22 Mar 02:36           4%  25 Mar 17:45  -        ok
gamma.user_at_example.com.json *     gamma.user@example.com    team      43%  22 Mar 02:14          83%  28 Mar 21:14  -        ok
delta.user_at_example.com.json       delta.user@example.com    team       0%  22 Mar 02:12          70%  28 Mar 21:12  -        ok
```

`*` means: this account matches the currently active `~/.codex/auth.json`.

`use-best` prefers accounts with at least `25%` weekly left. If nothing clears that bar, it falls back to the best remaining account. After a successful run, the command is cooled down for 5 minutes so another terminal cannot immediately flip `~/.codex/auth.json` again.

When you run the CLI in a real terminal, the human-readable output is colorized. `--json` stays plain.

## Typical Flow

```bash
$ codex-accounts list --refresh
$ codex-accounts use-best --dry-run
alpha.user_at_example.com.json -> alpha.user@example.com | 5h left 100% | weekly left 70%

$ codex-accounts use-best
Switched to best account: alpha.user_at_example.com.json (alpha.user@example.com)
```

## Import A New Account

```bash
$ codex-accounts import-new
```

What happens:

1. current `~/.codex/auth.json` is backed up
2. interactive `codex` is launched
3. you log into a new account and exit
4. the new auth is stored as `~/.codex/accounts/<email>.json`
5. the new auth also remains active as `~/.codex/auth.json`

Example stored names:

```text
~/.codex/accounts/
├── alpha.user_at_example.com.json
├── beta.user_at_example.com.json
└── gamma.user_at_example.com.json
```

## Quick Commands

```bash
codex-accounts doctor
codex-accounts list
codex-accounts list --refresh
codex-accounts list --json
codex-accounts use alpha.user_at_example.com
codex-accounts use-best --dry-run
codex-accounts use-best
codex-accounts import-new
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
codex-accounts doctor
codex-accounts list --refresh
codex-accounts use-best --dry-run
```
