"""Run after `cargo build`: python3 tests/switch_restart.py (requires pexpect)."""
import json
import os
from pathlib import Path
import tempfile
import time

import pexpect

binary = str(Path(__file__).resolve().parents[1] / 'target/debug/codex-accounts')
for answer, exit_code, should_restart in [('Y', 0, True), ('N', 0, False), ('', 0, False), ('Y', 1, True)]:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        auth = root / 'selected.json'
        auth.write_text('{"test":true}')
        cache = root / 'tmp/codex-accounts/cache-v4.json'
        cache.parent.mkdir(parents=True)
        cache.write_text(json.dumps({'generated_at': int(time.time()), 'results': [{
            'auth_file': auth.name, 'auth_path': str(auth), 'is_current': False,
            'account_label': 'test@example.com', 'email': 'test@example.com',
            'plan_type': 'team', 'five_hour': None, 'weekly': None,
            'reset_credits': None, 'status': 'Ok',
        }]}))
        fake = root / 'codex'
        fake.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$CODEX_HOME/restart-args"\nexit ' + str(exit_code) + '\n')
        fake.chmod(0o700)
        env = dict(os.environ, CODEX_HOME=str(root), PATH=str(root) + os.pathsep + os.environ['PATH'], TERM='xterm')
        child = pexpect.spawn(binary, ['switch'], env=env, encoding='utf-8', timeout=10, dimensions=(40, 180))
        try:
            child.expect('Wybierz konto')
            child.send('\r')
            child.expect_exact('[t/n]: ')
            child.sendline('t')
            child.expect_exact('Execute `codex app-server daemon restart`?')
            child.send(answer + '\r')
            child.expect(pexpect.EOF)
            child.close()
            assert child.exitstatus == exit_code, (answer, child.exitstatus, child.before)
            marker = root / 'restart-args'
            assert marker.exists() == should_restart
            if should_restart:
                assert marker.read_text().splitlines() == ['app-server', 'daemon', 'restart']
            assert (root / 'auth.json').read_bytes() == auth.read_bytes()
        finally:
            child.close(force=True)
print('PASS: Y restarts, N/Enter skip, restart failure is reported; account switches in all cases.')
