# Development

Use Nix for the same environment as CI:

```bash
nix build .#default
nix build .#checks.x86_64-linux.tested
```

For a source-only run:

```bash
cd server
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_*.py'
node --test pi/test_*.test.mjs
```

Some tests are skipped unless optional external programs are configured:

| Variable | Enables |
| --- | --- |
| `TRG_TEST_PI` or `PI_PACKAGE_DIR` | Pi integration tests |
| `TRG_TEST_AGENT_UTILS` | Real Agent Utils push-to-talk test |
| Real `jev`, `caco` or `aztts` on PATH | Their adapter tests |

The tests use temporary homes, loopback servers and fake providers. They must not
need real credentials or send real messages.

## Compatibility

Version-1 configs and archived request settings remain supported. Add tests whenever
the stored schema changes, so an old queued or blocked request remains inspectable
after an upgrade.

## Privacy

Before publishing, run a search for private hostnames, account names, secret paths,
real transcripts and receipt IDs. Public examples should use `example.invalid`,
loopback URLs and paths under the user's own config directory.
