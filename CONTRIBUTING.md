# Contributing to NightOwl Agent

Thanks for your interest in contributing. This document covers local setup, the test suite, and the conventions the codebase follows.

The agent is the open-source half of NightOwl — the hosted dashboard is closed-source and out of scope for this repo. Bug reports, feature requests, and PRs for the agent itself are all welcome.

## Ground rules

- **File an issue before a large PR.** Small fixes can go straight to a PR; non-trivial changes should be discussed first so we can align on scope.
- **Keep PRs focused.** One concern per PR — bug fix, feature, or refactor. Don't bundle.
- **Respect the architecture.** See `CLAUDE.md` for the invariants that matter (fork safety, SQLite WAL ordering, COPY vs INSERT tables, back-pressure).
- **No breaking changes without a version bump discussion.** Wire protocol, config keys, and DB schema are public surface.

## Local setup

### Requirements

- PHP **8.2+** with `pdo_pgsql`, `pdo_sqlite`, `pcntl`, `posix`, `zlib`
- Composer 2
- PostgreSQL **14+** (16 or 17 recommended)
- Docker (easiest way to get a test Postgres)

### Clone and install

```bash
git clone https://github.com/lemed99/nightowl-agent.git
cd nightowl-agent
composer install
```

### Spin up a test database

```bash
docker run -d --name nightowl-test-pg -p 5433:5432 \
  -e POSTGRES_DB=nightowl_test \
  -e POSTGRES_USER=nightowl_test \
  -e POSTGRES_PASSWORD=test123 \
  postgres:17-alpine
```

Set `NIGHTOWL_TEST_DB_PORT=5433` when running the full test suite.

## Running tests

The suite has three tiers — run them in order of expense:

| Suite | Command | Needs |
|-------|---------|-------|
| Unit | `vendor/bin/phpunit --testsuite Unit` | Nothing |
| Integration | `vendor/bin/phpunit --testsuite Unit --testsuite Integration` | PG tests auto-skip if unavailable |
| System | `NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit --testsuite System` | PG + `pcntl` + `posix` |

Full run:

```bash
NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit
```

Target a single test file with `vendor/bin/phpunit tests/Unit/Agent/SamplerTest.php`.

### Simulator

Useful when you want to exercise the agent end-to-end against real telemetry shapes:

```bash
php tests/Simulator/run.php --token=<token> --scenario=mixed --count=200
```

Benchmark harness (measures sustained throughput):

```bash
php tests/Simulator/benchmark.php --token=<token> --workers=4 --duration=10
```

## Code style

Format PHP with **Laravel Pint** before committing:

```bash
vendor/bin/pint --dirty
```

CI will reject unformatted diffs.

## Conventions

- **All agent classes are `final`** — no inheritance in runtime code.
- **No Eloquent in the agent runtime** — raw PDO only. The hot path is performance-critical.
- **Durations are microseconds** in the DB, milliseconds in API responses.
- **Error logging** uses `error_log("[NightOwl Agent] ...")` with a component tag.
- **`json_decode`** in drain/runtime paths uses `(..., true, N, JSON_THROW_ON_ERROR)` — never the no-args form.
- **Fork safety**: close SQLite PDO *before* fork, re-create *after*. Parent handle inheritance corrupts WAL on child exit.
- **WAL pragma order**: `busy_timeout` **must** be set before `journal_mode=WAL`.

Read the full rationale in `CLAUDE.md` — the invariants are load-bearing.

## Commit and PR style

- **Commit messages**: imperative mood, concise subject (<70 chars), body explaining *why* when the diff alone doesn't.
- **Branch naming**: `fix/...`, `feat/...`, `refactor/...`, `docs/...`.
- **PR description**: what changed, why, and how you tested it. Link the issue if one exists.

## Security issues

**Do not open a public issue for a security vulnerability.** Email the maintainer at [medewanouleonce@gmail.com](mailto:medewanouleonce@gmail.com) with details and a reproduction. We'll respond within 72 hours and coordinate a disclosure timeline.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
