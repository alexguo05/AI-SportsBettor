# VM Status and Operations Runbook

This document gives another developer or AI enough context to inspect, deploy,
restart, and troubleshoot the production VM safely.

## Production locations

- GCP project: `ai-sports-bettor`
- VM: `sportsbettor-ingest-01`
- Zone: `us-central1-a`
- Repository: `/opt/ai-sports-bettor`
- Python environment: `/opt/ai-sports-bettor/.venv`
- Runtime environment file: `/etc/ai-sports-bettor/x.env`
- Repository/runtime owner: `sportsbettor`
- Human SSH user used during setup: `alexguo`

Never print or copy the contents of `x.env` into chat, logs, Git, or frontend
code. It contains production credentials.

## Current production snapshot

Database-verified on August 5, 2026:

- Alembic revision: `20260805_17`
- Entity-bank versions: 1
- News enrichments:
  - 2,315 completed
  - 1,067 completed with warnings
- Entity mentions:
  - 6,599 resolved
  - 228 ambiguous
  - 1,409 unresolved
  - 47 ignored
- Polymarket:
  - 182 events
  - 2,319 markets
  - 4,638 outcome tokens
  - 2,117 market classifications
- Queue:
  - 3,382 completed news-enrichment jobs
  - 3,382 completed news-resolution jobs
  - 182 completed market-resolution jobs
  - 4 dead news-enrichment jobs
  - 22 dead market-resolution jobs
  - No pending backlog was present in this snapshot

The four dead news jobs contain model responses truncated before valid JSON was
completed. Most dead market jobs involve untyped group items or invalid
verbatim evidence in newly discovered grouped markets. These failures are
isolated; they do not block completed jobs.

The migration state was verified directly from Cloud SQL. Direct VM inspection
was unavailable during the latest documentation update because the active
gcloud identity lacked `compute.instances.get`. Confirm live service state from
the VM before assuming every service is running.

Last known deployed application commit: `7fca71e` (August 5, Kalshi
collectors). Use the commands below to confirm whether the VM has pulled the
latest commit.

## Expected services

The complete production service set is:

- `x-ingestion.service`
  - Pulls X recent-search data.
  - Normal cycle interval: 10 seconds.
- `polymarket-gamma.service`
  - Pulls Polymarket event and market structure.
  - Normal cycle interval: 15 minutes.
- `polymarket-order-books.service`
  - Pulls CLOB order books.
  - Normal cycle interval: 10 seconds.
- `polymarket-resolutions.service`
  - Reconciles settlement outcomes for markets that left the open Gamma feed.
  - Normal cycle interval: 1 hour.
  - Deployed August 4.
- `polymarket-trades.service`
  - Pulls executed trade prints from the public data API.
  - Normal cycle interval: 60 seconds.
  - Deployed August 4.
- `kalshi-markets.service`
  - Pulls Kalshi series/event/market structure plus settlement results
    (result, settlement value, settlement timestamp) via a per-series settled
    sweep. See `docs/KALSHI_INGESTION.md`.
  - Normal cycle interval: 15 minutes.
  - Deployed August 5.
- `kalshi-order-books.service`
  - Pulls Kalshi order books for active markets in batches of 100.
  - Normal cycle interval: 15 seconds.
  - Deployed August 5. Requires `kalshi-markets` to have run.
- `kalshi-trades.service`
  - Pulls the exchange-wide Kalshi trade feed and keeps trades for tracked
    markets.
  - Normal cycle interval: 60 seconds.
  - Deployed August 5. Requires `kalshi-markets` to have run.
  - All three Kalshi services need `KALSHI_API_KEY_ID` and
    `KALSHI_PRIVATE_KEY_PATH` in the systemd environment file.
- `news-market-linking.service` + `news-market-linking.timer`
  - Oneshot batch triggered hourly by the timer (not a long-running poller).
  - Rebuilds tweet-market links, then labels links whose two-hour reaction
    window has elapsed. See `docs/LINKING.md`.
  - Deployed August 4.
- `job-worker.service`
  - Processes enrichment and entity-resolution jobs.
  - Uses 10 worker threads, 2 video slots, and 5 market-event slots.
  - Checks PostgreSQL for work every second.
- `entity-bank-nflverse.service`
  - Checks nflverse once per day.
  - Writes nothing when the source snapshot is unchanged.

X, Gamma, CLOB, and the job worker were confirmed active during the initial
August 3 rollout. The nflverse poller code and service definition exist, but
its current VM status should be verified explicitly.

## Check service status

Run on the VM:

```bash
systemctl list-units --type=service --all --no-pager \
  'x-*' \
  'polymarket-*' \
  'kalshi-*' \
  'job-worker*' \
  'entity-bank-*'
```

Check one service:

```bash
sudo systemctl status job-worker.service --no-pager
```

Check whether all expected services are enabled:

```bash
systemctl list-unit-files --type=service --no-pager \
  'x-*' \
  'polymarket-*' \
  'job-worker*' \
  'entity-bank-*'
```

## View logs

Recent logs:

```bash
sudo journalctl -u job-worker.service -n 100 --no-pager
```

Follow logs without stopping the service:

```bash
sudo journalctl -u job-worker.service -f
```

Pressing `Ctrl+C` while following logs stops only `journalctl`; the systemd
service continues running.

Logs for the other services:

```bash
sudo journalctl -u x-ingestion.service -n 100 --no-pager
sudo journalctl -u polymarket-gamma.service -n 100 --no-pager
sudo journalctl -u polymarket-order-books.service -n 100 --no-pager
sudo journalctl -u entity-bank-nflverse.service -n 100 --no-pager
```

## Pull code using the deploy key

The repository is owned by `sportsbettor`. Run Git as that account so Git uses
its home directory and configured GitHub deploy key:

```bash
sudo -u sportsbettor -H git \
  -C /opt/ai-sports-bettor \
  pull --ff-only origin main
```

Verify the deployed commit:

```bash
sudo -u sportsbettor -H git \
  -C /opt/ai-sports-bettor \
  rev-parse --short HEAD
```

Test GitHub SSH authentication if pull fails:

```bash
sudo -u sportsbettor -H ssh -T git@github.com
```

Do not run `sudo git pull` as root. That creates ownership problems and may not
use the intended deploy key.

GitHub may print that the repository moved from the old remote URL. Redirected
pulls currently work, but do not change Git configuration casually during an
incident.

## Standard deployment sequence

Only deploy committed, reviewed code. Stop long-running processes before a
schema migration:

```bash
sudo systemctl stop job-worker.service
sudo systemctl stop x-ingestion.service
sudo systemctl stop polymarket-gamma.service
sudo systemctl stop polymarket-order-books.service
sudo systemctl stop entity-bank-nflverse.service
```

Pull as the repository owner:

```bash
sudo -u sportsbettor -H git \
  -C /opt/ai-sports-bettor \
  pull --ff-only origin main
```

Install or refresh project dependencies inside the project virtual
environment:

```bash
sudo -u sportsbettor -H bash -lc '
  set -euo pipefail
  cd /opt/ai-sports-bettor
  .venv/bin/python -m pip install -e .
'
```

Apply migrations using the production environment:

```bash
sudo -u sportsbettor -H bash -lc '
  set -euo pipefail
  set -a
  source /etc/ai-sports-bettor/x.env
  set +a

  cd /opt/ai-sports-bettor
  export PYTHONPATH=/opt/ai-sports-bettor

  .venv/bin/alembic upgrade head
  .venv/bin/alembic current
'
```

Review the migration output before restarting services. Then:

```bash
sudo systemctl daemon-reload
sudo systemctl start entity-bank-nflverse.service
sudo systemctl start job-worker.service
sudo systemctl start x-ingestion.service
sudo systemctl start polymarket-gamma.service
sudo systemctl start polymarket-order-books.service
```

Verify every service:

```bash
sudo systemctl status entity-bank-nflverse.service --no-pager
sudo systemctl status job-worker.service --no-pager
sudo systemctl status x-ingestion.service --no-pager
sudo systemctl status polymarket-gamma.service --no-pager
sudo systemctl status polymarket-order-books.service --no-pager
```

If a service is not installed, its `start` command will fail with “unit not
found.” Install that unit from the definitions in `docs/ENTITY_BANK.md`,
`docs/JOB_QUEUE.md`, or `docs/POLYMARKET_INGESTION.md`.

## Important executable paths

Do not use the VM's global `python`, `pip`, `alembic`, or Ubuntu packages.

Use:

```text
/opt/ai-sports-bettor/.venv/bin/python
/opt/ai-sports-bettor/.venv/bin/pip
/opt/ai-sports-bettor/.venv/bin/alembic
```

Running `alembic` or `python` directly from the human user's home previously
failed because those global commands are not installed. Installing Ubuntu's
Alembic package is not the fix.

## Initial production setup already completed

These one-time steps were completed during the August 3 rollout:

1. Cloud SQL migrated from `20260801_10` to `20260803_12`.
2. The nflverse canonical bank was populated for the 2026 season.
3. Existing data was seeded into `job_outbox`.
4. The full enrichment and entity-resolution backlog was processed.

Do not rerun the initial full queue seed during a normal restart. New X and
Gamma writes enqueue their own jobs transactionally.

Rerun a seed only when intentionally backfilling a new enrichment/extractor
version and after reviewing the version and idempotency behavior.

## Check queue status from the VM

Run this read-only inspection:

```bash
sudo -u sportsbettor -H bash -lc '
  set -euo pipefail
  set -a
  source /etc/ai-sports-bettor/x.env
  set +a

  cd /opt/ai-sports-bettor
  export PYTHONPATH=/opt/ai-sports-bettor

  .venv/bin/python -c "
from pathlib import Path
from sqlalchemy import text
from src.db.engine import create_database_resources

resources = create_database_resources(Path(\"src\"))
try:
    with resources.engine.connect() as connection:
        rows = connection.execute(text(
            \"SELECT job_type, status, count(*) \"
            \"FROM job_outbox \"
            \"GROUP BY job_type, status \"
            \"ORDER BY job_type, status\"
        ))
        for row in rows:
            print(tuple(row))
finally:
    resources.close()
"
'
```

Expected healthy behavior:

- New jobs move from `pending` to `leased` to `completed`.
- Successful news enrichment creates a `resolve_news` job.
- A small number of retries may occur.
- Repeated `dead` growth requires investigation.
- Old dead jobs do not prevent unrelated jobs from processing.

## Restart behavior

Restart one service:

```bash
sudo systemctl restart job-worker.service
```

The queue is durable:

- Pending jobs remain in PostgreSQL.
- Leased jobs become claimable again after their lease expires if the worker
  dies.
- Completed writes are idempotent.
- A normal `SIGTERM` stops new claims and waits for in-flight work.

Collectors also preserve their checkpoints, so a normal restart resumes from
the stored state rather than rebuilding everything.

## Common failures and fixes

### `ModuleNotFoundError: No module named 'anthropic'`

Cause: the VM virtual environment was not refreshed after pulling new
dependencies.

Fix:

```bash
sudo systemctl stop job-worker.service

sudo -u sportsbettor -H bash -lc '
  set -euo pipefail
  cd /opt/ai-sports-bettor
  .venv/bin/python -m pip install -e .
'

sudo systemctl start job-worker.service
```

### `ANTHROPIC_API_KEY is not configured`

Cause: the systemd environment file does not contain the key.

Fix:

```bash
sudo systemctl stop job-worker.service
sudoedit /etc/ai-sports-bettor/x.env
sudo systemctl start job-worker.service
```

Add the key inside the editor. Never paste it into chat or a shared terminal
transcript.

### `Command 'alembic' not found` or `Command 'python' not found`

Cause: a global executable was used.

Fix: use `.venv/bin/alembic` and `.venv/bin/python` from
`/opt/ai-sports-bettor`.

### `fatal: detected dubious ownership`

Cause: Git was run as a human or root account against a repository owned by
`sportsbettor`.

Fix:

```bash
sudo -u sportsbettor -H git \
  -C /opt/ai-sports-bettor \
  pull --ff-only origin main
```

Do not add a global `safe.directory` exception unless repository ownership is
intentionally changed.

### Worker is “active” but database counts do not move

An immediate systemd status can briefly show active before the process exits
and enters a restart loop.

Always inspect both:

```bash
sudo systemctl status job-worker.service --no-pager
sudo journalctl -u job-worker.service -n 100 --no-pager
```

Then check queue counts.

## Service definitions

Canonical examples are documented in:

- X/Gamma/CLOB: `docs/POLYMARKET_INGESTION.md` and X ingestion documentation
- Job worker: `docs/JOB_QUEUE.md`
- nflverse poller: `docs/ENTITY_BANK.md`

When changing a unit:

```bash
sudo systemctl daemon-reload
sudo systemctl restart <service-name>
sudo systemctl status <service-name> --no-pager
```

## Safety reminders

- Do not commit `.env` files or credentials.
- Do not run database migrations from a laptop unless intentionally targeting
  production with approved credentials.
- Do not rerun full queue seeding as a routine restart step.
- Do not delete dead jobs before recording and understanding their errors.
- Do not use `--apply` commands casually.
- Do not downgrade Alembic in production without understanding which tables or
  data the downgrade removes.
- Prefer read-only inspection before changing queue or database state.
