# Metric Studio — Runbook

Step-by-step guide for first-time setup and daily operation.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11+ |
| Conda environment | `bkms` |
| PostgreSQL + TimescaleDB | 16 |
| Anthropic API key | any valid `sk-ant-...` key |

---

## Step 1 — Create `.env`

Copy the example and fill in your values:

```bash
cp metric_studio/.env.example metric_studio/.env
```

Edit `metric_studio/.env`:

```env
ANTHROPIC_API_KEY=sk-ant-your-key-here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db_name
DB_USER=jhg_user
DB_PASSWORD=your_password
LLM_MODEL=claude-sonnet-4-6
```

> All commands below assume you are in `/home/rlaaudtjq0201/bkms_project`.

---

## Step 2 — Install Dependencies

```bash
conda activate bkms
pip install -r metric_studio/requirements.txt
```

Verify:

```bash
python -c "import langchain_anthropic, sqlalchemy, rich; print('OK')"
```

---

## Step 3 — Start PostgreSQL

On WSL2, services do not start automatically:

```bash
sudo service postgresql start
```

Verify the DB is reachable:

```bash
psql -U jhg_user -d your_db_name -c "SELECT COUNT(*) FROM public.tickers;"
```

Expected: a row count (should be several thousand tickers).

---

## Step 4 — Run DB Migrations (one-time)

Apply the 4 precomputed table schemas and 6 metric functions:

```bash
psql -U jhg_user -d your_db_name -f metric_studio/db/migrations/001_create_daily_ma.sql
psql -U jhg_user -d your_db_name -f metric_studio/db/migrations/002_create_daily_bb.sql
psql -U jhg_user -d your_db_name -f metric_studio/db/migrations/003_create_daily_atr.sql
psql -U jhg_user -d your_db_name -f metric_studio/db/migrations/004_create_daily_obv.sql
psql -U jhg_user -d your_db_name -f metric_studio/db/migrations/005_create_pg_functions.sql
```

Expected output for each file: `CREATE TABLE` / `CREATE INDEX` or `CREATE FUNCTION`.

---

## Step 5 — Populate Precomputed Tables (one-time, ~5–15 min)

This reads all of `daily_prices` and computes moving averages, Bollinger Bands, ATR, and OBV for every ticker and date:

```bash
cd metric_studio
conda run -n bkms python -m db.batch.update_precomputed --full
```

Expected output:

```
Full recompute — this may take several minutes...
  Updating daily_ma...
  Updating daily_bb...
  Updating daily_atr...
  Updating daily_obv...
Done.
```

Verify row counts:

```bash
psql -U jhg_user -d your_db_name -c "
SELECT 'daily_ma' AS t, COUNT(*) FROM public.daily_ma
UNION ALL SELECT 'daily_bb', COUNT(*) FROM public.daily_bb
UNION ALL SELECT 'daily_atr', COUNT(*) FROM public.daily_atr
UNION ALL SELECT 'daily_obv', COUNT(*) FROM public.daily_obv;"
```

Expected: all four tables have > 0 rows.

---

## Step 6 — Run Tests

Confirm all 21 unit + integration tests pass before using the CLI:

```bash
cd metric_studio
conda run -n bkms pytest tests/ -v
```

Expected: `21 passed`.

---

## Step 7 — Launch the CLI

```bash
cd metric_studio
conda run -n bkms python main.py
```

You will see:

```
Metric Studio — NL2SQL Agent for Securities Time-Series
Type your query, or 'exit' to quit.

>
```

### Example queries

```
> Find overbought stocks
> Show me stocks with RSI above 70 using a 14-day window
> Which stocks had a golden cross recently?
> Find low volatility stocks under 20% annualized vol
> Show top momentum stocks over 20 days with at least 5% gain
> Find stocks where MACD is bullish
```

### Session controls

After each result:

| Input | Action |
|---|---|
| `r` | Refine — keep conversation history and follow up |
| `n` | New query — clear history and start fresh |
| `q` | Quit |

---

## Nightly Batch (Incremental Update)

After new price data arrives each day, refresh only the latest date:

```bash
cd metric_studio
conda run -n bkms python -m db.batch.update_precomputed
```

This runs in seconds (no `--full` flag = incremental mode, latest date only).

To automate, add to cron:

```bash
# Run at 7am every weekday
0 7 * * 1-5 cd /home/rlaaudtjq0201/bkms_project/metric_studio && conda run -n bkms python -m db.batch.update_precomputed
```

---

## Troubleshooting

### `connection refused` on port 5432
PostgreSQL is not running. Run `sudo service postgresql start`.

### `ValidationError` on settings fields
The `.env` file is missing or in the wrong directory. It must be at `metric_studio/.env`, not the project root.

### `ModuleNotFoundError: No module named 'pipeline'`
Run pytest and main.py from inside the `metric_studio/` directory, not the project root.

### LLM returns SQL with markdown fences (` ```sql ... ``` `)
The GENERATE stage strips these automatically. If raw SQL still fails, the self-correction retry passes the error back to the LLM for a second attempt.

### Precomputed table queries return 0 rows
The batch job has not been run yet, or ran before the migration was applied. Re-run Step 5.

---

## File Reference

| Path | Purpose |
|---|---|
| `metric_studio/.env` | Local credentials (never commit) |
| `metric_studio/config.py` | Pydantic-settings config loader |
| `metric_studio/catalog/metrics.yaml` | 10 metric pattern definitions |
| `metric_studio/db/migrations/` | SQL DDL for tables and functions |
| `metric_studio/db/batch/update_precomputed.py` | Nightly batch populate script |
| `metric_studio/pipeline/` | 6 pipeline stage modules |
| `metric_studio/prompts/` | Jinja2 templates + few-shot SQL examples |
| `metric_studio/tests/` | 21 unit + integration tests |
| `metric_studio/main.py` | CLI entry point |
