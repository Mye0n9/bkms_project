# Metric Studio

A CLI-based NL2SQL agent for time-series securities screening. Type a natural-language query about stocks — the system resolves your intent, asks clarifying questions if needed, generates PostgreSQL SQL, executes it against a live database, and renders the results as a formatted table.

**Team:** Rian Kim, Myeongseop Kim, Hyeonggeun Jeon

---

## What It Does

```
> Find overbought stocks

[CLARIFY] How many trading days for the RSI window? (common: 14)
> 14
[CLARIFY] What RSI value defines overbought? (common: 70)
> 70

Analyzing query... → Generating SQL... → Executing query...

 ticker │ xymd       │ rsi_value │ clos
────────┼────────────┼───────────┼──────────
 NVDA   │ 2026-05-30 │ 83.21     │ 1087.40
 META   │ 2026-05-30 │ 79.45     │ 512.30
 ...
47 rows returned.

[r] Refine  [n] New query  [q] Quit:
```

---

## Architecture

The system is a 6-stage pipeline. Every stage reads from and writes to a shared `AgentState` dict.

```
User query
    │
    ▼
┌──────────────┐
│  UNDERSTAND  │  Claude matches the query to a metric pattern and extracts
│              │  any parameters already stated in the query
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   CLARIFY    │  For each missing required parameter, asks the user directly
│              │  in the terminal. Retries on invalid input.
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   SPECIFY    │  Validates all parameters are resolved. Builds a MetricSpec
│              │  (strategy, function/table name, resolved params).
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   GENERATE   │  Claude receives the MetricSpec + schema context + few-shot
│              │  SQL examples and writes the parameterized PostgreSQL query.
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   EXECUTE    │  Runs the SQL against PostgreSQL via SQLAlchemy. On failure,
│              │  passes the error back to GENERATE for one self-correction retry.
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   PRESENT    │  Renders the result as a rich table. Shows row count,
│              │  truncates display at 100 rows.
└──────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| LLM framework | LangChain 0.3.x |
| LLM | Claude (claude-sonnet-4-6 via `langchain-anthropic`) |
| Database | PostgreSQL 16 + TimescaleDB |
| DB client | SQLAlchemy 2.x + psycopg2 |
| Data | pandas |
| CLI rendering | rich |
| Config | pydantic-settings |
| Prompts | Jinja2 templates |
| Metric catalog | YAML |
| Tests | pytest + pytest-mock |

---

## Database

### Source Tables (pre-existing)

| Table | Description |
|---|---|
| `tickers` | Master table — ~8,000 NYSE/NASDAQ tickers with listing status |
| `daily_prices` | TimescaleDB hypertable — 10 years of OHLCV data, chunked by 3-month intervals |
| `sync_status` | Collection status per ticker |

### Precomputed Tables (created by migrations)

Populated once by the batch job, then refreshed nightly:

| Table | Contents |
|---|---|
| `daily_ma` | 5 / 10 / 20 / 50 / 200-day moving averages |
| `daily_bb` | Bollinger Bands — MA20, upper/lower band, bandwidth, %B |
| `daily_atr` | True Range and 14-day Average True Range |
| `daily_obv` | Cumulative On-Balance Volume |

### PostgreSQL Functions (on-demand computation)

Called directly in generated SQL with parameter values substituted:

| Function | Returns |
|---|---|
| `calc_rsi(window, as_of)` | RSI for all active tickers |
| `calc_macd(fast, slow, signal, as_of)` | MACD line, signal line, histogram |
| `calc_momentum(window, as_of)` | Price momentum % over N days |
| `calc_rolling_return(window, as_of)` | Rolling return % over N days |
| `calc_volatility(window, as_of)` | Annualized volatility % |
| `calc_pv_divergence(window, as_of)` | Price-volume divergence flag |

---

## Metric Catalog

Ten screening patterns defined in `catalog/metrics.yaml`. Each entry specifies the strategy, the underlying function or table, and clarifying questions for any unresolved parameters.

| Pattern | Strategy | Description |
|---|---|---|
| `rsi_overbought` | pg_function | RSI above threshold |
| `rsi_oversold` | pg_function | RSI below threshold |
| `macd_crossover` | pg_function | MACD line crosses signal line |
| `momentum_screen` | pg_function | Price momentum over N days |
| `rolling_return_rank` | pg_function | Top performers by rolling return |
| `volatility_filter` | pg_function | High or low annualized volatility |
| `pv_divergence` | pg_function | Price trend diverges from volume trend |
| `moving_average_cross` | precomputed_table | Short MA crosses long MA |
| `bollinger_breakout` | precomputed_table | Price breaks Bollinger Band |
| `atr_filter` | precomputed_table | High or low ATR relative to price |

---

## Project Structure

```
bkms_project/
├── metric_studio/
│   ├── catalog/
│   │   └── metrics.yaml            # 10 metric pattern definitions
│   ├── db/
│   │   ├── migrations/
│   │   │   ├── 001_create_daily_ma.sql
│   │   │   ├── 002_create_daily_bb.sql
│   │   │   ├── 003_create_daily_atr.sql
│   │   │   ├── 004_create_daily_obv.sql
│   │   │   └── 005_create_pg_functions.sql
│   │   └── batch/
│   │       └── update_precomputed.py   # Nightly refresh script
│   ├── pipeline/
│   │   ├── state.py                # AgentState TypedDict
│   │   ├── understand.py           # UNDERSTAND stage (LLM)
│   │   ├── clarify.py              # CLARIFY stage (terminal I/O)
│   │   ├── specify.py              # SPECIFY stage (catalog lookup)
│   │   ├── generate.py             # GENERATE stage (LLM → SQL)
│   │   ├── execute.py              # EXECUTE stage (SQLAlchemy)
│   │   └── present.py              # PRESENT stage (rich table)
│   ├── prompts/
│   │   ├── understand.jinja2       # Prompt for metric matching
│   │   ├── generate.jinja2         # Prompt for SQL generation
│   │   └── few_shots/
│   │       └── examples.yaml       # Per-pattern SQL examples
│   ├── tests/
│   │   ├── test_specify.py         # 10 tests
│   │   ├── test_understand.py      # 2 tests
│   │   ├── test_clarify.py         # 3 tests
│   │   ├── test_generate.py        # 2 tests
│   │   ├── test_execute.py         # 3 tests
│   │   └── test_integration.py     # 1 end-to-end test
│   ├── config.py                   # Pydantic-settings loader
│   ├── main.py                     # CLI REPL entry point
│   ├── requirements.txt
│   ├── .env.example
│   └── RUNBOOK.md                  # Full setup and operation guide
├── docs/
│   └── superpowers/
│       ├── specs/2026-06-01-metric-studio-design.md
│       └── plans/2026-06-01-metric-studio-implementation.md
├── bkms_proposal.md
├── db_spec_only.md
└── README.md
```

---

## Quick Start

See [`metric_studio/RUNBOOK.md`](metric_studio/RUNBOOK.md) for the full setup guide.

```bash
# 1. Configure credentials
cp metric_studio/.env.example metric_studio/.env
# edit metric_studio/.env with your values

# 2. Install dependencies
conda activate bkms
pip install -r metric_studio/requirements.txt

# 3. Start PostgreSQL (WSL2)
sudo service postgresql start

# 4. Apply migrations (one-time)
psql -U jhg_user -d your_db_name -f metric_studio/db/migrations/001_create_daily_ma.sql
psql -U jhg_user -d your_db_name -f metric_studio/db/migrations/002_create_daily_bb.sql
psql -U jhg_user -d your_db_name -f metric_studio/db/migrations/003_create_daily_atr.sql
psql -U jhg_user -d your_db_name -f metric_studio/db/migrations/004_create_daily_obv.sql
psql -U jhg_user -d your_db_name -f metric_studio/db/migrations/005_create_pg_functions.sql

# 5. Populate precomputed tables (one-time, ~5–15 min)
cd metric_studio && conda run -n bkms python -m db.batch.update_precomputed --full

# 6. Run tests
conda run -n bkms pytest tests/ -v   # expects 21 passed

# 7. Launch
conda run -n bkms python main.py
```

---

## Running Tests

```bash
cd metric_studio
conda run -n bkms pytest tests/ -v
```

All tests mock the LLM and database — no live credentials needed.

```
21 passed in 1.07s
```
