# Metric Studio — Design Specification

**Date:** 2026-06-01
**Team:** Rian Kim, Myeongseop Kim, Hyeonggeun Jeon
**Project:** NL2SQL Agent for Time-Series Securities Data

---

## 1. Overview

Metric Studio is a CLI-based NL2SQL agent that lets users explore time-series securities data through natural language. The agent detects ambiguity in user queries, asks clarifying questions to resolve missing parameters, and generates schema-aware SQL that is executed against a live PostgreSQL + TimescaleDB instance.

The system is implemented as a **6-stage LCEL pipeline** using LangChain, with a YAML-based metric catalog as the single source of truth for pattern definitions.

---

## 2. Architecture

### 2.1 Pipeline Stages

```
User types query
      │
      ▼
┌─────────────┐
│  UNDERSTAND │  Extract intent, match metric, identify resolved params
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   CLARIFY   │◄─── loops until all required params are resolved
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SPECIFY   │  Construct MetricSpec (strategy, table/function, params)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  GENERATE   │  MetricSpec → schema-aware parameterized SQL
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   EXECUTE   │  Run SQL against PostgreSQL via SQLAlchemy
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   PRESENT   │  Render result table in terminal (rich)
└─────────────┘
       │
       ▼
  User feedback? → loop back to CLARIFY or UNDERSTAND
```

### 2.2 Shared State

All stages read and write a single `AgentState` dict passed through the pipeline:

```python
class AgentState(TypedDict):
    raw_query: str           # original user input
    intent: str              # extracted intent description
    metric_id: str | None    # matched catalog entry id
    resolved_params: dict    # params confirmed so far
    unresolved_params: list  # params still needing clarification
    metric_spec: dict | None # finalized MetricSpec
    sql: str | None          # generated SQL
    result: DataFrame | None # query result
    conversation: list       # message history for session memory
```

### 2.3 Stage Responsibilities

**UNDERSTAND** — LLM call. Receives the raw query and the catalog's `id`/`aliases` list as context. Outputs `intent`, `metric_id` (best match or `None`), and any `resolved_params` already present in the query (e.g., "14-day RSI" resolves `window=14` immediately).

**CLARIFY** — No LLM call for param resolution. Iterates over `unresolved_params`, prints the catalog-defined `question` to the terminal, and waits for user input. If `metric_id` is `None`, presents the catalog list for the user to pick from. Loops until `unresolved_params` is empty.

**SPECIFY** — No LLM call. Pure Python: looks up the catalog entry by `metric_id`, validates all required params are resolved, and constructs the `MetricSpec` dict containing `strategy`, `function` or `table`, and `resolved_params`. Fails fast with a clear error message if anything is missing.

**GENERATE** — LLM call. Receives the `MetricSpec`, the full PostgreSQL schema (tickers + daily_prices + relevant precomputed table or function signature), and few-shot SQL examples from the catalog. Outputs parameterized SQL using named binding variables (no string interpolation). On query execution error, receives the error message and attempts one self-correction.

**EXECUTE** — No LLM call. Uses SQLAlchemy to execute the SQL against the live PostgreSQL instance with bound params. Returns a `pandas.DataFrame`. On error, routes back to GENERATE with the error message for one self-correction attempt.

**PRESENT** — No LLM call. Renders the DataFrame as a formatted table using `rich`. Displays row count and column headers. Prompts the user: refine the current query, start a new one, or exit.

---

## 3. Database Layer

### 3.1 Existing Schema (unchanged)

The baseline schema is already live. No modifications are made to these tables.

| Table | Description |
|---|---|
| `tickers` | Ticker master: NYSE/NASDAQ/AMS stocks and ETFs |
| `daily_prices` | TimescaleDB hypertable: 10 years of daily OHLCV, 3-month chunk partitioning, auto-compression after 3 months |
| `sync_status` | Daily batch collection status per ticker |

### 3.2 New: Precomputed Metric Tables

Populated by a nightly batch job (`db/batch/update_precomputed.py`) that runs after `daily_prices` is updated. Each table joins with `tickers` on `ticker_id`.

| Table | Contents | Migration file |
|---|---|---|
| `daily_ma` | 5/10/20/50/200-day moving averages of `clos` | `001_create_daily_ma.sql` |
| `daily_bb` | Bollinger Bands: 20-day MA, upper/lower bands (2σ) | `002_create_daily_bb.sql` |
| `daily_atr` | Average True Range, 14-day standard | `003_create_daily_atr.sql` |
| `daily_obv` | On-Balance Volume, cumulative | `004_create_daily_obv.sql` |

### 3.3 New: PostgreSQL Functions

Called at query time with user-supplied binding variables. Defined in `005_create_pg_functions.sql`.

| Function | Params | Description |
|---|---|---|
| `calc_rsi(ticker_id, window, threshold)` | window (int), threshold (numeric) | RSI over rolling window, filtered by threshold |
| `calc_macd(ticker_id, fast, slow, signal)` | fast, slow, signal (int) | MACD line and signal line |
| `calc_momentum(ticker_id, window)` | window (int) | Price momentum over N trading days |
| `calc_rolling_return(ticker_id, window)` | window (int) | Rolling return over N trading days |
| `calc_volatility(ticker_id, window)` | window (int) | Rolling standard deviation of returns |
| `calc_pv_divergence(ticker_id, window)` | window (int) | Price-volume divergence detection |

### 3.4 Setup Sequence

```
# One-time baseline restore (already done if DB is live)
psql -U jhg_user -d <dbname> -f bkms_db.sql

# Run migrations once to extend the schema
psql -U jhg_user -d <dbname> -f db/migrations/001_create_daily_ma.sql
psql -U jhg_user -d <dbname> -f db/migrations/002_create_daily_bb.sql
psql -U jhg_user -d <dbname> -f db/migrations/003_create_daily_atr.sql
psql -U jhg_user -d <dbname> -f db/migrations/004_create_daily_obv.sql
psql -U jhg_user -d <dbname> -f db/migrations/005_create_pg_functions.sql

# Populate precomputed tables for the first time
python db/batch/update_precomputed.py --full
```

---

## 4. Metric Catalog

The catalog lives at `catalog/metrics.yaml` and is the single source of truth shared by the SPECIFY and GENERATE stages. Adding a new pattern requires only a new YAML entry — no pipeline code changes.

### 4.1 Entry Structure

```yaml
- id: rsi_overbought
  display_name: "RSI Overbought"
  aliases: ["overbought", "RSI high", "relative strength index overbought"]
  description: "Stocks where RSI exceeds a threshold, signaling overbought condition"
  strategy: pg_function          # pg_function | precomputed_table
  function: calc_rsi             # used when strategy: pg_function
  params:
    window:
      type: integer
      default: 14
      clarify_if_missing: true
      question: "How many trading days for the RSI window? (common: 14)"
    threshold:
      type: numeric
      default: 70
      clarify_if_missing: true
      question: "What RSI threshold defines overbought? (common: 70)"

- id: moving_average_cross
  display_name: "Moving Average Crossover"
  aliases: ["MA cross", "golden cross", "death cross", "moving average signal"]
  description: "Stocks where a short-term MA crosses above or below a long-term MA"
  strategy: precomputed_table    # used when strategy: precomputed_table
  table: daily_ma
  params:
    short_window:
      type: integer
      default: 50
      clarify_if_missing: true
      question: "Which short-term MA window? (common: 5, 10, 20, 50)"
    long_window:
      type: integer
      default: 200
      clarify_if_missing: true
      question: "Which long-term MA window? (common: 50, 100, 200)"
    direction:
      type: enum
      values: [above, below]
      clarify_if_missing: true
      question: "Should the short MA cross above (bullish) or below (bearish) the long MA?"
```

### 4.2 Initial Catalog (10 patterns at launch)

| id | Strategy | Display name |
|---|---|---|
| `rsi_overbought` | pg_function | RSI Overbought |
| `rsi_oversold` | pg_function | RSI Oversold |
| `macd_crossover` | pg_function | MACD Crossover |
| `momentum_screen` | pg_function | Momentum Screen |
| `rolling_return_rank` | pg_function | Rolling Return Rank |
| `volatility_filter` | pg_function | Volatility Filter |
| `pv_divergence` | pg_function | Price-Volume Divergence |
| `moving_average_cross` | precomputed_table | Moving Average Crossover |
| `bollinger_breakout` | precomputed_table | Bollinger Band Breakout |
| `atr_filter` | precomputed_table | ATR-Based Volatility Filter |

---

## 5. CLI Interface

### 5.1 Entry Point

```bash
python main.py
```

### 5.2 Session Flow

```
Metric Studio — NL2SQL Agent for Securities Time-Series
Type your query, or 'exit' to quit.

> Find overbought stocks
[CLARIFY] How many trading days for the RSI window? (common: 14)
> 14
[CLARIFY] What RSI threshold defines overbought? (common: 70)
> 70
[GENERATE] Generating SQL...
[EXECUTE] Running query...

┌──────────┬────────────┬───────────┬──────────┐
│ ticker   │ xymd       │ rsi_value │ clos     │
├──────────┼────────────┼───────────┼──────────┤
│ AAPL     │ 2026-05-30 │ 74.3      │ 189.50   │
│ NVDA     │ 2026-05-30 │ 81.2      │ 875.20   │
└──────────┴────────────┴───────────┴──────────┘
12 rows returned.

[r] Refine query  [n] New query  [q] Quit:
```

### 5.3 Session Memory

The `conversation` list in `AgentState` holds the full message history for the current session. Follow-up queries ("now filter by volume > 1M") are resolved in context. Memory is not persisted across sessions.

---

## 6. Project Structure

```
metric_studio/
├── catalog/
│   └── metrics.yaml
├── db/
│   ├── migrations/
│   │   ├── 001_create_daily_ma.sql
│   │   ├── 002_create_daily_bb.sql
│   │   ├── 003_create_daily_atr.sql
│   │   ├── 004_create_daily_obv.sql
│   │   └── 005_create_pg_functions.sql
│   └── batch/
│       └── update_precomputed.py
├── pipeline/
│   ├── state.py
│   ├── understand.py
│   ├── clarify.py
│   ├── specify.py
│   ├── generate.py
│   ├── execute.py
│   └── present.py
├── prompts/
│   ├── understand.jinja2
│   ├── generate.jinja2
│   └── few_shots/
│       └── examples.yaml
├── config.py
├── main.py
└── requirements.txt
```

---

## 7. Configuration

Environment variables (`.env`):

```
ANTHROPIC_API_KEY=...
DB_HOST=...
DB_PORT=5432
DB_NAME=...
DB_USER=...
DB_PASSWORD=...
LLM_MODEL=claude-sonnet-4-6
```

---

## 8. Key Constraints

- SQL binding variables must use named params via SQLAlchemy — no string interpolation.
- GENERATE stage self-corrects at most once on execution error to avoid infinite loops.
- New metric patterns are added by appending a YAML entry and the corresponding SQL migration or function — no pipeline code changes required.
- `bkms_db.sql` is the baseline snapshot and is never read at runtime; the system connects only to the live PostgreSQL instance.
- Session memory is in-process only; no persistence across CLI sessions.
