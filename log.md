# Metric Studio — Project Log

## What Was Done

### Phase 1: Design & Planning

**Explored project context:**
- Read `bkms_proposal.md`: defines Metric Studio as an LLM-based NL2SQL agent for time-series securities screening (NYSE/NASDAQ, 10 years OHLCV)
- Read `db_spec_only.md`: existing schema has 3 tables — `tickers`, `daily_prices` (TimescaleDB hypertable), `sync_status`
- Inspected `bkms_db.sql`: PostgreSQL 16 + TimescaleDB dump, data is already live and chunked in 3-month intervals

**Clarified key decisions:**
- Interface: CLI chatbot (terminal REPL)
- LLM framework: LangChain (0.3.x)
- Architecture: Option A — LCEL pipeline with explicit state machine
- Metric catalog: moderate scope (10 patterns at launch), extensible via YAML

**Designed the system** (approved by user, section by section):
1. 6-stage pipeline: `UNDERSTAND → CLARIFY → SPECIFY → GENERATE → EXECUTE → PRESENT`
2. Shared `AgentState` TypedDict passed through all stages
3. Database layer: 4 precomputed tables (`daily_ma`, `daily_bb`, `daily_atr`, `daily_obv`) + 6 PostgreSQL functions (`calc_rsi`, `calc_macd`, `calc_momentum`, `calc_rolling_return`, `calc_volatility`, `calc_pv_divergence`)
4. Metric catalog: YAML file with 10 patterns, each defining strategy (`pg_function` or `precomputed_table`), params, and clarifying questions
5. CLI REPL in `main.py` with session memory and refine/new/quit loop

**Wrote and committed design documents:**
- `docs/superpowers/specs/2026-06-01-metric-studio-design.md` — full design spec
- `docs/superpowers/plans/2026-06-01-metric-studio-implementation.md` — 13-task implementation plan with full code for every step

---

### Phase 2: Implementation (Subagent-Driven, Tasks 1–2 complete)

**Task 1: Project scaffolding** ✅ (commits: `ae3222b`, `7111a71`)
- Created `metric_studio/` directory with all subdirs: `catalog/`, `db/migrations/`, `db/batch/`, `pipeline/`, `prompts/few_shots/`, `tests/`
- `metric_studio/requirements.txt` — 15 dependencies (langchain stack, sqlalchemy, psycopg2, pandas, rich, pyyaml, jinja2, pytest)
- `metric_studio/.env.example` — template with 7 env vars
- `metric_studio/__init__.py`, `pipeline/__init__.py`, `tests/__init__.py`
- Added Python patterns to `.gitignore`

**Task 2: Config module** ✅ (commit: `e5fc517`)
- `metric_studio/config.py` — pydantic-settings `Settings` class with `db_url` property and module-level `settings` singleton

---

## What Still Needs to Be Done

### Task 3: DB migrations — precomputed tables
Create 4 SQL files in `metric_studio/db/migrations/`:
- `001_create_daily_ma.sql` — `daily_ma` table (5/10/20/50/200-day moving averages)
- `002_create_daily_bb.sql` — `daily_bb` table (Bollinger Bands)
- `003_create_daily_atr.sql` — `daily_atr` table (Average True Range)
- `004_create_daily_obv.sql` — `daily_obv` table (On-Balance Volume)

Then apply them against the live PostgreSQL database.

### Task 4: DB migrations — PostgreSQL functions
Create `metric_studio/db/migrations/005_create_pg_functions.sql` with 6 functions:
- `calc_rsi(p_window, p_as_of)` — RSI for all active tickers
- `calc_macd(p_fast, p_slow, p_signal, p_as_of)` — MACD line + signal
- `calc_momentum(p_window, p_as_of)` — price momentum %
- `calc_rolling_return(p_window, p_as_of)` — rolling return %
- `calc_volatility(p_window, p_as_of)` — annualized volatility %
- `calc_pv_divergence(p_window, p_as_of)` — price-volume divergence flag

Apply against the live PostgreSQL database and smoke-test.

### Task 5: Batch update job
Create `metric_studio/db/batch/update_precomputed.py`:
- Script to populate/refresh the 4 precomputed tables from `daily_prices`
- Supports `--full` flag (full history recompute) and incremental mode (latest date only)
- Run `--full` once to populate tables for the first time

### Task 6: AgentState + metric catalog
Create:
- `metric_studio/pipeline/state.py` — `AgentState` TypedDict with fields: `raw_query`, `intent`, `metric_id`, `resolved_params`, `unresolved_params`, `metric_spec`, `sql`, `result`, `execution_error`, `conversation`
- `metric_studio/catalog/metrics.yaml` — 10 metric patterns with id, aliases, strategy, params, and clarifying questions

### Task 7: Catalog loader + SPECIFY stage (TDD)
Create `metric_studio/pipeline/specify.py` and `metric_studio/tests/test_specify.py`:
- `load_catalog()` — loads YAML from `catalog/metrics.yaml`
- `find_metric(catalog, metric_id)` — lookup by id
- `get_unresolved_params(metric, resolved)` — returns list of param names still missing
- `specify(state)` — validates all params are resolved and builds `MetricSpec` dict

### Task 8: UNDERSTAND stage
Create:
- `metric_studio/pipeline/understand.py` — LLM call using `ChatAnthropic.with_structured_output(UnderstandOutput)` to match metric and extract resolved params
- `metric_studio/prompts/understand.jinja2` — prompt template listing catalog aliases and asking for metric_id + extracted params

### Task 9: CLARIFY stage (TDD)
Create `metric_studio/pipeline/clarify.py` and `metric_studio/tests/test_clarify.py`:
- Loops over `unresolved_params`, prints catalog question, reads user input
- Handles integer/numeric/enum types and retries on invalid enum input

### Task 10: GENERATE stage
Create:
- `metric_studio/pipeline/generate.py` — LLM call that takes `MetricSpec` + schema context + few-shot examples and returns parameterized SQL
- `metric_studio/prompts/generate.jinja2` — prompt template
- `metric_studio/prompts/few_shots/examples.yaml` — SQL examples for each pattern used in few-shot prompting

### Task 11: EXECUTE stage (TDD)
Create `metric_studio/pipeline/execute.py` and `metric_studio/tests/test_execute.py`:
- Executes SQL against PostgreSQL via SQLAlchemy `text()` with named binding params
- Returns `pandas.DataFrame` on success, sets `execution_error` on failure (for GENERATE self-correction)

### Task 12: PRESENT stage
Create `metric_studio/pipeline/present.py`:
- Renders `DataFrame` as a `rich` table in the terminal
- Shows row count and truncates display to 100 rows

### Task 13: CLI REPL + integration test
Create:
- `metric_studio/main.py` — orchestrates all 6 stages in a REPL loop; includes one self-correction retry on SQL error; session memory via `conversation` list
- `metric_studio/tests/test_integration.py` — wires all stages with mocked LLM and DB, verifies a full query round-trip returns a DataFrame

---

## Key Files Reference

| File | Purpose |
|---|---|
| `bkms_db.sql` | Existing DB dump (baseline, never modified at runtime) |
| `db_spec_only.md` | Schema documentation for existing tables |
| `bkms_proposal.md` | Original project proposal |
| `docs/superpowers/specs/2026-06-01-metric-studio-design.md` | Full design spec |
| `docs/superpowers/plans/2026-06-01-metric-studio-implementation.md` | Implementation plan with full code |
| `metric_studio/catalog/metrics.yaml` | Metric pattern catalog (to be created in Task 6) |
| `metric_studio/config.py` | Environment config (done) |
| `metric_studio/pipeline/` | All 6 pipeline stage modules (Tasks 7–12) |
| `metric_studio/main.py` | CLI entry point (Task 13) |

## Notes

- The live PostgreSQL database is already running with `tickers` and `daily_prices` populated
- Tasks 3–5 require access to the live DB — run SQL migrations with `psql -U jhg_user`
- All pipeline tests mock the LLM (ChatAnthropic) and DB (SQLAlchemy) — no live credentials needed for unit tests
- The full implementation plan with code for every step is at `docs/superpowers/plans/2026-06-01-metric-studio-implementation.md`
