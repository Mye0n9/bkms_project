# Metric Studio Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a CLI-based NL2SQL agent that maps natural-language securities screening queries to parameterized PostgreSQL SQL via a 6-stage LangChain LCEL pipeline.

**Architecture:** Six pipeline stages (UNDERSTAND → CLARIFY → SPECIFY → GENERATE → EXECUTE → PRESENT) share a single `AgentState` dict. UNDERSTAND and GENERATE invoke Claude via LangChain; all other stages are pure Python. A YAML metric catalog is the single source of truth for pattern definitions and clarifying questions.

**Tech Stack:** Python 3.11+, LangChain 0.3.x, langchain-anthropic, SQLAlchemy 2.x, psycopg2-binary, PostgreSQL 16 + TimescaleDB, pandas, rich, pyyaml, jinja2, pytest, pytest-mock

---

## File Map

```
metric_studio/
├── catalog/
│   └── metrics.yaml               # 10 metric patterns — source of truth
├── db/
│   ├── migrations/
│   │   ├── 001_create_daily_ma.sql
│   │   ├── 002_create_daily_bb.sql
│   │   ├── 003_create_daily_atr.sql
│   │   ├── 004_create_daily_obv.sql
│   │   └── 005_create_pg_functions.sql
│   └── batch/
│       └── update_precomputed.py  # nightly batch: refresh precomputed tables
├── pipeline/
│   ├── state.py                   # AgentState TypedDict
│   ├── specify.py                 # SPECIFY: catalog lookup + MetricSpec builder
│   ├── understand.py              # UNDERSTAND: LLM metric matcher
│   ├── clarify.py                 # CLARIFY: terminal I/O param loop
│   ├── generate.py                # GENERATE: LLM SQL generator
│   ├── execute.py                 # EXECUTE: SQLAlchemy runner
│   └── present.py                 # PRESENT: rich table renderer
├── prompts/
│   ├── understand.jinja2
│   ├── generate.jinja2
│   └── few_shots/
│       └── examples.yaml          # per-pattern SQL examples for few-shot prompting
├── tests/
│   ├── test_specify.py
│   ├── test_clarify.py
│   ├── test_execute.py
│   └── test_integration.py
├── config.py
├── main.py
└── requirements.txt
```

---

## Task 1: Project Scaffolding

**Files:**
- Create: `metric_studio/requirements.txt`
- Create: `metric_studio/.env.example`
- Create all directories listed in the file map above

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p metric_studio/{catalog,db/migrations,db/batch,pipeline,prompts/few_shots,tests}
touch metric_studio/pipeline/__init__.py metric_studio/tests/__init__.py
```

- [ ] **Step 2: Write requirements.txt**

```
# metric_studio/requirements.txt
langchain>=0.3.0
langchain-core>=0.3.0
langchain-anthropic>=0.3.0
langchain-community>=0.3.0
pydantic>=2.0.0
pydantic-settings>=2.0.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
pandas>=2.0.0
rich>=13.0.0
python-dotenv>=1.0.0
pyyaml>=6.0.0
jinja2>=3.1.0
pytest>=8.0.0
pytest-mock>=3.12.0
```

- [ ] **Step 3: Write .env.example**

```
# metric_studio/.env.example
ANTHROPIC_API_KEY=sk-ant-...
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db_name
DB_USER=jhg_user
DB_PASSWORD=your_password
LLM_MODEL=claude-sonnet-4-6
```

- [ ] **Step 4: Install dependencies**

```bash
cd metric_studio && pip install -r requirements.txt
```

Expected: all packages install without error.

- [ ] **Step 5: Commit**

```bash
git add metric_studio/
git commit -m "feat: scaffold metric_studio project structure"
```

---

## Task 2: Config Module

**Files:**
- Create: `metric_studio/config.py`

- [ ] **Step 1: Write config.py**

```python
# metric_studio/config.py
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    anthropic_api_key: str
    db_host: str
    db_port: int = 5432
    db_name: str
    db_user: str
    db_password: str
    llm_model: str = "claude-sonnet-4-6"

    @property
    def db_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    model_config = {"env_file": ".env"}


settings = Settings()
```

- [ ] **Step 2: Verify it loads without error**

```bash
cd metric_studio && python -c "from config import settings; print(settings.db_url)"
```

Expected: prints a postgres URL (values from your `.env`).

- [ ] **Step 3: Commit**

```bash
git add metric_studio/config.py
git commit -m "feat: add config module with pydantic-settings"
```

---

## Task 3: DB Migrations — Precomputed Tables

**Files:**
- Create: `metric_studio/db/migrations/001_create_daily_ma.sql`
- Create: `metric_studio/db/migrations/002_create_daily_bb.sql`
- Create: `metric_studio/db/migrations/003_create_daily_atr.sql`
- Create: `metric_studio/db/migrations/004_create_daily_obv.sql`

- [ ] **Step 1: Write 001_create_daily_ma.sql**

```sql
-- metric_studio/db/migrations/001_create_daily_ma.sql
CREATE TABLE IF NOT EXISTS public.daily_ma (
    ticker_id INTEGER NOT NULL REFERENCES public.tickers(ticker_id),
    xymd      DATE    NOT NULL,
    ma_5      NUMERIC(19,8),
    ma_10     NUMERIC(19,8),
    ma_20     NUMERIC(19,8),
    ma_50     NUMERIC(19,8),
    ma_200    NUMERIC(19,8),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ticker_id, xymd)
);

CREATE INDEX IF NOT EXISTS daily_ma_xymd_idx ON public.daily_ma (xymd DESC);
```

- [ ] **Step 2: Write 002_create_daily_bb.sql**

```sql
-- metric_studio/db/migrations/002_create_daily_bb.sql
CREATE TABLE IF NOT EXISTS public.daily_bb (
    ticker_id   INTEGER NOT NULL REFERENCES public.tickers(ticker_id),
    xymd        DATE    NOT NULL,
    ma_20       NUMERIC(19,8),
    upper_band  NUMERIC(19,8),
    lower_band  NUMERIC(19,8),
    bandwidth   NUMERIC(19,8),
    pct_b       NUMERIC(19,8),
    created_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ticker_id, xymd)
);

CREATE INDEX IF NOT EXISTS daily_bb_xymd_idx ON public.daily_bb (xymd DESC);
```

- [ ] **Step 3: Write 003_create_daily_atr.sql**

```sql
-- metric_studio/db/migrations/003_create_daily_atr.sql
CREATE TABLE IF NOT EXISTS public.daily_atr (
    ticker_id  INTEGER NOT NULL REFERENCES public.tickers(ticker_id),
    xymd       DATE    NOT NULL,
    true_range NUMERIC(19,8),
    atr_14     NUMERIC(19,8),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ticker_id, xymd)
);

CREATE INDEX IF NOT EXISTS daily_atr_xymd_idx ON public.daily_atr (xymd DESC);
```

- [ ] **Step 4: Write 004_create_daily_obv.sql**

```sql
-- metric_studio/db/migrations/004_create_daily_obv.sql
CREATE TABLE IF NOT EXISTS public.daily_obv (
    ticker_id  INTEGER NOT NULL REFERENCES public.tickers(ticker_id),
    xymd       DATE    NOT NULL,
    obv        BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ticker_id, xymd)
);

CREATE INDEX IF NOT EXISTS daily_obv_xymd_idx ON public.daily_obv (xymd DESC);
```

- [ ] **Step 5: Apply migrations**

```bash
psql -U jhg_user -d $DB_NAME -f metric_studio/db/migrations/001_create_daily_ma.sql
psql -U jhg_user -d $DB_NAME -f metric_studio/db/migrations/002_create_daily_bb.sql
psql -U jhg_user -d $DB_NAME -f metric_studio/db/migrations/003_create_daily_atr.sql
psql -U jhg_user -d $DB_NAME -f metric_studio/db/migrations/004_create_daily_obv.sql
```

Expected: `CREATE TABLE` and `CREATE INDEX` for each file, no errors.

- [ ] **Step 6: Commit**

```bash
git add metric_studio/db/migrations/
git commit -m "feat: add precomputed metric table migrations"
```

---

## Task 4: DB Migrations — PostgreSQL Functions

**Files:**
- Create: `metric_studio/db/migrations/005_create_pg_functions.sql`

Functions take window/period params and `p_as_of DATE` (defaults to CURRENT_DATE). They return metric values for ALL active tickers on the most recent trading day ≤ `p_as_of`. The generated SQL applies threshold filtering via WHERE clause.

- [ ] **Step 1: Write 005_create_pg_functions.sql**

```sql
-- metric_studio/db/migrations/005_create_pg_functions.sql

-- RSI: Relative Strength Index
CREATE OR REPLACE FUNCTION public.calc_rsi(
    p_window INTEGER DEFAULT 14,
    p_as_of  DATE    DEFAULT CURRENT_DATE
)
RETURNS TABLE (ticker_id INTEGER, xymd DATE, rsi_value NUMERIC, clos NUMERIC)
LANGUAGE sql STABLE AS $$
    WITH latest_date AS (
        SELECT MAX(dp.xymd) AS dt
        FROM public.daily_prices dp
        WHERE dp.xymd <= p_as_of
    ),
    price_data AS (
        SELECT dp.ticker_id, dp.xymd, dp.clos,
               dp.clos - LAG(dp.clos) OVER (PARTITION BY dp.ticker_id ORDER BY dp.xymd) AS chg
        FROM public.daily_prices dp
        WHERE dp.xymd > (p_as_of - (p_window * 4))
          AND dp.xymd <= p_as_of
    ),
    gl AS (
        SELECT ticker_id, xymd, clos,
               GREATEST(chg, 0)  AS gain,
               GREATEST(-chg, 0) AS loss
        FROM price_data WHERE chg IS NOT NULL
    ),
    avg_gl AS (
        SELECT ticker_id, xymd, clos,
               AVG(gain) OVER (PARTITION BY ticker_id ORDER BY xymd
                               ROWS BETWEEN (p_window - 1) PRECEDING AND CURRENT ROW) AS avg_gain,
               AVG(loss) OVER (PARTITION BY ticker_id ORDER BY xymd
                               ROWS BETWEEN (p_window - 1) PRECEDING AND CURRENT ROW) AS avg_loss
        FROM gl
    ),
    rsi_all AS (
        SELECT ticker_id, xymd, clos,
               CASE WHEN avg_loss = 0 THEN 100.0
                    ELSE ROUND(100.0 - 100.0 / (1.0 + avg_gain / NULLIF(avg_loss, 0)), 4)
               END AS rsi_value
        FROM avg_gl
    )
    SELECT r.ticker_id, r.xymd, r.rsi_value, r.clos
    FROM rsi_all r
    JOIN public.tickers t ON t.ticker_id = r.ticker_id
    JOIN latest_date ld ON r.xymd = ld.dt
    WHERE t.lstg_yn = TRUE;
$$;

-- MACD: Moving Average Convergence Divergence
CREATE OR REPLACE FUNCTION public.calc_macd(
    p_fast   INTEGER DEFAULT 12,
    p_slow   INTEGER DEFAULT 26,
    p_signal INTEGER DEFAULT 9,
    p_as_of  DATE    DEFAULT CURRENT_DATE
)
RETURNS TABLE (ticker_id INTEGER, xymd DATE, macd_line NUMERIC,
               signal_line NUMERIC, histogram NUMERIC, clos NUMERIC)
LANGUAGE sql STABLE AS $$
    WITH latest_date AS (
        SELECT MAX(dp.xymd) AS dt FROM public.daily_prices dp WHERE dp.xymd <= p_as_of
    ),
    price_data AS (
        SELECT dp.ticker_id, dp.xymd, dp.clos
        FROM public.daily_prices dp
        WHERE dp.xymd > (p_as_of - (p_slow * 4))
          AND dp.xymd <= p_as_of
    ),
    ema_calc AS (
        SELECT ticker_id, xymd, clos,
               AVG(clos) OVER (PARTITION BY ticker_id ORDER BY xymd
                               ROWS BETWEEN (p_fast - 1) PRECEDING AND CURRENT ROW) AS ema_fast,
               AVG(clos) OVER (PARTITION BY ticker_id ORDER BY xymd
                               ROWS BETWEEN (p_slow - 1) PRECEDING AND CURRENT ROW) AS ema_slow
        FROM price_data
    ),
    macd_raw AS (
        SELECT ticker_id, xymd, clos,
               ema_fast - ema_slow AS macd_val
        FROM ema_calc
    ),
    with_signal AS (
        SELECT ticker_id, xymd, clos, macd_val,
               AVG(macd_val) OVER (PARTITION BY ticker_id ORDER BY xymd
                                   ROWS BETWEEN (p_signal - 1) PRECEDING AND CURRENT ROW) AS signal_val
        FROM macd_raw
    )
    SELECT m.ticker_id, m.xymd,
           ROUND(m.macd_val, 6)   AS macd_line,
           ROUND(m.signal_val, 6) AS signal_line,
           ROUND(m.macd_val - m.signal_val, 6) AS histogram,
           m.clos
    FROM with_signal m
    JOIN public.tickers t ON t.ticker_id = m.ticker_id
    JOIN latest_date ld ON m.xymd = ld.dt
    WHERE t.lstg_yn = TRUE;
$$;

-- Momentum: closing price change over N days
CREATE OR REPLACE FUNCTION public.calc_momentum(
    p_window INTEGER DEFAULT 20,
    p_as_of  DATE    DEFAULT CURRENT_DATE
)
RETURNS TABLE (ticker_id INTEGER, xymd DATE, momentum NUMERIC, clos NUMERIC)
LANGUAGE sql STABLE AS $$
    WITH latest_date AS (
        SELECT MAX(dp.xymd) AS dt FROM public.daily_prices dp WHERE dp.xymd <= p_as_of
    ),
    price_data AS (
        SELECT dp.ticker_id, dp.xymd, dp.clos,
               LAG(dp.clos, p_window) OVER (PARTITION BY dp.ticker_id ORDER BY dp.xymd) AS prev_clos
        FROM public.daily_prices dp
        WHERE dp.xymd > (p_as_of - (p_window * 2))
          AND dp.xymd <= p_as_of
    )
    SELECT pd.ticker_id, pd.xymd,
           ROUND((pd.clos - pd.prev_clos) / NULLIF(pd.prev_clos, 0) * 100, 4) AS momentum,
           pd.clos
    FROM price_data pd
    JOIN public.tickers t ON t.ticker_id = pd.ticker_id
    JOIN latest_date ld ON pd.xymd = ld.dt
    WHERE t.lstg_yn = TRUE AND pd.prev_clos IS NOT NULL;
$$;

-- Rolling Return: cumulative return over N days
CREATE OR REPLACE FUNCTION public.calc_rolling_return(
    p_window INTEGER DEFAULT 20,
    p_as_of  DATE    DEFAULT CURRENT_DATE
)
RETURNS TABLE (ticker_id INTEGER, xymd DATE, return_pct NUMERIC, clos NUMERIC)
LANGUAGE sql STABLE AS $$
    WITH latest_date AS (
        SELECT MAX(dp.xymd) AS dt FROM public.daily_prices dp WHERE dp.xymd <= p_as_of
    ),
    price_data AS (
        SELECT dp.ticker_id, dp.xymd, dp.clos,
               LAG(dp.clos, p_window) OVER (PARTITION BY dp.ticker_id ORDER BY dp.xymd) AS start_clos
        FROM public.daily_prices dp
        WHERE dp.xymd > (p_as_of - (p_window * 2))
          AND dp.xymd <= p_as_of
    )
    SELECT pd.ticker_id, pd.xymd,
           ROUND((pd.clos - pd.start_clos) / NULLIF(pd.start_clos, 0) * 100, 4) AS return_pct,
           pd.clos
    FROM price_data pd
    JOIN public.tickers t ON t.ticker_id = pd.ticker_id
    JOIN latest_date ld ON pd.xymd = ld.dt
    WHERE t.lstg_yn = TRUE AND pd.start_clos IS NOT NULL;
$$;

-- Volatility: rolling standard deviation of daily log returns
CREATE OR REPLACE FUNCTION public.calc_volatility(
    p_window INTEGER DEFAULT 20,
    p_as_of  DATE    DEFAULT CURRENT_DATE
)
RETURNS TABLE (ticker_id INTEGER, xymd DATE, volatility NUMERIC, clos NUMERIC)
LANGUAGE sql STABLE AS $$
    WITH latest_date AS (
        SELECT MAX(dp.xymd) AS dt FROM public.daily_prices dp WHERE dp.xymd <= p_as_of
    ),
    log_returns AS (
        SELECT dp.ticker_id, dp.xymd, dp.clos,
               LN(dp.clos / NULLIF(LAG(dp.clos) OVER (PARTITION BY dp.ticker_id ORDER BY dp.xymd), 0)) AS log_ret
        FROM public.daily_prices dp
        WHERE dp.xymd > (p_as_of - (p_window * 2))
          AND dp.xymd <= p_as_of
    ),
    rolling_vol AS (
        SELECT ticker_id, xymd, clos,
               STDDEV(log_ret) OVER (PARTITION BY ticker_id ORDER BY xymd
                                     ROWS BETWEEN (p_window - 1) PRECEDING AND CURRENT ROW) AS volatility
        FROM log_returns
        WHERE log_ret IS NOT NULL
    )
    SELECT rv.ticker_id, rv.xymd,
           ROUND(rv.volatility * SQRT(252) * 100, 4) AS volatility,
           rv.clos
    FROM rolling_vol rv
    JOIN public.tickers t ON t.ticker_id = rv.ticker_id
    JOIN latest_date ld ON rv.xymd = ld.dt
    WHERE t.lstg_yn = TRUE AND rv.volatility IS NOT NULL;
$$;

-- Price-Volume Divergence: price up but volume down (or vice versa)
CREATE OR REPLACE FUNCTION public.calc_pv_divergence(
    p_window INTEGER DEFAULT 20,
    p_as_of  DATE    DEFAULT CURRENT_DATE
)
RETURNS TABLE (ticker_id INTEGER, xymd DATE,
               price_trend NUMERIC, vol_trend NUMERIC,
               divergence BOOLEAN, clos NUMERIC)
LANGUAGE sql STABLE AS $$
    WITH latest_date AS (
        SELECT MAX(dp.xymd) AS dt FROM public.daily_prices dp WHERE dp.xymd <= p_as_of
    ),
    trends AS (
        SELECT dp.ticker_id, dp.xymd, dp.clos,
               REGR_SLOPE(dp.clos, EXTRACT(EPOCH FROM dp.xymd))
                   OVER (PARTITION BY dp.ticker_id ORDER BY dp.xymd
                         ROWS BETWEEN (p_window - 1) PRECEDING AND CURRENT ROW) AS price_slope,
               REGR_SLOPE(dp.tvol::NUMERIC, EXTRACT(EPOCH FROM dp.xymd))
                   OVER (PARTITION BY dp.ticker_id ORDER BY dp.xymd
                         ROWS BETWEEN (p_window - 1) PRECEDING AND CURRENT ROW) AS vol_slope
        FROM public.daily_prices dp
        WHERE dp.xymd > (p_as_of - (p_window * 2))
          AND dp.xymd <= p_as_of
    )
    SELECT t2.ticker_id, t2.xymd,
           ROUND(t2.price_slope::NUMERIC, 8) AS price_trend,
           ROUND(t2.vol_slope::NUMERIC, 8)   AS vol_trend,
           (SIGN(t2.price_slope) != SIGN(t2.vol_slope)) AS divergence,
           t2.clos
    FROM trends t2
    JOIN public.tickers t ON t.ticker_id = t2.ticker_id
    JOIN latest_date ld ON t2.xymd = ld.dt
    WHERE t.lstg_yn = TRUE
      AND t2.price_slope IS NOT NULL
      AND t2.vol_slope IS NOT NULL;
$$;
```

- [ ] **Step 2: Apply the migration**

```bash
psql -U jhg_user -d $DB_NAME -f metric_studio/db/migrations/005_create_pg_functions.sql
```

Expected: 6× `CREATE FUNCTION`, no errors.

- [ ] **Step 3: Smoke-test one function**

```bash
psql -U jhg_user -d $DB_NAME -c "SELECT ticker_id, xymd, rsi_value FROM public.calc_rsi(14, CURRENT_DATE) LIMIT 5;"
```

Expected: 5 rows with numeric rsi_value values.

- [ ] **Step 4: Commit**

```bash
git add metric_studio/db/migrations/005_create_pg_functions.sql
git commit -m "feat: add PostgreSQL metric functions (RSI, MACD, momentum, return, volatility, PV divergence)"
```

---

## Task 5: Batch Update Job

**Files:**
- Create: `metric_studio/db/batch/update_precomputed.py`

- [ ] **Step 1: Write update_precomputed.py**

```python
# metric_studio/db/batch/update_precomputed.py
"""
Nightly batch: populate/refresh the four precomputed metric tables from daily_prices.
Run with: python -m db.batch.update_precomputed [--full]
--full: recompute all history (slow, first-time use)
default: recompute only the most recent date in daily_prices
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import create_engine, text
from config import settings

MA_SQL = """
INSERT INTO public.daily_ma (ticker_id, xymd, ma_5, ma_10, ma_20, ma_50, ma_200)
SELECT
    ticker_id, xymd,
    AVG(clos) OVER (PARTITION BY ticker_id ORDER BY xymd ROWS BETWEEN 4   PRECEDING AND CURRENT ROW),
    AVG(clos) OVER (PARTITION BY ticker_id ORDER BY xymd ROWS BETWEEN 9   PRECEDING AND CURRENT ROW),
    AVG(clos) OVER (PARTITION BY ticker_id ORDER BY xymd ROWS BETWEEN 19  PRECEDING AND CURRENT ROW),
    AVG(clos) OVER (PARTITION BY ticker_id ORDER BY xymd ROWS BETWEEN 49  PRECEDING AND CURRENT ROW),
    AVG(clos) OVER (PARTITION BY ticker_id ORDER BY xymd ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
FROM public.daily_prices
{where_clause}
ON CONFLICT (ticker_id, xymd) DO UPDATE SET
    ma_5   = EXCLUDED.ma_5,   ma_10 = EXCLUDED.ma_10, ma_20 = EXCLUDED.ma_20,
    ma_50  = EXCLUDED.ma_50,  ma_200 = EXCLUDED.ma_200, created_at = NOW();
"""

BB_SQL = """
INSERT INTO public.daily_bb (ticker_id, xymd, ma_20, upper_band, lower_band, bandwidth, pct_b)
WITH base AS (
    SELECT ticker_id, xymd, clos,
        AVG(clos)    OVER w AS ma_20,
        STDDEV(clos) OVER w AS sd
    FROM public.daily_prices
    {where_clause}
    WINDOW w AS (PARTITION BY ticker_id ORDER BY xymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
)
SELECT ticker_id, xymd, ma_20,
    ma_20 + 2 * sd AS upper_band,
    ma_20 - 2 * sd AS lower_band,
    CASE WHEN ma_20 = 0 THEN NULL ELSE 4 * sd / ma_20 END AS bandwidth,
    CASE WHEN (4 * sd) = 0 THEN NULL ELSE (clos - (ma_20 - 2 * sd)) / (4 * sd) END AS pct_b
FROM base
ON CONFLICT (ticker_id, xymd) DO UPDATE SET
    ma_20 = EXCLUDED.ma_20, upper_band = EXCLUDED.upper_band,
    lower_band = EXCLUDED.lower_band, bandwidth = EXCLUDED.bandwidth,
    pct_b = EXCLUDED.pct_b, created_at = NOW();
"""

ATR_SQL = """
INSERT INTO public.daily_atr (ticker_id, xymd, true_range, atr_14)
WITH tr AS (
    SELECT ticker_id, xymd,
        GREATEST(
            high - low,
            ABS(high - LAG(clos) OVER (PARTITION BY ticker_id ORDER BY xymd)),
            ABS(low  - LAG(clos) OVER (PARTITION BY ticker_id ORDER BY xymd))
        ) AS true_range
    FROM public.daily_prices
    {where_clause}
)
SELECT ticker_id, xymd, true_range,
    AVG(true_range) OVER (PARTITION BY ticker_id ORDER BY xymd ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
FROM tr
ON CONFLICT (ticker_id, xymd) DO UPDATE SET
    true_range = EXCLUDED.true_range, atr_14 = EXCLUDED.atr_14, created_at = NOW();
"""

OBV_SQL = """
INSERT INTO public.daily_obv (ticker_id, xymd, obv)
WITH daily_direction AS (
    SELECT ticker_id, xymd,
        CASE
            WHEN clos > LAG(clos) OVER (PARTITION BY ticker_id ORDER BY xymd) THEN tvol
            WHEN clos < LAG(clos) OVER (PARTITION BY ticker_id ORDER BY xymd) THEN -tvol
            ELSE 0
        END AS signed_vol
    FROM public.daily_prices
),
cumulative AS (
    SELECT ticker_id, xymd,
        SUM(signed_vol) OVER (PARTITION BY ticker_id ORDER BY xymd
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS obv
    FROM daily_direction
)
SELECT ticker_id, xymd, obv FROM cumulative
{where_clause_plain}
ON CONFLICT (ticker_id, xymd) DO UPDATE SET obv = EXCLUDED.obv, created_at = NOW();
"""


def run(full: bool = False) -> None:
    engine = create_engine(settings.db_url)
    if full:
        where = ""
        where_plain = ""
        print("Full recompute — this may take several minutes...")
    else:
        where = "WHERE xymd = (SELECT MAX(xymd) FROM public.daily_prices)"
        where_plain = "WHERE xymd = (SELECT MAX(xymd) FROM cumulative)"
        print("Incremental update for latest date...")

    with engine.connect() as conn:
        for label, sql in [
            ("daily_ma",  MA_SQL.format(where_clause=where)),
            ("daily_bb",  BB_SQL.format(where_clause=where)),
            ("daily_atr", ATR_SQL.format(where_clause=where)),
            ("daily_obv", OBV_SQL.format(where_clause=where, where_clause_plain=where_plain)),
        ]:
            print(f"  Updating {label}...")
            conn.execute(text(sql))
        conn.commit()
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()
    run(full=args.full)
```

- [ ] **Step 2: Run full initial population**

```bash
cd metric_studio && python -m db.batch.update_precomputed --full
```

Expected: prints `Updating daily_ma... Updating daily_bb... Updating daily_atr... Updating daily_obv... Done.`

- [ ] **Step 3: Verify row counts**

```bash
psql -U jhg_user -d $DB_NAME -c "
SELECT 'daily_ma' AS t, COUNT(*) FROM public.daily_ma
UNION ALL SELECT 'daily_bb', COUNT(*) FROM public.daily_bb
UNION ALL SELECT 'daily_atr', COUNT(*) FROM public.daily_atr
UNION ALL SELECT 'daily_obv', COUNT(*) FROM public.daily_obv;"
```

Expected: all four tables have > 0 rows.

- [ ] **Step 4: Commit**

```bash
git add metric_studio/db/batch/update_precomputed.py
git commit -m "feat: add nightly batch job to refresh precomputed metric tables"
```

---

## Task 6: AgentState + Metric Catalog

**Files:**
- Create: `metric_studio/pipeline/state.py`
- Create: `metric_studio/catalog/metrics.yaml`

- [ ] **Step 1: Write pipeline/state.py**

```python
# metric_studio/pipeline/state.py
from typing import Any, TypedDict


class AgentState(TypedDict, total=False):
    raw_query: str
    intent: str
    metric_id: str | None
    resolved_params: dict[str, Any]
    unresolved_params: list[str]
    metric_spec: dict | None
    sql: str | None
    result: Any          # pandas.DataFrame or None
    execution_error: str | None
    conversation: list[dict]
```

- [ ] **Step 2: Write catalog/metrics.yaml**

```yaml
# metric_studio/catalog/metrics.yaml

- id: rsi_overbought
  display_name: "RSI Overbought"
  aliases: ["overbought", "RSI high", "relative strength overbought", "RSI above"]
  description: "Stocks where RSI exceeds a threshold, signaling overbought condition"
  strategy: pg_function
  function: calc_rsi
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
      question: "What RSI value defines overbought? (common: 70)"
    direction:
      type: enum
      values: [above, below]
      default: above
      clarify_if_missing: false

- id: rsi_oversold
  display_name: "RSI Oversold"
  aliases: ["oversold", "RSI low", "relative strength oversold", "RSI below"]
  description: "Stocks where RSI falls below a threshold, signaling oversold condition"
  strategy: pg_function
  function: calc_rsi
  params:
    window:
      type: integer
      default: 14
      clarify_if_missing: true
      question: "How many trading days for the RSI window? (common: 14)"
    threshold:
      type: numeric
      default: 30
      clarify_if_missing: true
      question: "What RSI value defines oversold? (common: 30)"
    direction:
      type: enum
      values: [above, below]
      default: below
      clarify_if_missing: false

- id: macd_crossover
  display_name: "MACD Crossover"
  aliases: ["MACD", "MACD signal", "MACD cross", "moving average convergence"]
  description: "Stocks where the MACD line crosses above or below the signal line"
  strategy: pg_function
  function: calc_macd
  params:
    fast:
      type: integer
      default: 12
      clarify_if_missing: true
      question: "Fast EMA period for MACD? (common: 12)"
    slow:
      type: integer
      default: 26
      clarify_if_missing: true
      question: "Slow EMA period for MACD? (common: 26)"
    signal:
      type: integer
      default: 9
      clarify_if_missing: true
      question: "Signal line period for MACD? (common: 9)"
    direction:
      type: enum
      values: [above, below]
      default: above
      clarify_if_missing: true
      question: "Should MACD line be above (bullish) or below (bearish) the signal line?"

- id: momentum_screen
  display_name: "Momentum Screen"
  aliases: ["momentum", "strong momentum", "price momentum", "trending stocks"]
  description: "Stocks with strong price momentum over N trading days"
  strategy: pg_function
  function: calc_momentum
  params:
    window:
      type: integer
      default: 20
      clarify_if_missing: true
      question: "How many trading days to measure momentum? (common: 20)"
    threshold:
      type: numeric
      default: 5.0
      clarify_if_missing: true
      question: "Minimum momentum percentage to qualify? (e.g., 5 for 5%)"

- id: rolling_return_rank
  display_name: "Rolling Return Rank"
  aliases: ["top performers", "best returns", "rolling return", "return over period"]
  description: "Stocks ranked by rolling return over N trading days"
  strategy: pg_function
  function: calc_rolling_return
  params:
    window:
      type: integer
      default: 20
      clarify_if_missing: true
      question: "Over how many trading days to measure return? (common: 5, 10, 20, 60)"

- id: volatility_filter
  display_name: "Volatility Filter"
  aliases: ["low volatility", "high volatility", "volatility screen", "stable stocks", "volatile stocks"]
  description: "Stocks filtered by annualized rolling volatility"
  strategy: pg_function
  function: calc_volatility
  params:
    window:
      type: integer
      default: 20
      clarify_if_missing: true
      question: "Rolling window in trading days for volatility? (common: 20)"
    threshold:
      type: numeric
      default: 20.0
      clarify_if_missing: true
      question: "Volatility threshold (annualized %)? (e.g., 20 for 20%)"
    direction:
      type: enum
      values: [above, below]
      clarify_if_missing: true
      question: "Filter for stocks ABOVE (high volatility) or BELOW (low volatility) the threshold?"

- id: pv_divergence
  display_name: "Price-Volume Divergence"
  aliases: ["divergence", "price volume divergence", "PV divergence", "volume divergence"]
  description: "Stocks where price trend and volume trend move in opposite directions"
  strategy: pg_function
  function: calc_pv_divergence
  params:
    window:
      type: integer
      default: 20
      clarify_if_missing: true
      question: "Lookback window in trading days for divergence detection? (common: 20)"

- id: moving_average_cross
  display_name: "Moving Average Crossover"
  aliases: ["MA cross", "golden cross", "death cross", "moving average signal", "MA crossover"]
  description: "Stocks where a short-term MA crosses above or below a long-term MA"
  strategy: precomputed_table
  table: daily_ma
  params:
    short_window:
      type: enum
      values: ["5", "10", "20", "50"]
      clarify_if_missing: true
      question: "Short-term MA window? Choose from: 5, 10, 20, 50"
    long_window:
      type: enum
      values: ["20", "50", "100", "200"]
      clarify_if_missing: true
      question: "Long-term MA window? Choose from: 20, 50, 100, 200"
    direction:
      type: enum
      values: [above, below]
      clarify_if_missing: true
      question: "Short MA crossing ABOVE (bullish/golden cross) or BELOW (bearish/death cross) the long MA?"

- id: bollinger_breakout
  display_name: "Bollinger Band Breakout"
  aliases: ["Bollinger", "Bollinger band", "BB breakout", "band breakout", "price breakout"]
  description: "Stocks where price breaks above the upper or below the lower Bollinger Band"
  strategy: precomputed_table
  table: daily_bb
  params:
    direction:
      type: enum
      values: [above, below]
      clarify_if_missing: true
      question: "Breakout above upper band (bullish) or below lower band (bearish)?"

- id: atr_filter
  display_name: "ATR Volatility Filter"
  aliases: ["ATR", "average true range", "ATR filter", "range filter"]
  description: "Stocks filtered by their 14-day Average True Range relative to price"
  strategy: precomputed_table
  table: daily_atr
  params:
    threshold:
      type: numeric
      default: 2.0
      clarify_if_missing: true
      question: "ATR threshold as percentage of price? (e.g., 2 for 2%)"
    direction:
      type: enum
      values: [above, below]
      clarify_if_missing: true
      question: "Filter for high ATR (above threshold) or low ATR (below threshold)?"
```

- [ ] **Step 3: Commit**

```bash
git add metric_studio/pipeline/state.py metric_studio/catalog/metrics.yaml
git commit -m "feat: add AgentState and 10-pattern metric catalog"
```

---

## Task 7: Catalog Loader + SPECIFY Stage

**Files:**
- Create: `metric_studio/pipeline/specify.py`
- Create: `metric_studio/tests/test_specify.py`

- [ ] **Step 1: Write the failing tests**

```python
# metric_studio/tests/test_specify.py
import pytest
from pipeline.specify import load_catalog, find_metric, get_unresolved_params, specify


def test_catalog_loads_ten_patterns():
    catalog = load_catalog()
    assert len(catalog) == 10


def test_find_metric_by_id():
    catalog = load_catalog()
    metric = find_metric(catalog, "rsi_overbought")
    assert metric is not None
    assert metric["id"] == "rsi_overbought"
    assert metric["strategy"] == "pg_function"


def test_find_metric_returns_none_for_unknown():
    catalog = load_catalog()
    assert find_metric(catalog, "does_not_exist") is None


def test_get_unresolved_params_all_missing():
    catalog = load_catalog()
    metric = find_metric(catalog, "rsi_overbought")
    unresolved = get_unresolved_params(metric, {})
    assert "window" in unresolved
    assert "threshold" in unresolved


def test_get_unresolved_params_partially_resolved():
    catalog = load_catalog()
    metric = find_metric(catalog, "rsi_overbought")
    unresolved = get_unresolved_params(metric, {"window": 14})
    assert "window" not in unresolved
    assert "threshold" in unresolved


def test_get_unresolved_params_all_resolved():
    catalog = load_catalog()
    metric = find_metric(catalog, "rsi_overbought")
    unresolved = get_unresolved_params(metric, {"window": 14, "threshold": 70})
    assert unresolved == []


def test_specify_builds_pg_function_spec():
    state = {
        "metric_id": "rsi_overbought",
        "resolved_params": {"window": 14, "threshold": 70},
        "unresolved_params": [],
    }
    result = specify(state)
    spec = result["metric_spec"]
    assert spec["strategy"] == "pg_function"
    assert spec["function"] == "calc_rsi"
    assert spec["resolved_params"]["window"] == 14
    assert result["unresolved_params"] == []


def test_specify_builds_precomputed_table_spec():
    state = {
        "metric_id": "moving_average_cross",
        "resolved_params": {"short_window": "50", "long_window": "200", "direction": "above"},
        "unresolved_params": [],
    }
    result = specify(state)
    spec = result["metric_spec"]
    assert spec["strategy"] == "precomputed_table"
    assert spec["table"] == "daily_ma"


def test_specify_raises_on_unresolved():
    state = {
        "metric_id": "rsi_overbought",
        "resolved_params": {},
        "unresolved_params": ["window", "threshold"],
    }
    with pytest.raises(ValueError, match="Unresolved params"):
        specify(state)


def test_specify_raises_on_unknown_metric():
    state = {
        "metric_id": "nonexistent",
        "resolved_params": {},
        "unresolved_params": [],
    }
    with pytest.raises(ValueError, match="Unknown metric_id"):
        specify(state)
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd metric_studio && pytest tests/test_specify.py -v
```

Expected: `ImportError` or `ModuleNotFoundError` — `pipeline.specify` does not exist yet.

- [ ] **Step 3: Write pipeline/specify.py**

```python
# metric_studio/pipeline/specify.py
from pathlib import Path
import yaml
from pipeline.state import AgentState

CATALOG_PATH = Path(__file__).parent.parent / "catalog" / "metrics.yaml"


def load_catalog() -> list[dict]:
    with open(CATALOG_PATH) as f:
        return yaml.safe_load(f)


def find_metric(catalog: list[dict], metric_id: str) -> dict | None:
    return next((m for m in catalog if m["id"] == metric_id), None)


def get_unresolved_params(metric: dict, resolved: dict) -> list[str]:
    return [
        name
        for name, defn in metric["params"].items()
        if defn.get("clarify_if_missing") and name not in resolved
    ]


def specify(state: AgentState) -> AgentState:
    catalog = load_catalog()
    metric = find_metric(catalog, state["metric_id"])
    if metric is None:
        raise ValueError(f"Unknown metric_id: {state['metric_id']}")

    unresolved = get_unresolved_params(metric, state.get("resolved_params", {}))
    if unresolved:
        raise ValueError(f"Unresolved params: {unresolved}")

    spec: dict = {
        "id": metric["id"],
        "display_name": metric["display_name"],
        "strategy": metric["strategy"],
        "resolved_params": state.get("resolved_params", {}),
    }
    if metric["strategy"] == "pg_function":
        spec["function"] = metric["function"]
    else:
        spec["table"] = metric["table"]

    return {**state, "metric_spec": spec, "unresolved_params": []}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
cd metric_studio && pytest tests/test_specify.py -v
```

Expected: all 10 tests pass.

- [ ] **Step 5: Commit**

```bash
git add metric_studio/pipeline/specify.py metric_studio/tests/test_specify.py
git commit -m "feat: add catalog loader and SPECIFY stage with tests"
```

---

## Task 8: UNDERSTAND Stage

**Files:**
- Create: `metric_studio/pipeline/understand.py`
- Create: `metric_studio/prompts/understand.jinja2`

- [ ] **Step 1: Write prompts/understand.jinja2**

```jinja2
{# metric_studio/prompts/understand.jinja2 #}
You are a financial data assistant that maps securities screening queries to known metric patterns.

Available metric patterns:
{% for item in catalog %}
- id: "{{ item.id }}" | {{ item.display_name }} | aliases: {{ item.aliases | join(', ') }}
{% endfor %}

User query: "{{ query }}"

{% if conversation %}
Conversation history (for context):
{% for msg in conversation %}
{{ msg.role }}: {{ msg.content }}
{% endfor %}
{% endif %}

Instructions:
1. Choose the best-matching metric id from the list above, or null if none fit.
2. Extract any parameter values already stated in the query (e.g. "14-day RSI" → window=14, "RSI > 70" → threshold=70, "golden cross" → direction="above").
3. Write a one-sentence description of what the user wants to find.
```

- [ ] **Step 2: Write pipeline/understand.py**

```python
# metric_studio/pipeline/understand.py
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from langchain_anthropic import ChatAnthropic
from pydantic import BaseModel, Field
from pipeline.state import AgentState
from pipeline.specify import load_catalog, find_metric, get_unresolved_params
from config import settings

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


class UnderstandOutput(BaseModel):
    intent: str = Field(description="One sentence: what the user wants to find")
    metric_id: str | None = Field(description="Matched metric id, or null")
    resolved_params: dict = Field(
        default_factory=dict,
        description="Parameter values already present in the query",
    )


def understand(state: AgentState, catalog: list[dict] | None = None) -> AgentState:
    if catalog is None:
        catalog = load_catalog()

    env = Environment(loader=FileSystemLoader(str(PROMPTS_DIR)))
    template = env.get_template("understand.jinja2")

    catalog_summary = [
        {"id": m["id"], "display_name": m["display_name"], "aliases": m["aliases"]}
        for m in catalog
    ]
    prompt_text = template.render(
        query=state["raw_query"],
        catalog=catalog_summary,
        conversation=state.get("conversation", []),
    )

    llm = ChatAnthropic(
        model=settings.llm_model,
        api_key=settings.anthropic_api_key,
    )
    output: UnderstandOutput = llm.with_structured_output(UnderstandOutput).invoke(prompt_text)

    unresolved: list[str] = []
    if output.metric_id:
        metric = find_metric(catalog, output.metric_id)
        if metric:
            unresolved = get_unresolved_params(metric, output.resolved_params)

    return {
        **state,
        "intent": output.intent,
        "metric_id": output.metric_id,
        "resolved_params": output.resolved_params,
        "unresolved_params": unresolved,
    }
```

- [ ] **Step 3: Write a smoke test with a mocked LLM**

```python
# metric_studio/tests/test_understand.py
from unittest.mock import MagicMock, patch
from pipeline.understand import understand, UnderstandOutput

MOCK_CATALOG = [
    {
        "id": "rsi_overbought",
        "display_name": "RSI Overbought",
        "aliases": ["overbought", "RSI high"],
        "params": {
            "window": {"type": "integer", "clarify_if_missing": True, "question": "Window?"},
            "threshold": {"type": "numeric", "clarify_if_missing": True, "question": "Threshold?"},
        },
    }
]


def _mock_llm(metric_id, resolved_params):
    mock_output = UnderstandOutput(
        intent="Find overbought stocks using RSI",
        metric_id=metric_id,
        resolved_params=resolved_params,
    )
    mock_structured = MagicMock()
    mock_structured.invoke.return_value = mock_output
    mock_llm_instance = MagicMock()
    mock_llm_instance.with_structured_output.return_value = mock_structured
    return mock_llm_instance


def test_understand_matches_metric_and_extracts_params():
    with patch("pipeline.understand.ChatAnthropic", return_value=_mock_llm("rsi_overbought", {"window": 14})):
        state = {"raw_query": "Find overbought stocks RSI 14-day", "conversation": []}
        result = understand(state, catalog=MOCK_CATALOG)

    assert result["metric_id"] == "rsi_overbought"
    assert result["resolved_params"]["window"] == 14
    assert "threshold" in result["unresolved_params"]
    assert "window" not in result["unresolved_params"]


def test_understand_returns_none_metric_id_when_no_match():
    with patch("pipeline.understand.ChatAnthropic", return_value=_mock_llm(None, {})):
        state = {"raw_query": "Something completely unrelated", "conversation": []}
        result = understand(state, catalog=MOCK_CATALOG)

    assert result["metric_id"] is None
    assert result["unresolved_params"] == []
```

- [ ] **Step 4: Run tests**

```bash
cd metric_studio && pytest tests/test_understand.py -v
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add metric_studio/pipeline/understand.py metric_studio/prompts/understand.jinja2 metric_studio/tests/test_understand.py
git commit -m "feat: add UNDERSTAND stage with LLM metric matching"
```

---

## Task 9: CLARIFY Stage

**Files:**
- Create: `metric_studio/pipeline/clarify.py`
- Create: `metric_studio/tests/test_clarify.py`

- [ ] **Step 1: Write failing tests**

```python
# metric_studio/tests/test_clarify.py
from unittest.mock import patch
from pipeline.clarify import clarify

MOCK_CATALOG = [
    {
        "id": "rsi_overbought",
        "params": {
            "window":    {"type": "integer", "clarify_if_missing": True, "question": "RSI window?"},
            "threshold": {"type": "numeric", "clarify_if_missing": True, "question": "Threshold?"},
            "direction": {"type": "enum",    "clarify_if_missing": False, "values": ["above", "below"]},
        },
    }
]


def test_clarify_resolves_integer_and_numeric():
    state = {
        "metric_id": "rsi_overbought",
        "resolved_params": {},
        "unresolved_params": ["window", "threshold"],
    }
    with patch("builtins.input", side_effect=["14", "70"]):
        result = clarify(state, MOCK_CATALOG)
    assert result["resolved_params"]["window"] == 14
    assert result["resolved_params"]["threshold"] == 70.0
    assert result["unresolved_params"] == []


def test_clarify_no_op_when_nothing_unresolved():
    state = {
        "metric_id": "rsi_overbought",
        "resolved_params": {"window": 14, "threshold": 70.0},
        "unresolved_params": [],
    }
    result = clarify(state, MOCK_CATALOG)
    assert result["resolved_params"] == {"window": 14, "threshold": 70.0}


def test_clarify_retries_invalid_enum():
    mock_catalog = [
        {
            "id": "moving_average_cross",
            "params": {
                "direction": {
                    "type": "enum",
                    "values": ["above", "below"],
                    "clarify_if_missing": True,
                    "question": "Direction?",
                }
            },
        }
    ]
    state = {
        "metric_id": "moving_average_cross",
        "resolved_params": {},
        "unresolved_params": ["direction"],
    }
    with patch("builtins.input", side_effect=["sideways", "above"]):
        result = clarify(state, mock_catalog)
    assert result["resolved_params"]["direction"] == "above"
```

- [ ] **Step 2: Run to confirm failure**

```bash
cd metric_studio && pytest tests/test_clarify.py -v
```

Expected: `ImportError` — `pipeline.clarify` does not exist yet.

- [ ] **Step 3: Write pipeline/clarify.py**

```python
# metric_studio/pipeline/clarify.py
from pipeline.state import AgentState


def clarify(state: AgentState, catalog: list[dict]) -> AgentState:
    if not state.get("unresolved_params"):
        return state

    metric = next((m for m in catalog if m["id"] == state["metric_id"]), None)
    if metric is None:
        return state

    resolved = dict(state.get("resolved_params", {}))
    remaining = list(state.get("unresolved_params", []))

    while remaining:
        param_name = remaining[0]
        param_def = metric["params"][param_name]
        print(f"[CLARIFY] {param_def['question']}")
        user_input = input("> ").strip()

        param_type = param_def.get("type", "string")
        try:
            if param_type == "integer":
                resolved[param_name] = int(user_input)
            elif param_type == "numeric":
                resolved[param_name] = float(user_input)
            elif param_type == "enum":
                valid = param_def.get("values", [])
                if user_input not in valid:
                    print(f"Please choose from: {', '.join(valid)}")
                    continue
                resolved[param_name] = user_input
            else:
                resolved[param_name] = user_input
        except (ValueError, TypeError):
            print(f"Invalid input. Expected {param_type}.")
            continue

        remaining.pop(0)

    return {**state, "resolved_params": resolved, "unresolved_params": []}
```

- [ ] **Step 4: Run tests**

```bash
cd metric_studio && pytest tests/test_clarify.py -v
```

Expected: all 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add metric_studio/pipeline/clarify.py metric_studio/tests/test_clarify.py
git commit -m "feat: add CLARIFY stage with terminal I/O param loop"
```

---

## Task 10: GENERATE Stage

**Files:**
- Create: `metric_studio/pipeline/generate.py`
- Create: `metric_studio/prompts/generate.jinja2`
- Create: `metric_studio/prompts/few_shots/examples.yaml`

- [ ] **Step 1: Write prompts/few_shots/examples.yaml**

```yaml
# metric_studio/prompts/few_shots/examples.yaml

- metric_id: rsi_overbought
  description: "RSI > 70 over 14-day window on most recent trading day"
  sql: |
    SELECT t.ticker, r.xymd, ROUND(r.rsi_value, 2) AS rsi_value, r.clos
    FROM public.calc_rsi(14, CURRENT_DATE) r
    JOIN public.tickers t ON t.ticker_id = r.ticker_id
    WHERE r.rsi_value > 70
    ORDER BY r.rsi_value DESC
    LIMIT 100;

- metric_id: rsi_oversold
  description: "RSI < 30 over 14-day window on most recent trading day"
  sql: |
    SELECT t.ticker, r.xymd, ROUND(r.rsi_value, 2) AS rsi_value, r.clos
    FROM public.calc_rsi(14, CURRENT_DATE) r
    JOIN public.tickers t ON t.ticker_id = r.ticker_id
    WHERE r.rsi_value < 30
    ORDER BY r.rsi_value ASC
    LIMIT 100;

- metric_id: moving_average_cross
  description: "50-day MA crosses above 200-day MA (golden cross) on most recent date"
  sql: |
    SELECT t.ticker, m.xymd, m.ma_50, m.ma_200,
           ROUND(m.ma_50 - m.ma_200, 4) AS spread
    FROM public.daily_ma m
    JOIN public.tickers t ON t.ticker_id = m.ticker_id
    WHERE m.xymd = (SELECT MAX(xymd) FROM public.daily_ma)
      AND m.ma_50 > m.ma_200
    ORDER BY spread DESC
    LIMIT 100;

- metric_id: bollinger_breakout
  description: "Price breaks above upper Bollinger Band on most recent date"
  sql: |
    SELECT t.ticker, b.xymd, b.upper_band, b.pct_b,
           dp.clos AS close_price
    FROM public.daily_bb b
    JOIN public.tickers t ON t.ticker_id = b.ticker_id
    JOIN public.daily_prices dp ON dp.ticker_id = b.ticker_id AND dp.xymd = b.xymd
    WHERE b.xymd = (SELECT MAX(xymd) FROM public.daily_bb)
      AND dp.clos > b.upper_band
    ORDER BY b.pct_b DESC
    LIMIT 100;

- metric_id: volatility_filter
  description: "Stocks with annualized volatility below 20% over 20-day window"
  sql: |
    SELECT t.ticker, v.xymd, ROUND(v.volatility, 2) AS volatility_pct, v.clos
    FROM public.calc_volatility(20, CURRENT_DATE) v
    JOIN public.tickers t ON t.ticker_id = v.ticker_id
    WHERE v.volatility < 20
    ORDER BY v.volatility ASC
    LIMIT 100;
```

- [ ] **Step 2: Write prompts/generate.jinja2**

```jinja2
{# metric_studio/prompts/generate.jinja2 #}
You are a PostgreSQL expert. Generate a single SELECT query for financial time-series screening.

Database schema:
{{ schema }}

Metric specification:
- Pattern: {{ metric_spec.display_name }}
- Strategy: {{ metric_spec.strategy }}
{% if metric_spec.strategy == "pg_function" %}
- PostgreSQL function: public.{{ metric_spec.function }}(...)
{% else %}
- Precomputed table: public.{{ metric_spec.table }}
{% endif %}
- Resolved parameters: {{ metric_spec.resolved_params }}

{% if few_shots %}
Reference examples for this pattern:
{% for ex in few_shots %}
-- {{ ex.description }}
{{ ex.sql }}
{% endfor %}
{% endif %}

{% if error %}
The previous SQL failed with:
  {{ error }}
Correct the SQL to fix this error.
{% endif %}

Requirements for the generated SQL:
1. Use the function or table above with the resolved parameters as literal values.
2. Always join public.tickers to include the ticker column.
3. Apply threshold/direction filters using WHERE clauses (not inside function calls).
4. Return columns: ticker, xymd, at least one metric column, clos.
5. LIMIT 100 rows.
6. Output ONLY the SQL — no explanation, no markdown fences.
```

- [ ] **Step 3: Write pipeline/generate.py**

```python
# metric_studio/pipeline/generate.py
from pathlib import Path
import yaml
from jinja2 import Environment, FileSystemLoader
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage
from pipeline.state import AgentState
from config import settings

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

SCHEMA = {
    "base": (
        "public.tickers(ticker_id INT PK, ticker VARCHAR, exchange_code VARCHAR, "
        "is_etf BOOL, lstg_yn BOOL)\n"
        "public.daily_prices(ticker_id INT FK, xymd DATE, clos NUMERIC, open NUMERIC, "
        "high NUMERIC, low NUMERIC, tvol BIGINT)"
    ),
    "daily_ma":  "public.daily_ma(ticker_id INT, xymd DATE, ma_5, ma_10, ma_20, ma_50, ma_200 NUMERIC)",
    "daily_bb":  "public.daily_bb(ticker_id INT, xymd DATE, ma_20, upper_band, lower_band, bandwidth, pct_b NUMERIC)",
    "daily_atr": "public.daily_atr(ticker_id INT, xymd DATE, true_range, atr_14 NUMERIC)",
    "daily_obv": "public.daily_obv(ticker_id INT, xymd DATE, obv BIGINT)",
    "calc_rsi":          "public.calc_rsi(p_window INT, p_as_of DATE) → (ticker_id, xymd, rsi_value, clos)",
    "calc_macd":         "public.calc_macd(p_fast INT, p_slow INT, p_signal INT, p_as_of DATE) → (ticker_id, xymd, macd_line, signal_line, histogram, clos)",
    "calc_momentum":     "public.calc_momentum(p_window INT, p_as_of DATE) → (ticker_id, xymd, momentum, clos)",
    "calc_rolling_return":"public.calc_rolling_return(p_window INT, p_as_of DATE) → (ticker_id, xymd, return_pct, clos)",
    "calc_volatility":   "public.calc_volatility(p_window INT, p_as_of DATE) → (ticker_id, xymd, volatility, clos)",
    "calc_pv_divergence":"public.calc_pv_divergence(p_window INT, p_as_of DATE) → (ticker_id, xymd, price_trend, vol_trend, divergence BOOL, clos)",
}


def _schema_context(metric_spec: dict) -> str:
    lines = [SCHEMA["base"]]
    key = metric_spec.get("function") or metric_spec.get("table", "")
    if key in SCHEMA:
        lines.append(SCHEMA[key])
    return "\n".join(lines)


def _load_few_shots(metric_id: str) -> list[dict]:
    path = PROMPTS_DIR / "few_shots" / "examples.yaml"
    all_examples = yaml.safe_load(path.read_text())
    return [ex for ex in all_examples if ex["metric_id"] == metric_id]


def generate(state: AgentState) -> AgentState:
    env = Environment(loader=FileSystemLoader(str(PROMPTS_DIR)))
    template = env.get_template("generate.jinja2")

    metric_spec = state["metric_spec"]
    prompt_text = template.render(
        metric_spec=metric_spec,
        schema=_schema_context(metric_spec),
        few_shots=_load_few_shots(metric_spec["id"]),
        error=state.get("execution_error", ""),
    )

    llm = ChatAnthropic(model=settings.llm_model, api_key=settings.anthropic_api_key)
    response = llm.invoke([HumanMessage(content=prompt_text)])
    sql = response.content.strip()

    if sql.startswith("```"):
        sql = "\n".join(sql.split("\n")[1:])
        sql = sql.rstrip("`").strip()

    return {**state, "sql": sql, "execution_error": None}
```

- [ ] **Step 4: Write a smoke test with mocked LLM**

```python
# metric_studio/tests/test_generate.py
from unittest.mock import MagicMock, patch
from pipeline.generate import generate

RSI_SPEC = {
    "id": "rsi_overbought",
    "display_name": "RSI Overbought",
    "strategy": "pg_function",
    "function": "calc_rsi",
    "resolved_params": {"window": 14, "threshold": 70},
}

EXPECTED_SQL = "SELECT t.ticker, r.xymd, r.rsi_value FROM public.calc_rsi(14, CURRENT_DATE) r JOIN public.tickers t ON t.ticker_id = r.ticker_id WHERE r.rsi_value > 70 LIMIT 100;"


def _mock_anthropic(sql: str):
    mock_response = MagicMock()
    mock_response.content = sql
    mock_instance = MagicMock()
    mock_instance.invoke.return_value = mock_response
    return mock_instance


def test_generate_returns_sql():
    with patch("pipeline.generate.ChatAnthropic", return_value=_mock_anthropic(EXPECTED_SQL)):
        state = {"metric_spec": RSI_SPEC, "execution_error": None, "conversation": []}
        result = generate(state)
    assert result["sql"] == EXPECTED_SQL
    assert result["execution_error"] is None


def test_generate_strips_markdown_fences():
    fenced = f"```sql\n{EXPECTED_SQL}\n```"
    with patch("pipeline.generate.ChatAnthropic", return_value=_mock_anthropic(fenced)):
        state = {"metric_spec": RSI_SPEC, "execution_error": None, "conversation": []}
        result = generate(state)
    assert "```" not in result["sql"]
    assert "SELECT" in result["sql"]
```

- [ ] **Step 5: Run tests**

```bash
cd metric_studio && pytest tests/test_generate.py -v
```

Expected: 2 tests pass.

- [ ] **Step 6: Commit**

```bash
git add metric_studio/pipeline/generate.py metric_studio/prompts/ metric_studio/tests/test_generate.py
git commit -m "feat: add GENERATE stage with Jinja2 prompts and few-shot examples"
```

---

## Task 11: EXECUTE Stage

**Files:**
- Create: `metric_studio/pipeline/execute.py`
- Create: `metric_studio/tests/test_execute.py`

- [ ] **Step 1: Write failing tests**

```python
# metric_studio/tests/test_execute.py
import pandas as pd
from unittest.mock import MagicMock, patch
from pipeline.execute import execute


def _mock_engine(rows, columns):
    mock_result = MagicMock()
    mock_result.fetchall.return_value = rows
    mock_result.keys.return_value = columns
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value = mock_result
    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_conn
    return mock_engine


def test_execute_returns_dataframe():
    engine = _mock_engine(
        rows=[("AAPL", "2026-05-30", 74.3, 189.50)],
        columns=["ticker", "xymd", "rsi_value", "clos"],
    )
    with patch("pipeline.execute.create_engine", return_value=engine):
        state = {"sql": "SELECT 1"}
        result = execute(state)

    assert result["execution_error"] is None
    assert isinstance(result["result"], pd.DataFrame)
    assert list(result["result"].columns) == ["ticker", "xymd", "rsi_value", "clos"]
    assert result["result"].iloc[0]["ticker"] == "AAPL"


def test_execute_captures_db_error():
    mock_engine = MagicMock()
    mock_engine.connect.side_effect = Exception("SSL connection error")
    with patch("pipeline.execute.create_engine", return_value=mock_engine):
        state = {"sql": "SELECT 1"}
        result = execute(state)

    assert result["result"] is None
    assert "SSL connection error" in result["execution_error"]


def test_execute_returns_empty_dataframe_for_zero_rows():
    engine = _mock_engine(rows=[], columns=["ticker", "xymd", "rsi_value", "clos"])
    with patch("pipeline.execute.create_engine", return_value=engine):
        state = {"sql": "SELECT 1 WHERE FALSE"}
        result = execute(state)

    assert result["execution_error"] is None
    assert len(result["result"]) == 0
```

- [ ] **Step 2: Run to confirm failure**

```bash
cd metric_studio && pytest tests/test_execute.py -v
```

Expected: `ImportError` — `pipeline.execute` does not exist yet.

- [ ] **Step 3: Write pipeline/execute.py**

```python
# metric_studio/pipeline/execute.py
import pandas as pd
from sqlalchemy import create_engine, text
from pipeline.state import AgentState
from config import settings


def execute(state: AgentState) -> AgentState:
    engine = create_engine(settings.db_url)
    try:
        with engine.connect() as conn:
            result = conn.execute(text(state["sql"]))
            df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
        return {**state, "result": df, "execution_error": None}
    except Exception as exc:
        return {**state, "result": None, "execution_error": str(exc)}
```

- [ ] **Step 4: Run tests**

```bash
cd metric_studio && pytest tests/test_execute.py -v
```

Expected: all 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add metric_studio/pipeline/execute.py metric_studio/tests/test_execute.py
git commit -m "feat: add EXECUTE stage with SQLAlchemy runner"
```

---

## Task 12: PRESENT Stage

**Files:**
- Create: `metric_studio/pipeline/present.py`

- [ ] **Step 1: Write pipeline/present.py**

```python
# metric_studio/pipeline/present.py
import pandas as pd
from rich.console import Console
from rich.table import Table
from pipeline.state import AgentState

console = Console()
MAX_DISPLAY_ROWS = 100


def present(state: AgentState) -> None:
    df: pd.DataFrame = state.get("result")

    if df is None or df.empty:
        console.print("[yellow]No results returned.[/yellow]")
        return

    table = Table(show_header=True, header_style="bold cyan", border_style="dim")
    for col in df.columns:
        table.add_column(str(col), no_wrap=False)

    display_df = df.head(MAX_DISPLAY_ROWS)
    for _, row in display_df.iterrows():
        table.add_row(*[str(v) for v in row])

    console.print(table)
    suffix = f" (showing first {MAX_DISPLAY_ROWS})" if len(df) > MAX_DISPLAY_ROWS else ""
    console.print(f"[dim]{len(df)} rows returned{suffix}.[/dim]")
```

- [ ] **Step 2: Verify render manually**

```python
# Run in a Python shell from metric_studio/
import pandas as pd
from pipeline.present import present

state = {
    "result": pd.DataFrame({
        "ticker": ["AAPL", "NVDA"],
        "xymd": ["2026-05-30", "2026-05-30"],
        "rsi_value": [74.3, 81.2],
        "clos": [189.50, 875.20],
    })
}
present(state)
```

Expected: a formatted rich table prints to the terminal with headers and 2 rows.

- [ ] **Step 3: Commit**

```bash
git add metric_studio/pipeline/present.py
git commit -m "feat: add PRESENT stage with rich table renderer"
```

---

## Task 13: CLI REPL + Integration

**Files:**
- Create: `metric_studio/main.py`
- Create: `metric_studio/tests/test_integration.py`

- [ ] **Step 1: Write main.py**

```python
# metric_studio/main.py
import sys
from pathlib import Path
import yaml
from rich.console import Console

from pipeline.state import AgentState
from pipeline.understand import understand
from pipeline.clarify import clarify
from pipeline.specify import specify
from pipeline.generate import generate
from pipeline.execute import execute
from pipeline.present import present

console = Console()
CATALOG_PATH = Path(__file__).parent / "catalog" / "metrics.yaml"


def load_catalog() -> list[dict]:
    with open(CATALOG_PATH) as f:
        return yaml.safe_load(f)


def run_query(raw_query: str, conversation: list, catalog: list) -> AgentState:
    state: AgentState = {
        "raw_query": raw_query,
        "intent": "",
        "metric_id": None,
        "resolved_params": {},
        "unresolved_params": [],
        "metric_spec": None,
        "sql": None,
        "result": None,
        "execution_error": None,
        "conversation": conversation,
    }

    console.print("[dim]Analyzing query...[/dim]")
    state = understand(state, catalog)

    if state["metric_id"] is None:
        console.print("[yellow]Could not match your query to a known metric pattern.[/yellow]")
        console.print("Available: " + ", ".join(m["display_name"] for m in catalog))
        return state

    state = clarify(state, catalog)

    try:
        state = specify(state)
    except ValueError as exc:
        console.print(f"[red]Error: {exc}[/red]")
        return state

    console.print("[dim]Generating SQL...[/dim]")
    state = generate(state)

    console.print("[dim]Executing query...[/dim]")
    state = execute(state)

    if state["execution_error"]:
        console.print("[yellow]Query error — attempting self-correction...[/yellow]")
        state = generate(state)
        state = execute(state)

    if state["execution_error"]:
        console.print(f"[red]Execution failed: {state['execution_error']}[/red]")
        return state

    present(state)
    return state


def main() -> None:
    catalog = load_catalog()
    conversation: list[dict] = []

    console.print("[bold]Metric Studio[/bold] — NL2SQL Agent for Securities Time-Series")
    console.print("Type your query, or 'exit' to quit.\n")

    while True:
        try:
            raw_query = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\nGoodbye.")
            break

        if raw_query.lower() in ("exit", "quit", "q"):
            console.print("Goodbye.")
            break

        if not raw_query:
            continue

        state = run_query(raw_query, conversation, catalog)

        if state.get("sql"):
            conversation.append({"role": "user", "content": raw_query})
            conversation.append({"role": "assistant", "content": state["sql"]})

        console.print()
        try:
            action = input("[r] Refine  [n] New query  [q] Quit: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            break

        if action == "q":
            console.print("Goodbye.")
            break
        elif action == "n":
            conversation = []


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Write integration test**

```python
# metric_studio/tests/test_integration.py
"""
Integration test: wires all stages together with mocked LLM and DB.
Verifies that a full query round-trip produces a DataFrame result.
"""
import pandas as pd
from unittest.mock import MagicMock, patch
from main import run_query
from pipeline.understand import UnderstandOutput

CATALOG = [
    {
        "id": "rsi_overbought",
        "display_name": "RSI Overbought",
        "aliases": ["overbought"],
        "strategy": "pg_function",
        "function": "calc_rsi",
        "params": {
            "window":    {"type": "integer", "clarify_if_missing": False, "default": 14},
            "threshold": {"type": "numeric", "clarify_if_missing": False, "default": 70},
        },
    }
]

FAKE_SQL = "SELECT t.ticker FROM public.calc_rsi(14, CURRENT_DATE) r JOIN public.tickers t ON t.ticker_id = r.ticker_id WHERE r.rsi_value > 70 LIMIT 100;"

FAKE_DF = pd.DataFrame({
    "ticker": ["AAPL"],
    "xymd": ["2026-05-30"],
    "rsi_value": [74.3],
    "clos": [189.50],
})


def _mock_understand():
    output = UnderstandOutput(
        intent="Find overbought stocks",
        metric_id="rsi_overbought",
        resolved_params={"window": 14, "threshold": 70},
    )
    structured = MagicMock()
    structured.invoke.return_value = output
    llm = MagicMock()
    llm.with_structured_output.return_value = structured
    return llm


def _mock_generate():
    response = MagicMock()
    response.content = FAKE_SQL
    llm = MagicMock()
    llm.invoke.return_value = response
    return llm


def _mock_db_engine():
    mock_result = MagicMock()
    mock_result.fetchall.return_value = [tuple(FAKE_DF.iloc[0])]
    mock_result.keys.return_value = list(FAKE_DF.columns)
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value = mock_result
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine


def test_full_pipeline_returns_dataframe():
    with (
        patch("pipeline.understand.ChatAnthropic", return_value=_mock_understand()),
        patch("pipeline.generate.ChatAnthropic", return_value=_mock_generate()),
        patch("pipeline.execute.create_engine", return_value=_mock_db_engine()),
    ):
        state = run_query("Find overbought stocks", [], CATALOG)

    assert state["result"] is not None
    assert isinstance(state["result"], pd.DataFrame)
    assert state["execution_error"] is None
    assert state["result"].iloc[0]["ticker"] == "AAPL"
```

- [ ] **Step 3: Run integration test**

```bash
cd metric_studio && pytest tests/test_integration.py -v
```

Expected: 1 test passes.

- [ ] **Step 4: Run all tests together**

```bash
cd metric_studio && pytest tests/ -v
```

Expected: all tests pass, no errors.

- [ ] **Step 5: Smoke-test the CLI with real credentials**

```bash
cd metric_studio && python main.py
```

Type: `Find overbought stocks`
Expected: CLARIFY questions appear (if params missing), then SQL is generated, executed against live DB, and results render as a rich table.

- [ ] **Step 6: Commit**

```bash
git add metric_studio/main.py metric_studio/tests/test_integration.py
git commit -m "feat: wire CLI REPL with full pipeline and integration test"
```

---

## Self-Review

**Spec coverage:**
- Natural-language query input → Task 13 (main.py REPL)
- Ambiguity detection → Task 8 (UNDERSTAND extracts unresolved_params)
- Clarifying questions → Task 9 (CLARIFY stage)
- Metric specification → Task 7 (SPECIFY stage) + Task 6 (catalog YAML)
- Schema-aware SQL generation → Task 10 (GENERATE with schema context)
- Query execution → Task 11 (EXECUTE with SQLAlchemy)
- Relational result presentation → Task 12 (PRESENT with rich)
- Precomputed tables → Task 3 (migrations) + Task 5 (batch job)
- PostgreSQL functions → Task 4 (migrations)
- Daily update policy → Task 5 (batch job with `--full` / incremental modes)
- User feedback loop → Task 13 (main.py refine/new/quit prompt)
- Interpretability → GENERATE prompt includes metric spec and parameters used

**No placeholders found.**

**Type consistency verified:** `AgentState` keys are used consistently across all stage files. `metric_spec["function"]` and `metric_spec["table"]` are set by `specify.py` and consumed by `generate.py`'s `_schema_context`. `UnderstandOutput` fields match what `understand.py` maps into `AgentState`.
