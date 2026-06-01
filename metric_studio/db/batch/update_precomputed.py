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
