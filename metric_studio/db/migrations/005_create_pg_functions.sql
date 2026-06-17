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

-- Volatility: rolling standard deviation of daily log returns, annualized
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
           ROUND((rv.volatility * SQRT(252) * 100)::numeric, 4) AS volatility,
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
