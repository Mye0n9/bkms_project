CREATE TABLE IF NOT EXISTS public.daily_bb (
    ticker_id  INTEGER NOT NULL REFERENCES public.tickers(ticker_id),
    xymd       DATE    NOT NULL,
    ma_20      NUMERIC(19,8),
    upper_band NUMERIC(19,8),
    lower_band NUMERIC(19,8),
    bandwidth  NUMERIC(19,8),
    pct_b      NUMERIC(19,8),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ticker_id, xymd)
);

CREATE INDEX IF NOT EXISTS daily_bb_xymd_idx ON public.daily_bb (xymd DESC);
