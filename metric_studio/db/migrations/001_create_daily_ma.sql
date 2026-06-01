CREATE TABLE IF NOT EXISTS public.daily_ma (
    ticker_id  INTEGER NOT NULL REFERENCES public.tickers(ticker_id),
    xymd       DATE    NOT NULL,
    ma_5       NUMERIC(19,8),
    ma_10      NUMERIC(19,8),
    ma_20      NUMERIC(19,8),
    ma_50      NUMERIC(19,8),
    ma_200     NUMERIC(19,8),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ticker_id, xymd)
);

CREATE INDEX IF NOT EXISTS daily_ma_xymd_idx ON public.daily_ma (xymd DESC);
