CREATE TABLE IF NOT EXISTS public.daily_atr (
    ticker_id  INTEGER NOT NULL REFERENCES public.tickers(ticker_id),
    xymd       DATE    NOT NULL,
    true_range NUMERIC(19,8),
    atr_14     NUMERIC(19,8),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ticker_id, xymd)
);

CREATE INDEX IF NOT EXISTS daily_atr_xymd_idx ON public.daily_atr (xymd DESC);
