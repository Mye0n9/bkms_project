CREATE TABLE IF NOT EXISTS public.daily_obv (
    ticker_id  INTEGER NOT NULL REFERENCES public.tickers(ticker_id),
    xymd       DATE    NOT NULL,
    obv        BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ticker_id, xymd)
);

CREATE INDEX IF NOT EXISTS daily_obv_xymd_idx ON public.daily_obv (xymd DESC);
