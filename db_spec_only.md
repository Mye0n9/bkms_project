# Database Schema Structure (PostgreSQL + TimescaleDB)

This document is an English translation of the provided database schema specification only.

## 1. `tickers` — Ticker Master Table

The `tickers` table is the master table for stock ticker information.

The schema introduces `ticker_id` as a surrogate primary key instead of using the natural key `ticker`. This allows the system to handle ticker change events more flexibly.

To improve KIS API data collection speed, the schema includes the standard product number `std_pdno` and separate flags for identifying ETF and ETN instruments.

| Column Name | Data Type | Constraint | Description |
|---|---|---|---|
| `ticker_id` | `SERIAL` | `PRIMARY KEY` | System-managed surrogate key, auto-incrementing integer |
| `ticker` | `VARCHAR(10)` | `NOT NULL` | Standardized ticker symbol for analysis, for example `AAPL`, `SCHW.D`, `BF.A` |
| `ticker_raw` | `VARCHAR(30)` |  | Raw ticker format for KIS API requests, for example `SCHW D`, `BF/A` |
| `exchange_code` | `VARCHAR(10)` | `NOT NULL` | Exchange code, fixed to `NAS`, `NYS`, or `AMS` |
| `name_ko` | `VARCHAR(150)` |  | Korean stock name |
| `prdt_eng_name` | `VARCHAR(150)` |  | English stock name |
| `std_pdno` | `VARCHAR(12)` | `NOT NULL` | KIS standard product number, ISIN code, used as a defensive key against ticker reuse |
| `is_etf` | `BOOLEAN` | `DEFAULT FALSE` | Flag indicating whether the instrument is an ETF |
| `is_etn` | `BOOLEAN` | `DEFAULT FALSE` | Flag indicating whether the instrument is an ETN |
| `ovrs_papr` | `NUMERIC(19, 4)` |  | Overseas par value, used to detect possible stock splits or reverse splits |
| `ovrs_stck_hist_rght_dvsn_cd` | `VARCHAR(2)` |  | Overseas stock history rights classification code |
| `natn_cd` | `VARCHAR(3)` |  | Country code, for example `US` |
| `tr_mket_cd` | `VARCHAR(2)` |  | Trading market code |
| `tr_crcy_cd` | `VARCHAR(3)` |  | Trading currency code, for example `USD` |
| `lstg_stck_num` | `BIGINT` |  | Number of listed shares |
| `lstg_dt` | `DATE` |  | Listing date |
| `lstg_abol_item_yn` | `BOOLEAN` | `DEFAULT FALSE` | Whether the instrument is delisted, mapped from `Y/N` |
| `lstg_abol_dt` | `DATE` |  | Delisting date |
| `lstg_yn` | `BOOLEAN` | `DEFAULT TRUE` | Whether the instrument is listed |
| `chng_bf_pdno` | `VARCHAR(12)` |  | Previous product number, used to detect ticker changes |
| `ptp_item_yn` | `BOOLEAN` | `DEFAULT FALSE` | Whether the instrument is a PTP item |
| `dtm_tr_psbl_yn` | `BOOLEAN` | `DEFAULT FALSE` | Whether daytime fractional trading is available |
| `updated_at` | `TIMESTAMP` | `DEFAULT NOW()` | Last synchronization timestamp for master information |

## 2. `daily_prices` — Daily Price Hypertable

The `daily_prices` table stores daily price data.

The primary key is a composite key consisting of the foreign key `ticker_id` and the business date `xymd`.

The reserved SQL word `open` must be handled separately with double quotes. Price columns use the `NUMERIC` type for precise calculation.

| Column Name | Data Type | Constraint | Description |
|---|---|---|---|
| `ticker_id` | `INTEGER` | `PK-1`, `FK` referencing `tickers` | Foreign key for identifying the instrument |
| `xymd` | `DATE` | `PK-2`, time axis | Business date loaded in `YYYY-MM-DD` format |
| `clos` | `NUMERIC(19, 8)` | `NOT NULL` | Adjusted closing price |
| `"open"` | `NUMERIC(19, 8)` | `NOT NULL` | Adjusted opening price; double quotes are required because `open` is a SQL reserved word |
| `high` | `NUMERIC(19, 8)` | `NOT NULL` | Adjusted high price |
| `low` | `NUMERIC(19, 8)` | `NOT NULL` | Adjusted low price |
| `tvol` | `BIGINT` | `NOT NULL` | Daily trading volume, stored as a large integer |
| `tamt` | `NUMERIC(24, 4)` |  | Daily trading amount, preserved in 1-dollar units |
| `sign` | `VARCHAR(1)` |  | Comparison sign |
| `diff` | `NUMERIC(19, 8)` |  | Absolute price change compared to the previous day |
| `rate` | `NUMERIC(6, 2)` |  | Rate of change, percentage |
| `created_at` | `TIMESTAMP` | `DEFAULT NOW()` | Initial data load timestamp |

### TimescaleDB Optimization Policy

- Partitioning: automatically split chunks by 3-month intervals based on the `xymd` time axis.
- Storage compression: automatically compress chunks older than 3 months by segmenting by `ticker_id` and ordering by `xymd` in descending order.
- The compression policy is intended to reduce disk usage by up to 90%.

## 3. `sync_status` — Collection History Management Table

The `sync_status` table manages collection status for each ticker.

The table has a one-to-one relationship with the `tickers` table by using `ticker_id` as both the primary key and foreign key.

It includes an error message column for failure handling and a field to track the latest synchronization date.

| Column Name | Data Type | Constraint | Description |
|---|---|---|---|
| `ticker_id` | `INTEGER` | `PRIMARY KEY`, `FK` | References the `tickers` table |
| `last_synced_date` | `DATE` |  | Latest business date for which synchronization was completed for the instrument, in `YYYY-MM-DD` format |
| `status` | `VARCHAR(20)` | `DEFAULT 'PENDING'` | Collection progress status, based on the status codes below |
| `error_message` | `TEXT` |  | Error log used to trace the cause of collection failure |
| `updated_at` | `TIMESTAMP` | `DEFAULT NOW()` | Last timestamp when the collection status changed |

### Standardized Status Codes

| Status Code | Description |
|---|---|
| `PENDING` | Initial registration state, or waiting state for full recollection caused by split or reverse split detection |
| `RUNNING` | Price collection is currently running through the API |
| `COMPLETED` | Historical data backfill and synchronization have been fully completed up to the configured latest business date |
| `FAILED` | Collection ended abnormally due to communication errors, API response rejection, or similar issues; the item is automatically skipped and retried in the next daily batch |
| `DELISTED` | Delisted ghost instrument; existing loaded data is preserved, but unnecessary API calls are immediately blocked |
| `SKIPPED` | Non-target instrument declared by the KIS API as a non-existing ticker |
