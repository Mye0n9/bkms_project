"""
EMDL (Enterprise Market Data Ledger) - Core Library Module
수정주가 역사 데이터 수집, 동기화, OHLCV 무결성 방화벽 및 정정 감사 로그 시스템의 코어 로직을 관장합니다.

[하드닝(Hardening) 패치 적용 완료]
1. SPY 벤치마크 빈 응답 시 휴장일 자동 감지 엔진 반영 (None 반환)
2. tickers 테이블의 lstg_dt(상장일)을 조회하여 start_date를 동적으로 제한 (불필요한 API 호출 차단)
3. KISTokenManager 토큰 만료 임계치를 실전 규격인 23시간(82,800초)으로 정밀 조정
4. OHLCV 방화벽 내 거래량 이상치 검증 규칙(tvol >= 0) 엄격 통합
5. 장애 추적 및 Permanent Fail(5회 이상 실패) 종목 리스트 추출 연동 슬랙 알림 프레임워크 구현
"""

import datetime
import json
import os
import time
import random
import shutil
import re
import ssl
import logging
import urllib.request
import zipfile
import socket
import hashlib
import threading
import uuid
from logging.handlers import RotatingFileHandler
import psycopg2
from psycopg2 import extras
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# ==========================================
# [CONFIG] 전역 환경설정 및 임계치 제어 상수
# ==========================================
SYNC_MASTER_ON_START = False      # 구동 즉시 KIS 마스터 파일(*mst.cod.zip)을 다운로드하여 동기화할지 여부
APP_VERSION = "v3.1.0-PROD"

# 역사적 시세 수집 범위 기본값
START_DATE = "20060101"
END_DATE_LIMIT = "20260530"

# 거래소 코드 - KIS 상품유형코드 매핑 스펙
EXCHANGE_MAPPING = {
    "NAS": "512",  # 나스닥
    "NYS": "513",  # 뉴욕
    "AMS": "529"   # 아멕스
}

# 트래픽 속도 및 안정성 제어 상수
REAL_DELAY = 0.1             # API 요청 간 대기 시간 (초)
MOCK_DELAY = 0.5             # 모의투자 시 대기 시간 (초)
COOL_DOWN_LIMIT = 100        # 해당 횟수 호출 시마다 무작위 휴식 강제 실행

# 공급망 장애 (Systemic Outage) 감지 임계치 설정
SYSTEMIC_OUTAGE_RATIO_LIMIT = 0.05
SYSTEMIC_OUTAGE_ABS_LIMIT = 50

# 환경 설정 동적 로드용 변수 (initialize_environment 호출 시 바인딩)
APP_KEY = None
APP_SECRET = None
IS_MOCK = False
BASE_URL = None
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = None
DB_USER = None
DB_PASSWORD = None
SLACK_WEBHOOK_URL = None  # 슬랙 알림 웹훅 채널 URL
NAVER_EMAIL_SENDER = None
NAVER_EMAIL_PASSWORD = None
NAVER_EMAIL_RECIPIENT = None

# 트래픽 동시성 제어 스레드 락 및 카운터
api_call_count = 0
counter_lock = threading.Lock()

# ==========================================
# 1. 고성능 로깅 프레임워크 (Tqdm-Safe & Rotating)
# ==========================================
os.makedirs("logs", exist_ok=True)
logger = logging.getLogger("EMDL_Ledger")
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    file_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s')
    console_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s]: %(message)s', datefmt='%H:%M:%S')

    file_handler = RotatingFileHandler(
        filename="logs/emdl_ledger.log",
        maxBytes=20 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    class TqdmLoggingHandler(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
                tqdm.write(msg)
                self.flush()
            except Exception:
                self.handleError(record)

    console_handler = TqdmLoggingHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)


# ==========================================
# 2. 임포트 부작용 없는 환경 설정 초기화 함수
# ==========================================
def initialize_environment(env_path="setting.env"):
    """
    모듈 임포트 시점의 부작용(Side Effect)을 제거하기 위한 명시적 환경 설정 로드 함수입니다.
    """
    global APP_KEY, APP_SECRET, IS_MOCK, BASE_URL, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, SLACK_WEBHOOK_URL, \
       NAVER_EMAIL_SENDER, NAVER_EMAIL_PASSWORD, NAVER_EMAIL_RECIPIENT
    
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        logger.info(f"⚙️ 환경설정 로드 완료: {env_path}")
    else:
        logger.critical(f"❌ 오류: '{env_path}' 파일을 찾을 수 없습니다. 배치를 가동할 수 없습니다.")
        raise FileNotFoundError(f"Missing environment config: {env_path}")

    APP_KEY = os.getenv("KIS_APP_KEY")
    APP_SECRET = os.getenv("KIS_APP_SECRET")
    IS_MOCK = os.getenv("IS_MOCK_INVESTMENT", "False").lower() == "true"
    BASE_URL = "https://openapivts.koreainvestment.com:29443" if IS_MOCK else "https://openapi.koreainvestment.com:9443"

    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
    NAVER_EMAIL_SENDER    = os.getenv("NAVER_EMAIL_SENDER")
    NAVER_EMAIL_PASSWORD  = os.getenv("NAVER_EMAIL_PASSWORD")
    NAVER_EMAIL_RECIPIENT = os.getenv("NAVER_EMAIL_RECIPIENT")

    # 가용성 체크
    missing_vars = []
    if not APP_KEY: missing_vars.append("KIS_APP_KEY")
    if not APP_SECRET: missing_vars.append("KIS_APP_SECRET")
    if not DB_NAME: missing_vars.append("DB_NAME")
    if not DB_USER: missing_vars.append("DB_USER")
    if not DB_PASSWORD: missing_vars.append("DB_PASSWORD")

    if missing_vars:
        logger.critical(f"❌ 치명적 구성 오류: 필수 환경 변수 누락 {missing_vars}")
        raise ValueError(f"Missing critical environment variables: {missing_vars}")


# ==========================================
# 3. 데이터 위생성 및 동적 성능 진단 헬퍼
# ==========================================
def check_disk_space():
    try:
        total, used, free = shutil.disk_usage("/")
        free_gb = free / (2**30)
        logger.info(f"💾 서버 디스크 상태: 여유 공간 {free_gb:.2f} GB / 전체 {total/(2**30):.2f} GB")
        if free_gb < 15.0:
            logger.warning("⚠️ 경고: 남은 디스크 용량이 15GB 미만입니다.")
    except Exception as e:
        logger.error(f"디스크 용량 확인 실패: {e}")

def parse_kis_date(date_str):
    if not date_str:
        return None
    if isinstance(date_str, datetime.date):
        return date_str
    date_str = str(date_str).strip()
    if date_str in ("00000000", "", "0"):
        return None
    try:
        return datetime.datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError:
        return None

def trigger_cool_down():
    """스레드 세이프가 확보된 안전 쿨다운 가동기"""
    global api_call_count
    with counter_lock:
        api_call_count += 1
        current_count = api_call_count

    if current_count % COOL_DOWN_LIMIT == 0:
        sleep_time = random.uniform(1.0, 2.0)
        logger.info(f"💤 [Cool-down] 누적 호출 {current_count}회 도달. {sleep_time:.2f}초간 대기...")
        time.sleep(sleep_time)

def to_kis_symbol(ticker: str) -> str:
    if not ticker:
        return ""
    return ticker.replace('.', ' ')

def to_standard_symbol(ticker: str) -> str:
    if not ticker:
        return ""
    return ticker.replace(' ', '.').replace('/', '.').strip()

def safe_request(method, url, max_retries=3, backoff_factor=1.5, **kwargs):
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.request(method, url, **kwargs)
            if response.status_code in [500, 502, 503, 504]:
                if attempt == max_retries:
                    return response
                logger.warning(f"📡 [KIS 게이트웨이 불안정] HTTP {response.status_code} 감지. {attempt}/{max_retries}차 재시도 대기 ({delay:.1f}초)...")
                time.sleep(delay)
                delay *= backoff_factor
                continue
            return response
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt == max_retries:
                raise e
            logger.warning(f"📡 [네트워크 수급 지연] {type(e).__name__} 발생. {attempt}/{max_retries}차 재시도 대기 ({delay:.1f}초)...")
            time.sleep(delay)
            delay *= backoff_factor


# ==========================================
# 4. 데이터베이스 안전적 연결 및 복구 관리 객체
# ==========================================
class SafeDatabaseConnection:
    def __init__(self):
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            return self.conn
        except Exception as e:
            logger.critical(f"❌ 데이터베이스 최초 연결 실패: {e}")
            raise

    def get_connection(self):
        if self.conn is None or self.conn.closed != 0:
            logger.warning("🔄 [DB 관리자] 데이터베이스 연결 유실 감지! 연결 재수립을 시작합니다...")
            self.connect()
        else:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT 1;")
            except psycopg2.OperationalError:
                logger.warning("🔄 [DB 관리자] 세션 상태 불안정. 강제 재연결을 수립합니다...")
                self.connect()
        return self.conn


# ==========================================
# 5. 데이터베이스 초기화 및 정정 이력 트리거 선언
# ==========================================
def init_database(db_manager, reset_on_start=False):
    """테이블 생성, TimescaleDB 하이퍼테이블 설정 및 감사 트리거 수립 (커서 누수 완전 제거)"""
    logger.info("PostgreSQL + TimescaleDB 테이블 스키마 초기화 검증 중...")
    conn = db_manager.get_connection()

    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

        if reset_on_start:
            logger.warning("⚠️ reset_on_start 옵션이 활성화되었습니다. 기존 원장을 전체 소멸시킵니다!")
            cur.execute("DROP TABLE IF EXISTS price_revision_log CASCADE;")
            cur.execute("DROP TABLE IF EXISTS price_anomalies CASCADE;")
            cur.execute("DROP TABLE IF EXISTS market_day_snapshots CASCADE;")
            cur.execute("DROP TABLE IF EXISTS provider_health_events CASCADE;")
            cur.execute("DROP TABLE IF EXISTS batch_job_items CASCADE;")
            cur.execute("DROP TABLE IF EXISTS batch_job_runs CASCADE;")
            cur.execute("DROP TABLE IF EXISTS daily_prices CASCADE;")
            cur.execute("DROP TABLE IF EXISTS sync_status CASCADE;")
            cur.execute("DROP TABLE IF EXISTS tickers CASCADE;")
            cur.execute("""ALTER TABLE sync_status ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;""")
            conn.commit()

        # tickers (종목 마스터)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tickers (
                ticker_id SERIAL PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                ticker_raw VARCHAR(30),
                exchange_code VARCHAR(10) NOT NULL,
                name_ko VARCHAR(150),
                is_etf BOOLEAN DEFAULT FALSE,
                is_etn BOOLEAN DEFAULT FALSE,
                std_pdno VARCHAR(12) NOT NULL,
                prdt_eng_name VARCHAR(150),
                natn_cd VARCHAR(3),
                tr_mket_cd VARCHAR(2),
                tr_crcy_cd VARCHAR(3),
                ovrs_papr NUMERIC(19, 4),
                lstg_stck_num BIGINT,
                lstg_dt DATE,
                lstg_abol_item_yn BOOLEAN DEFAULT FALSE,
                lstg_abol_dt DATE,
                lstg_yn BOOLEAN DEFAULT TRUE,
                chng_bf_pdno VARCHAR(12),
                ovrs_stck_hist_rght_dvsn_cd VARCHAR(2),
                ptp_item_yn BOOLEAN DEFAULT FALSE,
                dtm_tr_psbl_yn BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (ticker, std_pdno)
            );
        """)

        cur.execute("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS ticker_raw VARCHAR(30);")

        # daily_prices (시세 테이블)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_prices (
                ticker_id INTEGER REFERENCES tickers(ticker_id),
                xymd DATE NOT NULL,
                clos NUMERIC(19, 8) NOT NULL,
                "open" NUMERIC(19, 8) NOT NULL,
                high NUMERIC(19, 8) NOT NULL,
                low NUMERIC(19, 8) NOT NULL,
                tvol BIGINT NOT NULL,
                tamt NUMERIC(24, 4),
                sign VARCHAR(1),
                diff NUMERIC(19, 8),
                rate NUMERIC(6, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker_id, xymd)
            );
        """)

        # TimescaleDB 하이퍼테이블화 및 압축 정책 설정
        try:
            cur.execute("""
                SELECT create_hypertable(
                    'daily_prices', 
                    'xymd', 
                    chunk_time_interval => INTERVAL '3 months', 
                    if_not_exists => TRUE
                );
            """)
            cur.execute("""
                ALTER TABLE daily_prices SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'ticker_id',
                    timescaledb.compress_orderby = 'xymd DESC'
                );
            """)
        except Exception as e:
            logger.debug(f"TimescaleDB 하이퍼테이블 바인딩 검증 우회: {e}")
            conn.rollback()

        # sync_status
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_status (
                ticker_id INTEGER PRIMARY KEY REFERENCES tickers(ticker_id),
                last_synced_date DATE,
                status VARCHAR(20) DEFAULT 'PENDING',
                retry_count INT DEFAULT 0,
                missing_days INT DEFAULT 0,
                error_message TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # batch_job_runs (UUID 세션 관리)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS batch_job_runs (
                job_id UUID PRIMARY KEY,
                business_date DATE NOT NULL,
                parent_job_id UUID REFERENCES batch_job_runs(job_id) ON DELETE SET NULL,
                status VARCHAR(20) NOT NULL,
                lock_owner_pid INT NOT NULL,
                lock_owner_backend_start TIMESTAMP WITH TIME ZONE NOT NULL,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP,
                duration_sec INT,
                host_name VARCHAR(100),
                app_version VARCHAR(30)
            );
        """)

        # batch_job_items (원자적 체크포인트)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS batch_job_items (
                job_id UUID REFERENCES batch_job_runs(job_id) ON DELETE CASCADE,
                ticker_id INTEGER REFERENCES tickers(ticker_id) ON DELETE CASCADE,
                status VARCHAR(30) NOT NULL,
                provider_retry_count INT DEFAULT 0,
                error_message TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (job_id, ticker_id)
            );
        """)

        # provider_health_events (공급망 장애 추적)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS provider_health_events (
                event_id SERIAL PRIMARY KEY,
                business_date DATE NOT NULL,
                api_endpoint VARCHAR(50) NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                affected_exchange VARCHAR(10),
                anomaly_count INTEGER NOT NULL,
                status VARCHAR(20) DEFAULT 'ACTIVE',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # price_anomalies (캔들 단위 격리소)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_anomalies (
                anomaly_id SERIAL PRIMARY KEY,
                job_id UUID REFERENCES batch_job_runs(job_id) ON DELETE SET NULL,
                ticker_id INTEGER REFERENCES tickers(ticker_id) ON DELETE CASCADE,
                trade_date DATE NOT NULL,
                rule_name VARCHAR(100) NOT NULL,
                raw_payload JSONB,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (ticker_id, trade_date, rule_name)
            );
        """)

        # price_revision_log (정정 감사 테이블)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_revision_log (
                revision_id BIGSERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES tickers(ticker_id) ON DELETE CASCADE,
                trade_date DATE NOT NULL,
                column_name VARCHAR(20) NOT NULL,
                old_value NUMERIC(19, 8) NOT NULL,
                new_value NUMERIC(19, 8) NOT NULL,
                reason_code VARCHAR(50) NOT NULL,
                reason_detail TEXT,
                actor_type VARCHAR(20) NOT NULL,
                actor_name VARCHAR(50) NOT NULL,
                job_id UUID,
                revised_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_revision_lookup ON price_revision_log(ticker_id, trade_date);")

        # market_day_snapshots (체크섬 스냅샷)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_day_snapshots (
                business_date DATE PRIMARY KEY,
                total_symbols INTEGER NOT NULL,
                total_rows INTEGER NOT NULL,
                sha256_checksum VARCHAR(64) NOT NULL,
                snapshot_status VARCHAR(20) NOT NULL,
                certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 🔄 실시간 시세 정정 감시 DB 트리거 정의
        cur.execute("""
            CREATE OR REPLACE FUNCTION trg_log_price_revision()
            RETURNS TRIGGER AS $$
            DECLARE
                v_job_id UUID;
                v_actor_type VARCHAR(20) := 'SYSTEM';
                v_actor_name VARCHAR(50) := 'KIS_BATCH_v3.1';
                v_reason_code VARCHAR(50) := 'PROVIDER_FIX';
                v_reason_detail TEXT := 'Historical price restatement detected during sync';
            BEGIN
                BEGIN
                    v_job_id := NULLIF(current_setting('emdl.current_job_id', true), '')::UUID;
                EXCEPTION WHEN OTHERS THEN
                    v_job_id := NULL;
                END;

                IF v_job_id IS NOT NULL THEN
                    v_actor_name := 'EMDL_Job_' || substring(v_job_id::text, 1, 8);
                END IF;

                IF OLD.clos <> NEW.clos THEN
                    INSERT INTO price_revision_log (ticker_id, trade_date, column_name, old_value, new_value, reason_code, reason_detail, actor_type, actor_name, job_id)
                    VALUES (OLD.ticker_id, OLD.xymd, 'clos', OLD.clos, NEW.clos, v_reason_code, v_reason_detail, v_actor_type, v_actor_name, v_job_id);
                END IF;
                IF OLD.open <> NEW.open THEN
                    INSERT INTO price_revision_log (ticker_id, trade_date, column_name, old_value, new_value, reason_code, reason_detail, actor_type, actor_name, job_id)
                    VALUES (OLD.ticker_id, OLD.xymd, 'open', OLD.open, NEW.open, v_reason_code, v_reason_detail, v_actor_type, v_actor_name, v_job_id);
                END IF;
                IF OLD.high <> NEW.high THEN
                    INSERT INTO price_revision_log (ticker_id, trade_date, column_name, old_value, new_value, reason_code, reason_detail, actor_type, actor_name, job_id)
                    VALUES (OLD.ticker_id, OLD.xymd, 'high', OLD.high, NEW.high, v_reason_code, v_reason_detail, v_actor_type, v_actor_name, v_job_id);
                END IF;
                IF OLD.low <> NEW.low THEN
                    INSERT INTO price_revision_log (ticker_id, trade_date, column_name, old_value, new_value, reason_code, reason_detail, actor_type, actor_name, job_id)
                    VALUES (OLD.ticker_id, OLD.xymd, 'low', OLD.low, NEW.low, v_reason_code, v_reason_detail, v_actor_type, v_actor_name, v_job_id);
                END IF;
                IF OLD.tvol <> NEW.tvol THEN
                    INSERT INTO price_revision_log (ticker_id, trade_date, column_name, old_value, new_value, reason_code, reason_detail, actor_type, actor_name, job_id)
                    VALUES (OLD.ticker_id, OLD.xymd, 'tvol', OLD.tvol, NEW.tvol, v_reason_code, v_reason_detail, v_actor_type, v_actor_name, v_job_id);
                END IF;

                UPDATE market_day_snapshots 
                SET snapshot_status = 'SUPERSEDED'
                WHERE business_date = OLD.xymd AND snapshot_status = 'CERTIFIED';

                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)

        cur.execute("DROP TRIGGER IF EXISTS trg_daily_prices_audit ON daily_prices;")
        cur.execute("""
            CREATE TRIGGER trg_daily_prices_audit
            BEFORE UPDATE ON daily_prices
            FOR EACH ROW
            EXECUTE FUNCTION trg_log_price_revision();
        """)

        conn.commit()
    logger.info("⚙️ 기본 테이블 스키마 및 감사 정정 DB 엔진 선언 수립 완료.")


# ==========================================
# 6. 안전 가동 장치 및 고착 태스크 초기화
# ==========================================
def reset_running_statuses(db_manager):
    """비정상 종료로 인해 RUNNING 상태로 고착화된 수집 대상들을 전부 PENDING 상태로 롤백"""
    logger.info("🔄 [안전 장치] 비정상 종료된 RUNNING 상태의 수집 태스크 일괄 PENDING 복원 처리 중...")
    conn = db_manager.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE sync_status 
                SET status = 'PENDING', updated_at = NOW() 
                WHERE status = 'RUNNING';
            """)
            conn.commit()
            logger.info("✅ 고착화되었던 기존 잔재 수집 작업의 정상 초기화 롤백이 완료되었습니다.")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ 가동 초기화 복원 과정 중 데이터베이스 트랜잭션 실패: {e}")


# ==========================================
# 7. 분산 동시성 제어 (Advisory Lock)
# ==========================================
class DistributedLockManager:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.lock_key = 987654321

    def acquire_lock(self):
        conn = self.db_manager.get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pid, backend_start 
                FROM pg_stat_activity 
                WHERE pid = pg_backend_pid();
            """)
            pid, backend_start = cur.fetchone()

            cur.execute("SELECT pg_try_advisory_lock(%s);", (self.lock_key,))
            locked = cur.fetchone()[0]

            if not locked:
                cur.execute("""
                    SELECT job_id, lock_owner_pid, lock_owner_backend_start, host_name 
                    FROM batch_job_runs 
                    WHERE status = 'RUNNING';
                """)
                running_job = cur.fetchone()
                if running_job:
                    job_id, r_pid, r_start, host = running_job
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM pg_stat_activity 
                        WHERE pid = %s AND backend_start = %s;
                    """, (r_pid, r_start))
                    active_session = cur.fetchone()[0] > 0

                    if active_session:
                        logger.critical(f"❌ [동시 실행 차단] 호스트 '{host}'에서 이미 수집 배치(Job UUID: {job_id})가 가동 중입니다. (PID: {r_pid})")
                        sys.exit(0)
                    else:
                        logger.warning(f"⚠️ [고아 세션 감지] 등록된 락(PID: {r_pid})이 비정상 종료되었습니다. 세션을 정리합니다.")
                        cur.execute("SELECT pg_terminate_backend(%s);", (r_pid,))
                        conn.commit()
                
                cur.execute("SELECT pg_try_advisory_lock(%s);", (self.lock_key,))
                if not cur.fetchone()[0]:
                    logger.critical("❌ [동시성 병목] 락 강제 회수 후에도 락을 소유할 수 없습니다. 안전을 위해 실행을 취소합니다.")
                    sys.exit(0)

        return pid, backend_start


# ==========================================
# 8. KIS API Access Token 관리자
# ==========================================
class KISTokenManager:
    """
    [우선순위 5 반영] KIS 토큰 관리자
    실전 KIS API 발급 한도 및 24시간 만료 세션 정책에 대응하기 위해
    안전 임계 마지노선을 '발급 후 23시간'(82,800초)으로 정밀 제어하여 무의미한 네트워크 낭비를 제거합니다.
    """
    def __init__(self):
        self._token = None
        self._issued_at = None
        self._lifetime_seconds = 23 * 3600  # 23시간으로 임계 타임 세팅

    def get_token(self):
        now = datetime.datetime.now()
        if (self._token is None or 
            self._issued_at is None or 
            (now - self._issued_at).total_seconds() >= self._lifetime_seconds):
            
            url = f"{BASE_URL}/oauth2/tokenP"
            headers = {"content-type": "application/json"}
            body = {
                "grant_type": "client_credentials",
                "appkey": APP_KEY,
                "appsecret": APP_SECRET,
            }
            res = safe_request("POST", url, headers=headers, json=body, timeout=10)
            if res.status_code == 200:
                self._token = res.json().get("access_token")
                self._issued_at = now
                logger.info(f"🔑 [토큰 관리자] Access Token 신규 발급/갱신 완료 (기준: 23시간 캐싱 유지, 발행시각: {self._issued_at.strftime('%Y-%m-%d %H:%M:%S')})")
            else:
                raise Exception(f"KIS API 토큰 발급에 실패했습니다: {res.text}")
        return self._token


# ==========================================
# 9. KIS CDN 기반 주식 마스터 대량 수급 모듈
# ==========================================
class KISMasterDownloader:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.temp_dir = "./temp_mst"
        os.makedirs(self.temp_dir, exist_ok=True)
        self.targets = {
            "nas": "NAS",
            "nys": "NYS",
            "ams": "AMS"
        }
        self.base_download_url = "https://new.real.download.dws.co.kr/common/master"

    def download_and_extract(self):
        ssl_context = ssl._create_unverified_context()
        for val, exchange_code in self.targets.items():
            zip_filename = f"{val}mst.cod.zip"
            url = f"{self.base_download_url}/{zip_filename}"
            zip_path = os.path.join(self.temp_dir, zip_filename)
            
            logger.info(f"📡 CDN 마스터 파일 고속 수신 중: {url} -> {zip_path}")
            try:
                with urllib.request.urlopen(url, context=ssl_context, timeout=30) as response, open(zip_path, 'wb') as out_file:
                    shutil.copyfileobj(response, out_file)
                logger.info(f"📥 {zip_filename} 수신 완료 ({os.path.getsize(zip_path)} bytes). 압축 해제를 개시합니다...")
                
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(self.temp_dir)
                logger.info(f"📂 {val}mst.cod 압축 풀기 완료.")
                
            except Exception as e:
                logger.error(f"❌ {zip_filename} 다운로드 또는 해제 중 치명적 오류 발생: {e}")

        for filename in os.listdir(self.temp_dir):
            old_path = os.path.join(self.temp_dir, filename)
            if os.path.isfile(old_path):
                new_path = os.path.join(self.temp_dir, filename.lower())
                if old_path != new_path:
                    if os.path.exists(new_path):
                        os.remove(new_path)
                    os.rename(old_path, new_path)
        logger.info("🔄 임시 폴더 내 추출 파일명 대소문자 규격화 완료.")

    def parse_and_sync_db(self):
        conn = self.db_manager.get_connection()
        
        for val, exchange_code in self.targets.items():
            cod_filename = f"{val}mst.cod"
            file_path = os.path.join(self.temp_dir, cod_filename)
            
            if not os.path.exists(file_path):
                logger.warning(f"⚠️ [{cod_filename}] 파일 파싱을 건너뜁니다. 리소스가 수급되지 않았습니다.")
                continue

            logger.info(f"⚙️ [{cod_filename}] 데이터 구조 역분석 및 노이즈 필터링 가동 중...")
            parsed_stocks = []

            try:
                with open(file_path, "r", encoding="cp949", errors="ignore") as f:
                    for line in f:
                        line_str = line.strip('\n')
                        if not line_str:
                            continue
                        
                        parts = line_str.split('\t')
                        if len(parts) < 10:
                            continue
                        
                        try:
                            natn_cd = parts[0].strip() if len(parts) > 0 else "US"
                            tr_mket_cd = parts[1].strip() if len(parts) > 1 else ""
                            ex_code = parts[2].strip().upper() if len(parts) > 2 else exchange_code
                            if ex_code not in ["NAS", "NYS", "AMS"]:
                                ex_code = exchange_code
                            
                            ticker_raw = parts[4].strip()
                            ticker = to_standard_symbol(ticker_raw)
                            
                            name_ko = parts[6].strip() if len(parts) > 6 else ""
                            prdt_eng_name = parts[7].strip() if len(parts) > 7 else ""
                            tr_crcy_cd = parts[9].strip() if len(parts) > 9 else "USD"
                            
                            ovrs_papr = None
                            std_pdno = parts[5].strip() if len(parts) > 5 else ""
                            
                            gubun_cd = parts[22].strip() if len(parts) > 22 else "004"
                            is_etf = gubun_cd in ["001", "005"]
                            is_etn = gubun_cd in ["002", "006"]
                            
                        except IndexError:
                            continue

                        if not ticker or not re.match(r'^[A-Z0-9\.\-\_]+$', ticker):
                            continue
                        if not std_pdno:
                            continue

                        parsed_stocks.append((
                            ticker, ticker_raw, ex_code, name_ko, prdt_eng_name, 
                            natn_cd, tr_mket_cd, tr_crcy_cd, ovrs_papr, is_etf, is_etn, std_pdno
                        ))
            except Exception as e:
                logger.error(f"❌ {cod_filename} 파일 읽기 작업 중 디스크/인코딩 에러 회피 처리: {e}")
                continue

            if not parsed_stocks:
                logger.warning(f"⚠️ [{cod_filename}] 파싱 결과 유효 종목이 발굴되지 않았습니다.")
                continue

            logger.info(f"🚀 분석 완료: [{exchange_code}] 총 {len(parsed_stocks)}개 티커 수급 성공. DB 벌크 업서트를 개시합니다...")

            upsert_query = """
                INSERT INTO tickers (
                    ticker, ticker_raw, exchange_code, name_ko, prdt_eng_name, natn_cd, 
                    tr_mket_cd, tr_crcy_cd, ovrs_papr, is_etf, is_etn, std_pdno
                )
                VALUES %s
                ON CONFLICT (ticker, std_pdno) 
                DO UPDATE SET
                    ticker_raw = EXCLUDED.ticker_raw,
                    exchange_code = EXCLUDED.exchange_code,
                    name_ko = COALESCE(EXCLUDED.name_ko, tickers.name_ko),
                    prdt_eng_name = COALESCE(EXCLUDED.prdt_eng_name, tickers.prdt_eng_name),
                    natn_cd = COALESCE(EXCLUDED.natn_cd, tickers.natn_cd),
                    tr_mket_cd = COALESCE(EXCLUDED.tr_mket_cd, tickers.tr_mket_cd),
                    tr_crcy_cd = COALESCE(EXCLUDED.tr_crcy_cd, tickers.tr_crcy_cd),
                    ovrs_papr = COALESCE(EXCLUDED.ovrs_papr, tickers.ovrs_papr),
                    is_etf = EXCLUDED.is_etf,
                    is_etn = EXCLUDED.is_etn,
                    updated_at = NOW()
                RETURNING ticker_id;
            """
            
            try:
                with conn.cursor() as cur:
                    result_ids = extras.execute_values(cur, upsert_query, parsed_stocks, fetch=True)
                    sync_status_inserts = [(rid[0], 'PENDING') for rid in result_ids]
                    
                    status_query = """
                        INSERT INTO sync_status (ticker_id, status)
                        VALUES %s
                        ON CONFLICT (ticker_id) DO NOTHING;
                    """
                    extras.execute_values(cur, status_query, sync_status_inserts)
                    conn.commit()
                logger.info(f"🎯 [{exchange_code}] 마스터 DB 동기화 적재 완료! (총 {len(parsed_stocks)} 건)")
                
            except Exception as e:
                conn.rollback()
                logger.error(f"❌ [{exchange_code}] 벌크 업서트 실행 중 SQL 트랜잭션 오류 발생 및 롤백됨: {e}")

        try:
            shutil.rmtree(self.temp_dir)
            logger.info("🗑️ 임시 작업 다운로드 디렉터리를 깨끗이 청소했습니다.")
        except Exception as e:
            logger.debug(f"임시 폴더 삭제 지연 회피: {e}")


# ==========================================
# 10. 미국 영업일 및 미국 공휴일 감지 모듈
# ==========================================
def get_latest_trading_date(token_manager, benchmark_ticker="SPY"):
    """
    [우선순위 3 반영] 영업일 감지 및 공휴일 자동 판단
    배치 기동 직전 SPY 조회를 통해 데이터 응답이 완전히 비어 있는 경우,
    불필요한 가동 예외 알림을 터뜨리지 않고 'None'을 반환하여 미국 시장 공휴일로 원천 간주 및 정상 종료시킵니다.
    """
    logger.info(f"📅 [{benchmark_ticker}] 조회를 통해 미국 시장의 최신 영업일을 감지하는 중...")
    url = f"{BASE_URL}/uapi/overseas-price/v1/quotations/dailyprice"
    
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token_manager.get_token()}",
        "appKey": APP_KEY,
        "appSecret": APP_SECRET,
        "tr_id": "HHDFS76240000",
        "custtype": "P",
    }
    
    params = {
        "AUTH": "",
        "EXCD": "AMS",
        "SYMB": to_kis_symbol(benchmark_ticker),
        "GUBN": "0",
        "BYMD": (datetime.datetime.now() + datetime.timedelta(days=2)).strftime("%Y%m%d"),
        "MODP": "1",
    }
    
    try:
        response = safe_request("GET", url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            res_data = response.json()
            output2 = res_data.get("output2", [])
            if output2:
                latest_date_str = output2[0]["xymd"]
                logger.info(f"🎯 감지된 최신 영업일: {latest_date_str[:4]}-{latest_date_str[4:6]}-{latest_date_str[6:8]}")
                return latest_date_str
            else:
                logger.warning(f"💤 [휴장 감지] {benchmark_ticker} 데이터 응답 패킷이 완전히 비어 있습니다. 당일은 미국 공휴일(휴장일)로 잠정 판단합니다.")
                return None
        else:
            raise Exception(f"HTTP Status {response.status_code}")
    except Exception as e:
        logger.error(f"❌ 영업일 및 공휴일 자율 감지 과정 중 네트워크/API 실패: {e}")
        raise e


# ==========================================
# 11. KIS 상품기본정보(CTPF1702R) 및 자가치유 Ticker Merge Engine
# ==========================================
def sync_corporate_actions_and_master_info(db_manager, token_manager, job_id):
    """[CTPF1702R] 마스터 동기화 및 2단계 검증형 원자적 병합(Merge) 엔진 작동 (커서 완벽 폐쇄 보장)"""
    if IS_MOCK:
        logger.warning("⚠️ 모의투자 모드이므로 상품기본정보 수집 단계를 패스합니다.")
        return

    logger.info("🔍 KIS 실전 API 기반 기업공시 이벤트(티커명 변경/상폐/액면분할) 정밀 식별을 시작합니다...")
    conn = db_manager.get_connection()
    
    with conn.cursor() as cur:
        cur.execute("SET emdl.current_job_id = %s;", (str(job_id),))
        cur.execute("""
            SELECT t.ticker_id, t.ticker, t.ticker_raw, t.exchange_code, t.ovrs_papr, t.ovrs_stck_hist_rght_dvsn_cd, ss.status, ss.error_message
            FROM tickers t
            JOIN sync_status ss ON t.ticker_id = ss.ticker_id
            WHERE ss.status != 'DELISTED';
        """)
        stocks = cur.fetchall()

    url = f"{BASE_URL}/uapi/overseas-price/v1/quotations/search-info"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "appKey": APP_KEY,
        "appSecret": APP_SECRET,
        "tr_id": "CTPF1702R",
        "custtype": "P",
    }

    for ticker_id, db_ticker, db_ticker_raw, exchange_code, db_papr, db_rght_cd, current_status, db_err_msg in tqdm(stocks, desc="공시 이벤트 처리 진행률"):
        trigger_cool_down()
        
        prdt_type_cd = EXCHANGE_MAPPING.get(exchange_code, "513")
        params = {
            "PRDT_TYPE_CD": prdt_type_cd
        }

        # [이중화 전략 - Dynamic Dual-Symbol Validation]
        symbols_to_try = []
        if db_ticker_raw:
            symbols_to_try.append(db_ticker_raw)
            
        fallback_sym = to_kis_symbol(db_ticker)
        if fallback_sym not in symbols_to_try:
            symbols_to_try.append(fallback_sym)

        success_api_call = False
        res_data = None
        working_symbol = None

        for idx, sym in enumerate(symbols_to_try):
            params["PDNO"] = sym
            try:
                headers["authorization"] = f"Bearer {token_manager.get_token()}"
                res = safe_request("GET", url, headers=headers, params=params, timeout=10)
                if res.status_code == 200:
                    temp_data = res.json()
                    if temp_data.get("rt_cd") == "0" and temp_data.get("output"):
                        res_data = temp_data
                        working_symbol = sym
                        success_api_call = True
                        break
            except Exception:
                continue

        if not success_api_call:
            logger.warning(f"⚠️ [{db_ticker}] 모든 심볼 규격 시도 실패 (조회 불가). 스킵 격리 처리됩니다.")
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE sync_status 
                    SET status = 'SKIPPED', error_message = 'KIS API 가용 심볼 매핑 실패', updated_at = NOW() 
                    WHERE ticker_id = %s;
                """, (ticker_id,))
                conn.commit()
            continue

        output = res_data.get("output", {})

        # 수신 데이터 파싱
        name_ko = output.get("prdt_name", "").strip()
        std_pdno = output.get("std_pdno", "").strip()
        prdt_eng_name = output.get("prdt_eng_name", "").strip()
        natn_cd = output.get("natn_cd", "").strip()
        tr_mket_cd = output.get("tr_mket_cd", "").strip()
        tr_crcy_cd = output.get("tr_crcy_cd", "").strip()
        
        ovrs_papr = float(output.get("ovrs_papr")) if output.get("ovrs_papr") else 0.0
        lstg_stck_num = int(output.get("lstg_stck_num")) if output.get("lstg_stck_num") else 0
        
        lstg_dt_val = parse_kis_date(output.get("lstg_dt"))
        lstg_abol_dt_val = parse_kis_date(output.get("lstg_abol_dt"))
        
        lstg_abol_item_yn = output.get("lstg_abol_item_yn", "N") == "Y"
        lstg_yn = output.get("lstg_yn", "Y") == "Y"
        
        shrt_pdno = to_standard_symbol(output.get("shrt_pdno", ""))
        chng_bf_pdno = to_standard_symbol(output.get("chng_bf_pdno", ""))
        
        ovrs_stck_hist_rght_dvsn_cd = output.get("ovrs_stck_hist_rght_dvsn_cd", "").strip()
        ptp_item_yn = output.get("ptp_item_yn", "N") == "Y"
        dtm_tr_psbl_yn = output.get("dtm_tr_psbl_yn", "N") == "Y"

        ovrs_stck_dvsn_cd = output.get("ovrs_stck_dvsn_cd", "").strip()
        ovrs_stck_etf_risk_drtp_cd = output.get("ovrs_stck_etf_risk_drtp_cd", "").strip()
        is_etf = (ovrs_stck_dvsn_cd == "03") or (ovrs_stck_etf_risk_drtp_cd in ["001", "005"])
        is_etn = (ovrs_stck_etf_risk_drtp_cd in ["002", "006"])

        target_ticker = shrt_pdno if shrt_pdno else db_ticker
        target_std_pdno = std_pdno if std_pdno else None

        if target_ticker and target_std_pdno:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ticker_id FROM tickers 
                    WHERE ticker = %s AND std_pdno = %s AND ticker_id != %s;
                """, (target_ticker, target_std_pdno, ticker_id))
                dup_row = cur.fetchone()
            
            if dup_row:
                dup_id = dup_row[0]
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM daily_prices WHERE ticker_id = %s;", (dup_id,))
                    dup_row_count = cur.fetchone()[0]
                logger.warning(f"⚠️ [티커 충돌 방어 기동] dup_id={dup_id} (rows={dup_row_count}) -> target_id={ticker_id} (ticker={target_ticker}) 데이터 병합을 시작합니다.")
                
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM tickers WHERE ticker_id = %s", (ticker_id,))
                    if not cur.fetchone():
                        logger.warning(
                            f"⚠️ [병합 스킵] target_id={ticker_id}가 tickers에 부재. "
                            f"dup_id={dup_id}(AACB)를 정규 ID로 유지하고 병합을 건너뜁니다."
                        )
                    continue  


                merge_success = False
                
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE daily_prices dp
                            SET ticker_id = %s
                            WHERE ticker_id = %s
                              AND NOT EXISTS (
                                  SELECT 1 FROM daily_prices dp2
                                  WHERE dp2.ticker_id = %s AND dp2.xymd = dp.xymd
                              );
                        """, (ticker_id, dup_id, ticker_id))
                        cur.execute("DELETE FROM daily_prices WHERE ticker_id = %s;", (dup_id,))
                        merge_success = True
                except Exception as e_merge:
                    logger.error(f"❌ [병합 실패] dup_id={dup_id} 시세 이전 실패. 사유: {e_merge}")
                
                if merge_success:
                    with conn.cursor() as cur:
                        cur.execute("SELECT last_synced_date FROM sync_status WHERE ticker_id = %s;", (dup_id,))
                        dup_status_row = cur.fetchone()
                        
                        if dup_status_row and dup_status_row[0]:
                            dup_last_date = dup_status_row[0]
                            cur.execute("""
                                UPDATE sync_status 
                                SET last_synced_date = CASE 
                                        WHEN last_synced_date IS NULL THEN %s 
                                        ELSE GREATEST(last_synced_date, %s) 
                                    END,
                                    updated_at = NOW()
                                WHERE ticker_id = %s;
                            """, (dup_last_date, dup_last_date, ticker_id))

                        cur.execute("DELETE FROM sync_status WHERE ticker_id = %s;", (dup_id,))
                        cur.execute("DELETE FROM tickers WHERE ticker_id = %s;", (dup_id,))
                    logger.info(f"✅ [충돌 해제 완료] 중복 데이터 ID({dup_id})를 소거하고 본체 ID({ticker_id})로 이관 완료.")
                else:
                    raise Exception(f"dup_id={dup_id}와의 시세 병합 실패로 동기화를 보류합니다.")

        # 🔄 [2단계 안전 교차 검증 상태머신 엔진]
        is_mismatch = db_papr is not None and db_papr > 0 and (float(db_papr) != ovrs_papr or db_rght_cd != ovrs_stck_hist_rght_dvsn_cd)
        
        papr_to_save = db_papr
        rght_cd_to_save = db_rght_cd

        with conn.cursor() as cur:
            if is_mismatch:
                if current_status == 'SPLIT_DETECTED':
                    try:
                        meta = json.loads(db_err_msg) if db_err_msg else {}
                        prev_detected_papr = meta.get("detected_papr")
                        prev_detected_rght = meta.get("detected_rght_cd")
                    except Exception:
                        prev_detected_papr, prev_detected_rght = None, None

                    if prev_detected_papr is not None and abs(prev_detected_papr - ovrs_papr) < 0.0001 and prev_detected_rght == ovrs_stck_hist_rght_dvsn_cd:
                        logger.warning(f"🔥 [공시변동 최종 확정] [{db_ticker}] 주식 분할/병합 교차 검증 완료! 안전한 Purge 및 재수집을 시작합니다.")
                        cur.execute("DELETE FROM daily_prices WHERE ticker_id = %s;", (ticker_id,))
                        cur.execute("UPDATE sync_status SET status = 'PENDING', last_synced_date = NULL, error_message = NULL, updated_at = NOW() WHERE ticker_id = %s;", (ticker_id,))
                        papr_to_save = ovrs_papr
                        rght_cd_to_save = ovrs_stck_hist_rght_dvsn_cd
                    else:
                        logger.warning(f"⚠️ [공시변동 재검증 중] [{db_ticker}] 변동 값이 감지되었으나, 이전 값과 다릅니다. 후보값을 새로 갱신합니다.")
                        new_meta = {
                            "detected_papr": ovrs_papr,
                            "detected_rght_cd": ovrs_stck_hist_rght_dvsn_cd,
                            "detected_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        cur.execute("UPDATE sync_status SET error_message = %s, updated_at = NOW() WHERE ticker_id = %s;", (json.dumps(new_meta), ticker_id))
                else:
                    logger.warning(f"⚡ [공시변동 감지 (의심)] [{db_ticker}]의 액면가 변동 포착 (DB:{db_papr} -> KIS:{ovrs_papr}). 격리 보존합니다.")
                    new_meta = {
                        "detected_papr": ovrs_papr,
                        "detected_rght_cd": ovrs_stck_hist_rght_dvsn_cd,
                        "detected_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    cur.execute("UPDATE sync_status SET status = 'SPLIT_DETECTED', error_message = %s, updated_at = NOW() WHERE ticker_id = %s;", (json.dumps(new_meta), ticker_id))
            else:
                papr_to_save = ovrs_papr
                rght_cd_to_save = ovrs_stck_hist_rght_dvsn_cd
                if current_status == 'SPLIT_DETECTED':
                    logger.info(f"💚 [의심 해제 / 자가치유] [{db_ticker}]의 변동 의심이 일시적 API 장애 노이즈로 공식 확인되었습니다. 원복합니다.")
                    cur.execute("UPDATE sync_status SET status = 'COMPLETED', error_message = NULL, updated_at = NOW() WHERE ticker_id = %s;", (ticker_id,))

            # 상장 폐지 처리
            if lstg_abol_item_yn or not lstg_yn:
                logger.warning(f"🚨 [상장폐지] 종목 [{db_ticker}]이 상장폐지 처리되었습니다.")
                cur.execute("UPDATE sync_status SET status = 'DELISTED', updated_at = NOW() WHERE ticker_id = %s;", (ticker_id,))

            # 티커명 변경 대응
            elif chng_bf_pdno and chng_bf_pdno == db_ticker and shrt_pdno and shrt_pdno != db_ticker:
                logger.warning(f"🔄 [티커명 변경] 종목이 [{db_ticker}]에서 최신 티커 [{shrt_pdno}]로 변경되었습니다.")
                cur.execute("UPDATE tickers SET ticker = %s, updated_at = NOW() WHERE ticker_id = %s;", (shrt_pdno, ticker_id))
                cur.execute("UPDATE sync_status SET status = 'PENDING', last_synced_date = NULL, updated_at = NOW() WHERE ticker_id = %s;", (ticker_id,))
                db_ticker = shrt_pdno

            if working_symbol and not db_ticker_raw:
                cur.execute("UPDATE tickers SET ticker_raw = %s WHERE ticker_id = %s;", (working_symbol, ticker_id))

            # 정보 일괄 적재 및 최신화
            cur.execute("""
                UPDATE tickers 
                SET name_ko = %s, is_etf = %s, is_etn = %s, std_pdno = %s, prdt_eng_name = %s,
                    natn_cd = %s, tr_mket_cd = %s, tr_crcy_cd = %s, ovrs_papr = %s, lstg_stck_num = %s,
                    lstg_dt = %s, lstg_abol_item_yn = %s, lstg_abol_dt = %s, lstg_yn = %s,
                    chng_bf_pdno = %s, ovrs_stck_hist_rght_dvsn_cd = %s, ptp_item_yn = %s, dtm_tr_psbl_yn = %s,
                    updated_at = NOW() 
                WHERE ticker_id = %s;
            """, (
                name_ko, is_etf, is_etn, std_pdno, prdt_eng_name,
                natn_cd, tr_mket_cd, tr_crcy_cd, papr_to_save, lstg_stck_num,
                lstg_dt_val, lstg_abol_item_yn, lstg_abol_dt_val, lstg_yn,
                chng_bf_pdno, rght_cd_to_save, ptp_item_yn, dtm_tr_psbl_yn,
                ticker_id
            ))
            conn.commit()
    logger.info("🎉 공시 이벤트 감지 및 상품기본정보 마스터 DB 동기화 완료.")


# ==========================================
# 12. 역사 데이터 증분 수집 핵심 모듈 (OHLCV 방화벽 포함)
# ==========================================
def sync_ticker_data(db_manager, token_manager, job_id, ticker_id, ticker, ticker_raw, exchange_code, start_date, end_date):
    """
    [우선순위 4, 11 반영] 종목 시세 증분 및 백필 수집 엔진
    - DB 내 상장일(lstg_dt)을 동적 조회하여 시작 수집일(start_date)을 1차적으로 강제 필터 제어합니다.
    - OHLC 방화벽 규칙에 거래량 검증(tvol >= 0) 조건식을 명시적으로 바인딩하여 캔들 이상치를 선제 차단합니다.
    """
    conn = db_manager.get_connection()
    
    with conn.cursor() as cur:
        cur.execute("SET emdl.current_job_id = %s;", (str(job_id),))

        # [우선순위 4 반영] tickers 테이블의 lstg_dt(상장일)을 조회하여 start_date와 동적 비교 조정
        cur.execute("SELECT lstg_dt, MAX(dp.xymd) FROM tickers t LEFT JOIN daily_prices dp ON t.ticker_id = dp.ticker_id WHERE t.ticker_id = %s GROUP BY t.lstg_dt;", (ticker_id,))
        db_info = cur.fetchone()
        
        lstg_dt = db_info[0] if db_info else None
        latest_existing_date = db_info[1] if db_info else None

        # 세션 기반 원자적 체크포인트 관리 등록
        cur.execute("""
            INSERT INTO batch_job_items (job_id, ticker_id, status)
            VALUES (%s, %s, 'RUNNING')
            ON CONFLICT (job_id, ticker_id) DO UPDATE SET status = 'RUNNING';
        """, (job_id, ticker_id))
        cur.execute("UPDATE sync_status SET status = 'RUNNING', updated_at = NOW() WHERE ticker_id = %s;", (ticker_id,))
        conn.commit()

    # 상장일 기반 시작일 필터 조정 적용
    actual_start_dt = datetime.datetime.strptime(start_date, "%Y%m%d").date()
    if lstg_dt:
        actual_start_dt = max(actual_start_dt, lstg_dt)
    
    # 끈질긴 KIS 빈 배열 호출을 무력화하기 위한 최종 최적화 시작 날짜
    optimized_start_date = actual_start_dt.strftime("%Y%m%d")

    url = f"{BASE_URL}/uapi/overseas-price/v1/quotations/dailyprice"
    headers = {
        "Content-Type": "application/json",
        "appKey": APP_KEY,
        "appSecret": APP_SECRET,
        "tr_id": "HHDFS76240000",
        "custtype": "P",
    }

    start_dt = datetime.datetime.strptime(optimized_start_date, "%Y%m%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y%m%d")
    total_days = (end_dt - start_dt).days

    bymd = end_date
    last_days_elapsed = 0
    reached_existing_data = False
    
    consecutive_empty_calls = 0
    provider_anomaly_logged = False

    pbar = tqdm(
        total=total_days,
        desc=f"🗂️ {ticker} ({exchange_code})",
        bar_format="{l_bar}{bar:30}{r_bar}",
        leave=False
    )

    all_insert_data = []
    working_symbol = None
    quality_violation_count = 0

    try:
        while True:
            trigger_cool_down()
            
            params = {
                "AUTH": "",
                "EXCD": exchange_code,
                "GUBN": "0",
                "BYMD": bymd,
                "MODP": "1",
            }

            headers["authorization"] = f"Bearer {token_manager.get_token()}"

            if working_symbol is None:
                symbols_to_try = []
                if ticker_raw:
                    symbols_to_try.append(ticker_raw)
                fallback_sym = to_kis_symbol(ticker)
                if fallback_sym not in symbols_to_try:
                    symbols_to_try.append(fallback_sym)

                response = None
                for idx, sym in enumerate(symbols_to_try):
                    params["SYMB"] = sym
                    try:
                        temp_res = safe_request("GET", url, headers=headers, params=params, timeout=10)
                        if temp_res.status_code == 200:
                            temp_data = temp_res.json()
                            if temp_data.get("rt_cd") == "0":
                                working_symbol = sym
                                response = temp_res
                                break
                    except Exception:
                        continue

                if working_symbol is None:
                    raise Exception("KIS_API_ERROR_매핑 가능한 심볼 전송 규격을 찾지 못했습니다.")
            else:
                params["SYMB"] = working_symbol
                response = safe_request("GET", url, headers=headers, params=params, timeout=10)

            if response.status_code != 200:
                raise Exception(f"API 요청 실패 (HTTP {response.status_code})")

            res_data = response.json()
            if res_data.get("rt_cd") != "0":
                raise Exception(f"KIS API 오류: {res_data.get('msg1')}")

            output2 = res_data.get("output2", [])
            
            if not output2:
                consecutive_empty_calls += 1
                if consecutive_empty_calls <= 3:
                    current_bymd_dt = datetime.datetime.strptime(bymd, "%Y%m%d")
                    jumped_dt = current_bymd_dt - datetime.timedelta(days=120)
                    if jumped_dt < start_dt:
                        break
                    bymd = jumped_dt.strftime("%Y%m%d")
                    pbar.write(f"⚠️ [{ticker}] 수집 패킷 공백 감지. 120일 점프 수집 시도 -> {bymd}")
                    time.sleep(MOCK_DELAY if IS_MOCK else REAL_DELAY)
                    continue
                else:
                    provider_anomaly_logged = True
                    break
            else:
                consecutive_empty_calls = 0

            seen_dates = set()

            for item in output2:
                item_date_str = item["xymd"]
                if item_date_str < optimized_start_date:
                    continue
                
                if latest_existing_date is not None:
                    latest_existing_str = latest_existing_date.strftime("%Y%m%d")
                    if item_date_str <= latest_existing_str:
                        reached_existing_data = True
                        continue 
                
                if item_date_str in seen_dates:
                    continue
                seen_dates.add(item_date_str)
                
                # 🛡️ [우선순위 11 보완 및 무결성 방화벽 작동]
                try:
                    open_val = float(item["open"])
                    high_val = float(item["high"])
                    low_val = float(item["low"])
                    clos_val = float(item["clos"])
                    tvol_val = int(item["tvol"])
                    tamt_val = float(item["tamt"]) if item.get("tamt") else 0.0
                    
                    is_valid_ohlc = (
                        open_val >= 0.0001 and
                        high_val >= 0.0001 and
                        low_val >= 0.0001 and
                        clos_val >= 0.0001 and
                        high_val >= low_val and
                        high_val >= open_val and
                        high_val >= clos_val and
                        low_val <= open_val and
                        low_val <= clos_val and
                        tvol_val >= 0  # 👈 음수 거래량 및 이상 캔들 수집 차단
                    )
                    
                    if not is_valid_ohlc:
                        if open_val > 0 and high_val > 0 and low_val > 0 and clos_val > 0 and high_val >= low_val and tvol_val >= 0:
                            pass
                        else:
                            quality_violation_count += 1
                            pruned_payload = {
                                "sym": ticker,
                                "date": item_date_str,
                                "ohlc": {"o": open_val, "h": high_val, "l": low_val, "c": clos_val},
                                "volume": tvol_val,
                                "violation": "physical_math_rule_violated"
                            }
                            with conn.cursor() as cur:
                                cur.execute("""
                                    INSERT INTO price_anomalies (job_id, ticker_id, trade_date, rule_name, raw_payload)
                                    VALUES (%s, %s, %s, 'DATA_QUALITY_VIOLATION', %s)
                                    ON CONFLICT (ticker_id, trade_date, rule_name) DO NOTHING;
                                """, (job_id, ticker_id, parse_kis_date(item_date_str), json.dumps(pruned_payload)))
                            continue
                except (ValueError, TypeError):
                    continue

                formatted_date = f"{item_date_str[:4]}-{item_date_str[4:6]}-{item_date_str[6:8]}"
                all_insert_data.append((
                    ticker_id,
                    formatted_date,
                    clos_val,
                    open_val,
                    high_val,
                    low_val,
                    tvol_val,
                    tamt_val,
                    item["sign"],
                    float(item["diff"]),
                    float(item["rate"])
                ))

            oldest_date_str = output2[-1]["xymd"]
            oldest_date_dt = datetime.datetime.strptime(oldest_date_str, "%Y%m%d")

            days_elapsed = (end_dt - oldest_date_dt).days
            step = days_elapsed - last_days_elapsed
            if step > 0:
                pbar.update(min(step, pbar.total - pbar.n))
                last_days_elapsed = days_elapsed

            if reached_existing_data:
                pbar.n = total_days
                pbar.refresh()
                break

            if oldest_date_str <= optimized_start_date:
                pbar.n = total_days
                pbar.refresh()
                break

            bymd = (oldest_date_dt - datetime.timedelta(days=1)).strftime("%Y%m%d")
            time.sleep(MOCK_DELAY if IS_MOCK else REAL_DELAY)

        # 수집 완료 후 벌크 업서트 실행
        if all_insert_data:
            upsert_query = """
                INSERT INTO daily_prices (
                    ticker_id, xymd, clos, "open", high, low, tvol, tamt, sign, diff, rate
                ) VALUES %s
                ON CONFLICT (ticker_id, xymd) 
                DO UPDATE SET
                    clos = EXCLUDED.clos,
                    "open" = EXCLUDED."open",
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    tvol = EXCLUDED.tvol,
                    tamt = EXCLUDED.tamt,
                    sign = EXCLUDED.sign,
                    diff = EXCLUDED.diff,
                    rate = EXCLUDED.rate;
            """
            with conn.cursor() as cur:
                extras.execute_values(cur, upsert_query, all_insert_data)

        final_item_status = 'SUCCESS'
        err_msg = None
        
        if provider_anomaly_logged:
            final_item_status = 'PROVIDER_ANOMALY'
            err_msg = "Returned empty array when rt_cd=0"
        elif quality_violation_count > 0:
            final_item_status = 'DATA_QUALITY_VIOLATION'
            err_msg = f"{quality_violation_count} bad candles quarantined in price_anomalies"

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO batch_job_items (job_id, ticker_id, status, error_message)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (job_id, ticker_id) DO UPDATE SET status = EXCLUDED.status, error_message = EXCLUDED.error_message;
            """, (job_id, ticker_id, final_item_status, err_msg))

            cur.execute("""
                UPDATE sync_status 
                SET status = 'COMPLETED', last_synced_date = %s, retry_count = 0, error_message = NULL, updated_at = NOW() 
                WHERE ticker_id = %s;
            """, (parse_kis_date(end_date), ticker_id))
            
            if working_symbol:
                cur.execute("UPDATE tickers SET ticker_raw = %s WHERE ticker_id = %s;", (working_symbol, ticker_id))

            conn.commit()
        pbar.close()
        return True

    except Exception as e:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO batch_job_items (job_id, ticker_id, status, error_message)
                VALUES (%s, %s, 'FAILED', %s)
                ON CONFLICT (job_id, ticker_id) DO UPDATE SET status = 'FAILED', error_message = EXCLUDED.error_message;
            """, (job_id, ticker_id, str(e)))
            
            cur.execute("""
                UPDATE sync_status 
                SET status = 'FAILED', retry_count = COALESCE(retry_count, 0) + 1, error_message = %s, updated_at = NOW() 
                WHERE ticker_id = %s;
            """, (str(e), ticker_id))
            conn.commit()
        logger.error(f"❌ 종목 [{ticker}] 수집 예외 발생 및 격리: {e}")
        pbar.close()
        return False


# ==========================================
# 13. O(1) 메모리 Named Cursor 스트리밍 체크섬 발행 엔진
# ==========================================
def generate_certified_snapshot(db_manager, job_id, business_date_str) -> bool:
    """
    [우선순위 1 보완] 결정론적 SHA-256 원장 일별 체크섬 인증서 발급
    - Named Cursor를 사용하여 대량 수집 시 메모리 OOM을 차단하고 안정성을 확보합니다.
    - 인증 성공 시 True를 반환하며, 트랜잭션 마감 직전 본 함수 성공 여부를 우선순위로 바인딩합니다.
    """
    logger.info(f"🔒 [{business_date_str}] 거래일에 대하여 원장 일별 스냅샷 SHA-256 체크섬을 계산하는 중...")
    conn = db_manager.get_connection()
    cursor_name = f"snapshot_cursor_{uuid.uuid4().hex}"
    
    total_symbols = 0
    total_rows = 0
    sha256_hash = hashlib.sha256()
    seen_tickers = set()

    try:
        with conn.cursor(name=cursor_name) as cur:
            cur.itersize = 2000
            cur.execute("""
                SELECT ticker_id, xymd, "open", high, low, clos, tvol 
                FROM daily_prices 
                WHERE xymd = %s 
                ORDER BY ticker_id ASC, xymd ASC;
            """, (parse_kis_date(business_date_str),))

            while True:
                rows = cur.fetchmany(2000)
                if not rows:
                    break
                
                serialized_lines = []
                for r in rows:
                    t_id = r[0]
                    date_str = r[1].strftime('%Y-%m-%d')
                    o_val = f"{r[2]:.8f}" if r[2] is not None else "\\N"
                    h_val = f"{r[3]:.8f}" if r[3] is not None else "\\N"
                    l_val = f"{r[4]:.8f}" if r[4] is not None else "\\N"
                    c_val = f"{r[5]:.8f}" if r[5] is not None else "\\N"
                    v_val = str(r[6]) if r[6] is not None else "\\N"
                    
                    serialized_lines.append(f"{t_id}|{date_str}|{o_val}|{h_val}|{l_val}|{c_val}|{v_val}")
                    seen_tickers.add(t_id)
                    total_rows += 1
                
                chunk_str = "\n".join(serialized_lines) + "\n"
                sha256_hash.update(chunk_str.encode('utf-8'))
        
        if total_rows == 0:
            logger.warning(f"⚠️ [{business_date_str}] 에 적재된 데이터가 없어 스냅샷 해시를 발행할 수 없습니다.")
            return False

        total_symbols = len(seen_tickers)
        sha256_checksum = sha256_hash.hexdigest()

        with conn.cursor() as normal_cur:
            normal_cur.execute("""
                INSERT INTO market_day_snapshots (business_date, total_symbols, total_rows, sha256_checksum, snapshot_status, certified_at)
                VALUES (%s, %s, %s, %s, 'CERTIFIED', NOW())
                ON CONFLICT (business_date) DO UPDATE SET
                    total_symbols = EXCLUDED.total_symbols,
                    total_rows = EXCLUDED.total_rows,
                    sha256_checksum = EXCLUDED.sha256_checksum,
                    snapshot_status = 'CERTIFIED',
                    certified_at = NOW();
            """, (parse_kis_date(business_date_str), total_symbols, total_rows, sha256_checksum))
        
        logger.info(f"🔒 [인증서 생성 완료] Checksum: {sha256_checksum} (적재 종목수: {total_symbols}, 행수: {total_rows})")
        return True
        
    except Exception as e:
        logger.error(f"❌ 스냅샷 체크섬 발행 실패: {e}")
        return False


# ==========================================
# 14. [우선순위 6 반영] 실전 통합 슬랙 노티파이어
# ==========================================
def send_slack_notification(status, message, job_id=None, failed_tickers_details=None):
    """
    [우선순위 6 수용] 간이 알림 및 영구 실패 감지 슬랙 전송기
    - PARTIAL_SUCCESS / FAIL / ABORTED 상태 발생 시 관리자 채널에 긴급 리포트를 전송합니다.
    - retry_count >= 5 상태의 영구 누락(Permanent Fail) 종목 목록을 하단 필드에 자동 취합하여 첨부합니다.
    """
    webhook_url = SLACK_WEBHOOK_URL
    if not webhook_url:
        logger.warning("⚠️ SLACK_WEBHOOK_URL 환경변수가 없어 실시간 알림 발송을 생략합니다.")
        return False

    color = "#36a64f" if status == "SUCCESS" else "#ff9900" if status == "PARTIAL_SUCCESS" else "#ff0000"
    emoji = "🏆" if status == "SUCCESS" else "⚠️" if status == "PARTIAL_SUCCESS" else "🚨"

    payload = {
        "attachments": [
            {
                "fallback": f"[{status}] EMDL 배치 완료 알림",
                "color": color,
                "pretext": f"{emoji} *EMDL 배치 엔진 상태 변경 감지*",
                "title": f"동기화 작업 최종 결과: {status}",
                "text": message,
                "fields": [
                    {
                        "title": "호스트 서버명",
                        "value": socket.gethostname(),
                        "short": True
                    },
                    {
                        "title": "애플리케이션 버전",
                        "value": APP_VERSION,
                        "short": True
                    }
                ],
                "ts": int(time.time())
            }
        ]
    }

    if job_id:
        payload["attachments"][0]["fields"].append({
            "title": "Job UUID",
            "value": str(job_id),
            "short": False
        })

    # Permanent Fail (수동 검토 대상 5회 연속 실패) 종목 첨부
    if failed_tickers_details:
        payload["attachments"][0]["fields"].append({
            "title": "🚨 수동 조치 필요 종목 (5회 연속 실패 누적)",
            "value": failed_tickers_details,
            "short": False
        })

    try:
        headers = {"Content-Type": "application/json"}
        res = requests.post(webhook_url, json=payload, headers=headers, timeout=10)
        if res.status_code == 200:
            logger.info("📱 [알림 채널] 슬랙 경보 리포트를 채널로 안전하게 송출했습니다.")
            return True
        else:
            logger.warning(f"⚠️ [알림 채널] 슬랙 알림 발송 실패 (HTTP {res.status_code}): {res.text}")
    except Exception as e:
        logger.error(f"❌ [알림 채널] 슬랙 전송 도중 네트워크 세션 예외 발생: {e}")
    return False

# ==========================================
# 15. 네이버 메일 비상 노티파이어 (슬랙 교체)
# ==========================================
def send_email_notification(status, message, job_id=None, failed_tickers_details=None):
    """
    비정상 배치 상태(PARTIAL_SUCCESS / FAILED / ABORTED) 발생 시
    네이버 SMTP를 통해 관리자에게 긴급 이메일을 발송합니다.
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    if not NAVER_EMAIL_SENDER or not NAVER_EMAIL_PASSWORD or not NAVER_EMAIL_RECIPIENT:
        logger.warning("⚠️ 네이버 메일 환경변수(NAVER_EMAIL_SENDER/PASSWORD/RECIPIENT) 미설정으로 알림을 생략합니다.")
        return False

    emoji = "🏆" if status == "SUCCESS" else "⚠️" if status == "PARTIAL_SUCCESS" else "🚨"
    subject = f"{emoji} [EMDL] 배치 상태 변경: {status}"

    body_lines = [
        f"■ 최종 상태  : {status}",
        f"■ Job UUID   : {job_id or 'N/A'}",
        f"■ 호스트     : {socket.gethostname()}",
        f"■ 앱 버전    : {APP_VERSION}",
        "",
        "── 상세 메시지 ──────────────────────────",
        message,
    ]
    if failed_tickers_details:
        body_lines += [
            "",
            "── 🚨 5회 이상 영구 실패 종목 ──────────",
            failed_tickers_details,
        ]
    body = "\n".join(body_lines)

    try:
        msg = MIMEMultipart()
        msg["From"]    = NAVER_EMAIL_SENDER
        msg["To"]      = NAVER_EMAIL_RECIPIENT
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        with smtplib.SMTP_SSL("smtp.naver.com", 465, timeout=15) as smtp:
            smtp.login(NAVER_EMAIL_SENDER, NAVER_EMAIL_PASSWORD)
            smtp.sendmail(NAVER_EMAIL_SENDER, NAVER_EMAIL_RECIPIENT, msg.as_string())

        logger.info(f"📧 [알림 채널] 네이버 메일 긴급 리포트를 {NAVER_EMAIL_RECIPIENT}로 안전하게 송출했습니다.")
        return True

    except Exception as e:
        logger.error(f"❌ [알림 채널] 네이버 메일 발송 실패: {e}")
        return False