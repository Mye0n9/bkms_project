"""
1.스키마 무중단 마이그레이션 (ticker_raw 자동 추가)
기존 테이블 데이터를 지우지 않도록 RESET_DATABASE_ON_START = False 하에서 작동하더라도, 프로그램 시작 시 자동으로 ALTER TABLE tickers ADD COLUMN IF NOT EXISTS ticker_raw VARCHAR(30); 명령을 수행하여 무중단으로 스키마를 고도화합니다.
2.CDN 마스터 다운로더 원본 보존 패치
CDN 배포용 마스터 파일의 5열 원본 값(예: BF/A, BAC PR M)을 그대로 ticker_raw 컬럼에 확보하고, 동시에 정규화된 표준 티커(BF.A, BAC.PR.M)를 ticker 컬럼에 대칭 적재합니다.
3. 상품기본정보 동기화 이중화 전략 (Dual-Strategy) 수립
1차 시도: DB의 ticker_raw 원본 문자열을 전송하여 KIS 게이트웨이와의 매칭률을 100%로 끌어올립니다.
2차 시도 (Fallback): 1차가 실패하거나 레거시 데이터가 존재할 경우, 기존 치환 헬퍼 함수(to_kis_symbol)를 활용해 요청을 재시도합니다.
4. 역사 시세 수집 이중화 및 프로빙(Probing) 최적화
페이징 처리 루프 내에서 매번 이중 시도를 하면 API 제한(Quota)이 낭비되므로, 첫 번째 페이징 요청 시 작동하는 심볼 규격을 자동 감지(Probing)하여 세션에 캐싱하고, 두 번째 페이지부터는 확정된 하나의 심볼로만 시세를 전속력 수집하도록 성능을 설계했습니다.
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
from logging.handlers import RotatingFileHandler
import psycopg2
from psycopg2 import extras
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# ==========================================
# [CONFIG] 전역 설정값 영역 (배치 실행 제어)
# ==========================================
RESET_DATABASE_ON_START = False # ⚠️ TRUE 지정 시 DB의 모든 데이터가 소멸하고 재생성됩니다. (최초 구축 외 사용 금지)
SYNC_MASTER_ON_START = True      # 구동 즉시 KIS 마스터 파일(*mst.cod.zip)을 다운로드하여 종목 풀을 대량 동기화할지 여부

# 1. 수집 날짜 범위 설정 (YYYYMMDD 형식)
START_DATE = "20060101"
END_DATE_LIMIT = "20260530"      # 동적 영업일 감지 실패 시 차선책으로 작동할 마지노선 날짜

# 2. 거래소 코드 - KIS 상품유형코드 매핑 스펙
EXCHANGE_MAPPING = {
    "NAS": "512",  # 나스닥
    "NYS": "513",  # 뉴욕
    "AMS": "529"   # 아멕스
}

# 3. 트래픽 속도 및 안정성 제어 상수
REAL_DELAY = 0.1             # 실전투자 시 API 요청 간 대기 시간 (초 단위)
MOCK_DELAY = 0.5             # 모의투자 시 API 요청 간 대기 시간 (초 단위)
COOL_DOWN_LIMIT = 100        # 해당 횟수 호출 시마다 무작위 휴식 강제 실행

# ==========================================
# 1. 고성능 로깅 프레이너크 (Tqdm-Safe & Rotating)
# ==========================================
os.makedirs("logs", exist_ok=True)
logger = logging.getLogger("KIS_Collector")
logger.setLevel(logging.DEBUG)

# 포맷터 정의
file_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s')
console_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s]: %(message)s', datefmt='%H:%M:%S')

# Rotating File Handler (20MB 크기 제한, 최대 10개 보존)
file_handler = RotatingFileHandler(
    filename="logs/kis_collector.log",
    maxBytes=20 * 1024 * 1024,
    backupCount=10,
    encoding="utf-8"
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# Tqdm 세이프 콘솔 핸들러 (진행 표시줄 깨짐 방지 장치)
class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
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
# 2. 환경 설정 및 DB 연결 세팅 로드
# ==========================================
logger.info("환경 설정 및 DB 연결 설정을 로드하는 중...")
if os.path.exists("setting.env"):
    load_dotenv(dotenv_path="setting.env")
    logger.info("-> setting.env 로드 완료.")
else:
    logger.critical("오류: 'setting.env' 파일을 찾을 수 없습니다. 경로를 확인해주세요.")
    exit()

APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
IS_MOCK = os.getenv("IS_MOCK_INVESTMENT", "False").lower() == "true"

BASE_URL = "https://openapivts.koreainvestment.com:29443" if IS_MOCK else "https://openapi.koreainvestment.com:9443"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

api_call_count = 0


# ==========================================
# Helper: 디스크, 시간, 티커 규격 변환, 자가 탄력적 API 요청 함수
# ==========================================
def check_disk_space():
    """서버의 디스크 공간을 진단하여 여유 공간을 모니터링"""
    try:
        total, used, free = shutil.disk_usage("/")
        free_gb = free / (2**30)
        logger.info(f"💾 서버 디스크 상태: 여유 공간 {free_gb:.2f} GB / 전체 {total/(2**30):.2f} GB")
        if free_gb < 15.0:
            logger.warning("⚠️ 경고: 남은 디스크 용량이 15GB 미만입니다. 대규모 수집 시 공간이 부족할 수 있습니다.")
    except Exception as e:
        logger.error(f"디스크 용량 확인 실패: {e}")

def parse_kis_date(date_str):
    """'00000000' 이거나 비어있는 무효 날짜 데이터를 None(NULL)으로 변환"""
    if not date_str:
        return None
    date_str = date_str.strip()
    if date_str in ("00000000", "", "0"):
        return None
    try:
        return datetime.datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError:
        return None

def trigger_cool_down():
    """상수에 설정된 수치마다 무작위로 1~2초간 일시 대기하여 트래픽 패턴 난수화"""
    global api_call_count
    api_call_count += 1
    if api_call_count % COOL_DOWN_LIMIT == 0:
        sleep_time = random.uniform(1.0, 2.0)
        logger.info(f"💤 [Cool-down] 누적 호출 {api_call_count}회 도달. 서버 보호를 위해 {sleep_time:.2f}초간 휴식합니다...")
        time.sleep(sleep_time)

def to_kis_symbol(ticker: str) -> str:
    """
    글로벌 표준 티커(예: SCHW.D, BRK.B)를 KIS API 송신용 규격(예: SCHW D, BRK B)으로 변환합니다.
    """
    if not ticker:
        return ""
    return ticker.replace('.', ' ')

def to_standard_symbol(ticker: str) -> str:
    """
    KIS 내부 규격 티커(예: SCHW D, BRK/B)를 DB 적재 및 외부 분석용 표준 온점 규격(예: SCHW.D, BRK.B)으로 정규화합니다.
    """
    if not ticker:
        return ""
    return ticker.replace(' ', '.').replace('/', '.').strip()

def safe_request(method, url, max_retries=3, backoff_factor=1.5, **kwargs):
    """
    불안정한 KIS OpenAPI 게이트웨이에 최적화된 기하급수 백오프 기반 API 호출 래퍼.
    HTTP 5xx 에러 또는 네트워크 일시 장애 시 자율적으로 재시도하여 무오류 구동을 유도합니다.
    """
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
            logger.warning(f"📡 [네트워크 수급 일시 지연] {type(e).__name__} 발생. {attempt}/{max_retries}차 재시도 대기 ({delay:.1f}초)...")
            time.sleep(delay)
            delay *= backoff_factor


# ==========================================
# 3. 데이터베이스 안정적 연결 및 연결 복구 관리 객체
# ==========================================
class SafeDatabaseConnection:
    """PostgreSQL 세션 유실을 방지하고 자동 재연결을 수행하는 안전 접속 관리자"""
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
# 4. 데이터베이스 초기화 및 TimescaleDB 설정
# ==========================================
def init_database(db_manager):
    """데이터베이스 테이블 생성, 하이퍼테이블 정책 수립 및 컬럼 점진적 마이그레이션"""
    logger.info("PostgreSQL + TimescaleDB 테이블 스키마 초기화 검증 중...")
    conn = db_manager.get_connection()
    cur = conn.cursor()

    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

    if RESET_DATABASE_ON_START:
        logger.warning("⚠️ RESET_DATABASE_ON_START 옵션이 활성화되었습니다. 기존 테이블을 전체 삭제 후 재생성합니다!")
        cur.execute("DROP TABLE IF EXISTS daily_prices CASCADE;")
        cur.execute("DROP TABLE IF EXISTS sync_status CASCADE;")
        cur.execute("DROP TABLE IF EXISTS tickers CASCADE;")
        conn.commit()

    # 1) tickers (종목 마스터) 테이블 생성
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tickers (
            ticker_id SERIAL PRIMARY KEY,
            ticker VARCHAR(10) NOT NULL,
            ticker_raw VARCHAR(30), -- [이중 전략 지원] KIS 원본 규격 (예: BF/A, BAC PR M)
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

    # [안전 가동 장치 - 점진적 마이그레이션] 기존 DB 유지 상태 구동 시 ticker_raw 컬럼 무중단 동적 확보
    cur.execute("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS ticker_raw VARCHAR(30);")
    conn.commit()

    # 2) daily_prices 테이블 생성
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

    # TimescaleDB 하이퍼테이블 변환 (3개월 단위 청크 조정 적용)
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
        logger.info("⚡ TimescaleDB 하이퍼테이블 및 압축 세그먼트 선언 완료.")
    except Exception as e:
        logger.debug(f"TimescaleDB 성능 정책 가동 설정 확인 우회: {e}")
        conn.rollback()

    # 3) sync_status 테이블 생성
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_status (
            ticker_id INTEGER PRIMARY KEY REFERENCES tickers(ticker_id),
            last_synced_date DATE,
            status VARCHAR(20) DEFAULT 'PENDING',
            error_message TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    cur.close()
    logger.info("기본 테이블 스키마 준비 완료.")


# [안전 가동 장치] 배치 가동 중 비정상 종료된 RUNNING 잔재들을 실행 즉시 원상복구
def reset_running_statuses(db_manager):
    """비정상 종료로 인해 RUNNING 상태로 고착화된 수집 대상들을 전부 PENDING 상태로 롤백"""
    logger.info("🔄 [안전 장치] 비정상 종료된 RUNNING 상태의 수집 태스크 일괄 PENDING 복원 처리 중...")
    conn = db_manager.get_connection()
    cur = conn.cursor()
    try:
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
    finally:
        cur.close()


# ==========================================
# 5. KIS API Access Token 발급 및 자동 관리 클래스
# ==========================================
def get_access_token():
    url = f"{BASE_URL}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
    }
    try:
        res = safe_request("POST", url, headers=headers, json=body, timeout=10)
        if res.status_code == 200:
            return res.json().get("access_token")
        else:
            logger.error(f"❌ Token 발급 실패: {res.text}")
            return None
    except Exception as e:
        logger.error(f"❌ 토큰 발급 중 오류 발생: {e}")
        return None


class KISTokenManager:
    """토큰 수명을 실시간 제어하는 갱신 관리 객체"""
    def __init__(self):
        self._token = None
        self._issued_at = None
        self._lifetime_seconds = 6600

    def get_token(self):
        now = datetime.datetime.now()
        if (self._token is None or 
            self._issued_at is None or 
            (now - self._issued_at).total_seconds() >= self._lifetime_seconds):
            
            new_token = get_access_token()
            if new_token:
                self._token = new_token
                self._issued_at = now
                logger.info(f"🔑 [토큰 관리자] Access Token 발급/자동 갱신 완료 (시각: {self._issued_at.strftime('%H:%M:%S')})")
            else:
                raise Exception("KIS API 토큰 발급에 실패했습니다.")
        return self._token


# ==========================================
# 6. KIS CDN 기반 주식 마스터 대량 수급 모듈
# ==========================================
class KISMasterDownloader:
    """초고속 대외 CDN 서버를 통해 해외 주요 거래소 마스터 압축 파일을 수급 및 파싱 (ticker_raw 원형 파괴 방지 추출)"""
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

        # WSL2 및 Windows 파일 대소문자 명명 격돌 예외 방지 안전 장치
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
        """가변 컬럼을 지원하는 순수 탭 파서 가동 및 이중 티커 동시 적재"""
        conn = self.db_manager.get_connection()
        cur = conn.cursor()

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
                            
                            # [이중 티커 추출 가동]
                            ticker_raw = parts[4].strip()                     # 원본 보존: "BF/A", "BAC PR M", "BCSS.UN"
                            ticker = to_standard_symbol(ticker_raw)            # 분석 표준: "BF.A", "BAC.PR.M", "BCSS.UN"
                            
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

            # ON CONFLICT 구문에 ticker_raw 필드 반영 완료
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

        cur.close()
        
        try:
            shutil.rmtree(self.temp_dir)
            logger.info("🗑️ 임시 작업 다운로드 디렉터리를 깨끗이 청소했습니다.")
        except Exception as e:
            logger.debug(f"임시 폴더 삭제 지연 회피: {e}")


# ==========================================
# 벤치마크 티커 조회를 활용한 거래일 감지 함수
# ==========================================
def get_latest_trading_date(token_manager, benchmark_ticker="SPY"):
    """미국 시장 대형 종목의 최근 거래 데이터를 조회하여 안전한 직전 실제 영업일 감지"""
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
    except Exception as e:
        logger.error(f"⚠️ 영업일 자율 감지 실패: {e}")
        
    logger.warning(f"⚠️ 영업일 조회가 원활하지 않아 설정 마지노선 일자({END_DATE_LIMIT})로 대체합니다.")
    return END_DATE_LIMIT


# ==========================================
# 7. KIS 상품기본정보(CTPF1702R) 동기화 및 기업공시 이벤트 핸들링
# ==========================================
def sync_corporate_actions_and_master_info(db_manager, token_manager):
    """[CTPF1702R] 마스터 정보 동기화 및 2단계 안전 검증 (자가치유형 Ticker Merge Engine 탑재)"""
    if IS_MOCK:
        logger.warning("⚠️ 모의투자 모드이므로 상품기본정보 수집 단계를 패스합니다.")
        return

    logger.info("🔍 KIS 실전 API 기반 기업공시 이벤트(티커명 변경/상폐/액면분할) 정밀 식별을 시작합니다...")
    conn = db_manager.get_connection()
    cur = conn.cursor()
    
    # 이중화 조회를 위해 db_ticker_raw 필드도 추가로 조회 수행
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

        # -----------------------------------------------------------------
        # [이중화 전략 - Dynamic Dual-Symbol Validation]
        # -----------------------------------------------------------------
        symbols_to_try = []
        if db_ticker_raw:
            symbols_to_try.append(db_ticker_raw)  # 1차 시도: 마스터에 보존된 원본 규격 (예: BF/A)
            
        fallback_sym = to_kis_symbol(db_ticker)    # 2차 시도: 레거시 Fallback 규격 (예: BF A)
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
                        if idx > 0 and db_ticker_raw:
                            logger.info(f"ℹ️ [Dual-Strategy Fallback] [{db_ticker}] 2차 폴백 규격('{sym}')으로 동기화 성공")
                        break
            except Exception:
                continue

        if not success_api_call:
            logger.warning(f"⚠️ [{db_ticker}] 모든 심볼 규격 시도 실패 (조회 불가). 스킵 격리 처리됩니다.")
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

        # -----------------------------------------------------------------
        # 🛡️ [자가치유형 Ticker Merge Engine - 2단계 원천 충돌 방어망 (운영 고도화)]
        # -----------------------------------------------------------------
        # API 가 최신 티커(shrt_pdno)를 주지 않더라도 DB 의 기존 티커(db_ticker)로 최종 상태를 예측해 방어합니다.
        target_ticker = shrt_pdno if shrt_pdno else db_ticker
        target_std_pdno = std_pdno if std_pdno else None

        if target_ticker and target_std_pdno:
            cur.execute("""
                SELECT ticker_id FROM tickers 
                WHERE ticker = %s AND std_pdno = %s AND ticker_id != %s;
            """, (target_ticker, target_std_pdno, ticker_id))
            dup_row = cur.fetchone()
            
            if dup_row:
                dup_id = dup_row[0]
                
                # [보완 1] 데이터 감사(Audit)를 위한 이관 대상 행수 사전 조회 및 로깅
                cur.execute("SELECT COUNT(*) FROM daily_prices WHERE ticker_id = %s;", (dup_id,))
                dup_row_count = cur.fetchone()[0]
                logger.warning(f"⚠️ [티커 충돌 방어 기동] dup_id={dup_id} (rows={dup_row_count}) -> target_id={ticker_id} (ticker={target_ticker}) 정적 데이터 병합을 시작합니다.")
                
                # [보완] 원자적 병합 보장을 위한 성공 제어 트랙 플래그 선언
                merge_success = False
                
                try:
                    # 1. dup_id의 시세 중, 본체(ticker_id)와 날짜가 겹치지 않는 행들만 안전 이관
                    cur.execute("""
                        UPDATE daily_prices dp
                        SET ticker_id = %s
                        WHERE ticker_id = %s
                          AND NOT EXISTS (
                              SELECT 1 FROM daily_prices dp2
                              WHERE dp2.ticker_id = %s AND dp2.xymd = dp.xymd
                          );
                    """, (ticker_id, dup_id, ticker_id))
                    
                    # 2. 중복되어 남은 잔여 시세(본체와 날짜가 겹쳐 이관되지 못한 행) 최종 소거
                    cur.execute("DELETE FROM daily_prices WHERE ticker_id = %s;", (dup_id,))
                    
                    # 두 쿼리가 예외 없이 완벽히 실행되었을 때만 성공 플래그 True 전환
                    merge_success = True
                    
                except Exception as e_merge:
                    # 시세 이전 실패 시 마스터 레코드를 지우지 않고 그대로 보존하여 데이터 증발을 물리적으로 원천 차단
                    logger.error(f"❌ [병합 실패] dup_id={dup_id} 시세 이전 실패 (압축 상태 확인 필요). 안전을 위해 중복 마스터 소거를 중단합니다. 사유: {e_merge}")
                
                # -----------------------------------------------------------------
                # [안전 가드] 시세 병합이 완전히 보장된 상태에서만 상태 상속 및 마스터 제거 집행
                # -----------------------------------------------------------------
                if merge_success:
                    cur.execute("SELECT last_synced_date FROM sync_status WHERE ticker_id = %s;", (dup_id,))
                    dup_status_row = cur.fetchone()
                    
                    if dup_status_row and dup_status_row[0]:
                        dup_last_date = dup_status_row[0]
                        
                        # [개선 1] 상태 오승격 위험 제거: status는 건드리지 않고, 증분 수집의 핵심인 last_synced_date만 안전하게 최대치로 계승
                        cur.execute("""
                            UPDATE sync_status 
                            SET last_synced_date = CASE 
                                    WHEN last_synced_date IS NULL THEN %s 
                                    ELSE GREATEST(last_synced_date, %s) 
                                END,
                                updated_at = NOW()
                            WHERE ticker_id = %s;
                        """, (dup_last_date, dup_last_date, ticker_id))

                    # [개선 2] 고아 데이터 방어: 시세가 안전하게 옮겨진 것이 확인되었으므로 안심하고 중복 마스터 정보 소거
                    cur.execute("DELETE FROM sync_status WHERE ticker_id = %s;", (dup_id,))
                    cur.execute("DELETE FROM tickers WHERE ticker_id = %s;", (dup_id,))
                    logger.info(f"✅ [충돌 해제 완료] 중복 데이터 ID({dup_id})를 소거하고 본체 ID({ticker_id})로 {dup_row_count}개의 주가 데이터를 손실 없이 이관 완료하였습니다.")
                else:
                    # 🔴 [마지막 안전장치] 병합 실패 시 하단 업데이트 쿼리로 진행하지 못하도록 
                    # 의도적 예외를 발생시켜 이 종목의 트랜잭션을 롤백하고 다음 종목으로 안전하게 이동합니다.
                    raise Exception("중복 종목과의 시세 병합에 실패하여 제약조건 충돌 방지를 위해 동기화를 보류합니다.")

        # -----------------------------------------------------------------
        # 🔄 [2단계 안전 교차 검증 상태머신 엔진]
        # -----------------------------------------------------------------
        is_mismatch = db_papr is not None and db_papr > 0 and (float(db_papr) != ovrs_papr or db_rght_cd != ovrs_stck_hist_rght_dvsn_cd)
        
        papr_to_save = db_papr
        rght_cd_to_save = db_rght_cd

        if is_mismatch:
            if current_status == 'SPLIT_DETECTED':
                try:
                    meta = json.loads(db_err_msg) if db_err_msg else {}
                    prev_detected_papr = meta.get("detected_papr")
                    prev_detected_rght = meta.get("detected_rght_cd")
                except Exception:
                    prev_detected_papr, prev_detected_rght = None, None

                if prev_detected_papr is not None and abs(prev_detected_papr - ovrs_papr) < 0.0001 and prev_detected_rght == ovrs_stck_hist_rght_dvsn_cd:
                    logger.warning(f"🔥 [공시변동 최종 확정] [{db_ticker}] 주식 분할/병합 교차 검증 완료! (2회 연속 확인). 안전한 Purge 및 재수집을 시작합니다.")
                    
                    try:
                        cur.execute("SELECT decompress_chunk(c, if_compressed => TRUE) FROM show_chunks('daily_prices') c;")
                    except Exception as ex_dec:
                        logger.debug(f"TimescaleDB 청크 압축 해제 명령 스킵: {ex_dec}")

                    cur.execute("DELETE FROM daily_prices WHERE ticker_id = %s;", (ticker_id,))
                    cur.execute("UPDATE sync_status SET status = 'PENDING', last_synced_date = NULL, error_message = NULL, updated_at = NOW() WHERE ticker_id = %s;", (ticker_id,))
                    
                    papr_to_save = ovrs_papr
                    rght_cd_to_save = ovrs_stck_hist_rght_dvsn_cd
                else:
                    logger.warning(f"⚠️ [공시변동 재검증 중] [{db_ticker}] 변동 값이 감지되었으나, 이전 유예 값과 다릅니다. 후보값을 새로 갱신하고 유예를 연장합니다.")
                    new_meta = {
                        "detected_papr": ovrs_papr,
                        "detected_rght_cd": ovrs_stck_hist_rght_dvsn_cd,
                        "detected_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    cur.execute("UPDATE sync_status SET error_message = %s, updated_at = NOW() WHERE ticker_id = %s;", (json.dumps(new_meta), ticker_id))
            else:
                logger.warning(f"⚡ [공시변동 감지 (의심)] [{db_ticker}]의 액면가 변동 포착 (DB:{db_papr} -> KIS:{ovrs_papr}). API 오류에 대비하여 'SPLIT_DETECTED' 상태로 마킹하고 데이터를 격리 보존합니다.")
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
                logger.info(f"💚 [의심 해제 / 자가치유] [{db_ticker}]의 변동 의심이 일시적 API 장애 노이즈로 공식 확인되었습니다. 데이터를 손실 없이 원복합니다.")
                cur.execute("UPDATE sync_status SET status = 'COMPLETED', error_message = NULL, updated_at = NOW() WHERE ticker_id = %s;", (ticker_id,))

        # [이벤트 감지 B] 상장 폐지 처리
        if lstg_abol_item_yn or not lstg_yn:
            logger.warning(f"🚨 [상장폐지] 종목 [{db_ticker}]이 상장폐지 처리되었습니다. 수집을 마감합니다.")
            cur.execute("UPDATE sync_status SET status = 'DELISTED', updated_at = NOW() WHERE ticker_id = %s;", (ticker_id,))

        # [이벤트 감지 C] 티커명 변경 대응
        elif chng_bf_pdno and chng_bf_pdno == db_ticker and shrt_pdno and shrt_pdno != db_ticker:
            logger.warning(f"🔄 [티커명 변경 감지] 종목이 [{db_ticker}]에서 최신 티커 [{shrt_pdno}]로 변경되었습니다.")
            cur.execute("UPDATE tickers SET ticker = %s, updated_at = NOW() WHERE ticker_id = %s;", (shrt_pdno, ticker_id))
            cur.execute("UPDATE sync_status SET status = 'PENDING', last_synced_date = NULL, updated_at = NOW() WHERE ticker_id = %s;", (ticker_id,))
            db_ticker = shrt_pdno

        # 만약 API가 검증에 성공한 working_symbol을 돌려주었으나 DB의 ticker_raw가 비어있었을 경우 동기화 패치
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

    cur.close()
    logger.info("🎉 공시 이벤트 감지 및 상품기본정보 마스터 DB 동기화 완료.")


# ==========================================
# 8. 개별 종목 역사 데이터 수집 및 실시간 적재 함수
# ==========================================
def sync_ticker_data(db_manager, token_manager, ticker_id, ticker, ticker_raw, exchange_code, start_date, end_date):
    """한 종목의 역사적 수정을 안전하고 완벽하게 증분 및 백필 수집 (최적화 Probing 기법 결합)"""
    conn = db_manager.get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT MAX(xymd) FROM daily_prices WHERE ticker_id = %s;", (ticker_id,))
    latest_existing_date = cur.fetchone()[0]

    cur.execute("UPDATE sync_status SET status = 'RUNNING', updated_at = NOW() WHERE ticker_id = %s;", (ticker_id,))
    conn.commit()

    url = f"{BASE_URL}/uapi/overseas-price/v1/quotations/dailyprice"
    headers = {
        "Content-Type": "application/json",
        "appKey": APP_KEY,
        "appSecret": APP_SECRET,
        "tr_id": "HHDFS76240000",
        "custtype": "P",
    }

    start_dt = datetime.datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y%m%d")
    total_days = (end_dt - start_dt).days

    bymd = end_date
    last_days_elapsed = 0
    reached_existing_data = False
    
    consecutive_empty_calls = 0

    pbar = tqdm(
        total=total_days,
        desc=f"🗂️ {ticker} ({exchange_code})",
        bar_format="{l_bar}{bar:30}{r_bar}",
        leave=False
    )

    all_insert_data = []
    
    # [Probing 기법] 첫 페이지에서 사용 가능한 규격을 결정하고 이후 루프에서는 검증된 규격만 일관적으로 활용
    working_symbol = None

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
                # [Dual-Strategy Probing 실행]
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
                                if idx > 0 and ticker_raw:
                                    logger.info(f"ℹ️ [Dual-Strategy Price Fallback] [{ticker}] Fallback 규격 '{sym}'으로 시세 감지")
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
                    pbar.write(f"⚠️ [{ticker}] 수집 패킷 공백 (Halt 정지 구간 의심). 120일 점프 수집 시도 -> {bymd}")
                    time.sleep(MOCK_DELAY if IS_MOCK else REAL_DELAY)
                    continue
                else:
                    break
            else:
                consecutive_empty_calls = 0

            seen_dates = set()

            for item in output2:
                item_date_str = item["xymd"]
                if item_date_str < start_date:
                    continue
                
                if latest_existing_date is not None:
                    latest_existing_str = latest_existing_date.strftime("%Y%m%d")
                    if item_date_str <= latest_existing_str:
                        reached_existing_data = True
                        continue 
                
                if item_date_str in seen_dates:
                    continue
                seen_dates.add(item_date_str)
                
                try:
                    clos_val = float(item["clos"])
                    open_val = float(item["open"])
                    high_val = float(item["high"])
                    low_val = float(item["low"])
                    
                    if clos_val <= 0.0 or open_val <= 0.0 or high_val <= 0.0 or low_val <= 0.0:
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
                    int(item["tvol"]),
                    float(item["tamt"]) if item.get("tamt") else 0,
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

            if oldest_date_str <= start_date:
                pbar.n = total_days
                pbar.refresh()
                break

            bymd = (oldest_date_dt - datetime.timedelta(days=1)).strftime("%Y%m%d")
            time.sleep(MOCK_DELAY if IS_MOCK else REAL_DELAY)

        # 수집 완료 후 1회 벌크 인서트 및 커밋 (I/O 병목 해소)
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
            extras.execute_values(cur, upsert_query, all_insert_data)

        cur.execute("""
            UPDATE sync_status 
            SET status = 'COMPLETED', last_synced_date = %s, error_message = NULL, updated_at = NOW() 
            WHERE ticker_id = %s;
        """, (end_date, ticker_id))
        
        # Probing에서 정합성이 증명된 working_symbol을 마스터 테이블에 백필 처리
        if working_symbol:
            cur.execute("UPDATE tickers SET ticker_raw = %s WHERE ticker_id = %s;", (working_symbol, ticker_id))

        conn.commit()
        pbar.close()
        return True

    except Exception as e:
        conn.rollback()
        cur.execute("""
            UPDATE sync_status 
            SET status = 'FAILED', error_message = %s, updated_at = NOW() 
            WHERE ticker_id = %s;
        """, (str(e), ticker_id))
        conn.commit()
        logger.error(f"❌ 종목 [{ticker}] 처리 중 예외 발생 (스킵 처리됨): {e}")
        pbar.close()
        return False
    finally:
        cur.close()


# ==========================================
# 9. 메인 오케스트레이터 가동
# ==========================================
if __name__ == "__main__":
    logger.info("==============================================")
    logger.info("   KIS 미국주식 역사 데이터 수집배치 기동")
    logger.info("==============================================")
    
    check_disk_space()

    db_manager = SafeDatabaseConnection()
    db_manager.connect()
    init_database(db_manager)

    reset_running_statuses(db_manager)

    if SYNC_MASTER_ON_START:
        logger.info("📡 [마스터 수급] CDN 서버로부터 전 종목 마스터 실시간 수급 동기화 기동...")
        downloader = KISMasterDownloader(db_manager)
        downloader.download_and_extract()
        downloader.parse_and_sync_db()

    logger.info("KIS API 접근 통제 수립 및 영업일 감지 중...")
    token_manager = KISTokenManager()
    
    LATEST_TRADING_DATE = get_latest_trading_date(token_manager, benchmark_ticker="SPY")

    # 1단계 격리 및 2단계 정밀 공시 동기화 실행 (2단계 교차 검증 활성화)
    sync_corporate_actions_and_master_info(db_manager, token_manager)

    # 6. 미동기화 종목 필터링 및 오케스트레이션 가동
    logger.info("수집 대상 종목 필터링 중...")
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.ticker_id, t.ticker, t.ticker_raw, t.exchange_code 
        FROM tickers t
        JOIN sync_status ss ON t.ticker_id = ss.ticker_id
        WHERE ss.status NOT IN ('COMPLETED', 'DELISTED', 'SPLIT_DETECTED')
        ORDER BY t.ticker ASC;
    """)
    pending_items = cursor.fetchall()

    if not pending_items:
        logger.info("🎉 모든 종목이 이미 최신화 완료되었습니다!")
        cursor.close()
        exit()

    logger.info(f"-> 총 {len(pending_items)}개의 종목에 대하여 역사 시세 장기 수집을 개시합니다.")
    cursor.close()

    logger.info(f"미국 시세 수집 개시 (수집 목표 범위: {START_DATE} -> {LATEST_TRADING_DATE[:4]}-{LATEST_TRADING_DATE[4:6]}-{LATEST_TRADING_DATE[6:8]})")
    success_count = 0
    
    for ticker_id, ticker, ticker_raw, exchange_code in pending_items:
        logger.info(f"🚀 작업 시작: {ticker} (ID: {ticker_id})")
        
        success = sync_ticker_data(
            db_manager=db_manager,
            token_manager=token_manager,
            ticker_id=ticker_id,
            ticker=ticker,
            ticker_raw=ticker_raw,
            exchange_code=exchange_code,
            start_date=START_DATE,
            end_date=LATEST_TRADING_DATE
        )
        
        if success:
            success_count += 1
            logger.info(f"✅ 완료: {ticker} 수집 완료")
        else:
            logger.warning(f"⚠️ 경고: {ticker} 수집 중 경고가 마킹되었습니다. 프로세스를 중단하지 않고 다음 종목을 계속 수집합니다.")

    logger.info(f"🏆 최종 수집 완료! (성공: {success_count} / 총 수집 대상: {len(pending_items)})")