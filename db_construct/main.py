"""
EMDL (Enterprise Market Data Ledger) - Production Daily Orchestrator
매일 자동으로 기동되어 스케줄 검증 후, 어제 하루치 신규 시세를 안전하게 추가하는 메인 원장 실행기입니다.

[하드닝(Hardening) 및 네이버 이메일 노티파이어 연동 완료]
1. SIGTERM/SIGINT 시그널 캐치 가디언 바인딩 완료 (Advisory Lock 세션 강제 close 연동)
2. run_daily_ledger_batch 내 최외각 예외 처리(try-except) 통합으로 즉사(FAILED) 상황 전격 자동 방어 및 이메일 송출
3. pending_items 수집 대상 종목 공백으로 조기 종료 시, RUNNING 고착 없이 SUCCESS 정상 봉인 후 종료 처리
4. SPY 벤치마크 조회를 통한 미국 시장 공휴일 감지 시 알림 채널 스팸 없이 자율 정상 종료 (Exit 0)
5. 비정상 상황(PARTIAL_SUCCESS, FAILED, ABORTED) 발생 시에만 네이버 메일 발송 (성공 알림 노이즈 통제)
6. 메일 비상 리포트에 5회 이상 영구 실패(Permanent Fail) 종목 목록을 동적으로 스캔하여 첨부
7. KeyboardInterrupt 중복 데드 코드 제거 및 깔끔한 시그널 가디언 단일화
"""

import sys
import uuid
import socket
import datetime
import signal

# 제어 가디언 및 코어 라이브러리 연동
from daily_scheduler import verify_run_window
from daily_data import (
    initialize_environment,
    SYNC_MASTER_ON_START,
    START_DATE,
    APP_VERSION,
    SYSTEMIC_OUTAGE_RATIO_LIMIT,
    SYSTEMIC_OUTAGE_ABS_LIMIT,
    logger,
    
    check_disk_space,
    parse_kis_date,
    get_latest_trading_date,
    
    SafeDatabaseConnection,
    DistributedLockManager,
    KISTokenManager,
    
    init_database,
    reset_running_statuses,
    KISMasterDownloader,
    sync_corporate_actions_and_master_info,
    sync_ticker_data,
    generate_certified_snapshot,
    send_email_notification  # 👈 기존 슬랙에서 네이버 이메일 수신기로 교체 바인딩
)

# 시그널 핸들러 및 트랜잭션 관리를 위한 전역 핸들 확보
global_db_manager = None
global_job_id = None


def fetch_permanent_fails_list(db_manager):
    """[우선순위 6] 5회 이상 동기화 수집에 실패하여 영구 대기 상태에 머무는 종목 리스트 추출"""
    if not db_manager:
        return None
    try:
        conn = db_manager.get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.ticker, ss.error_message 
                FROM sync_status ss
                JOIN tickers t ON ss.ticker_id = t.ticker_id
                WHERE ss.status = 'FAILED' AND COALESCE(ss.retry_count, 0) >= 5
                ORDER BY t.ticker ASC;
            """)
            rows = cur.fetchall()
            if rows:
                return "\n".join([f"• {r[0]}: {str(r[1])[:50]}..." for r in rows])
    except Exception as e:
        logger.error(f"❌ [영구 실패 조회] 5회 이상 실패 종목 스캔 중 DB 오류 발생: {e}")
    return None


def handle_termination_signal(signum, frame):
    """
    [우선순위 2] SIGTERM / SIGINT 긴급 탈출 시그널 가드레일 핸들러
    세션 수준에서 PostgreSQL Advisory Lock이 자동으로 해제될 수 있도록 커넥션 소거(close)를 보장합니다.
    """
    logger.critical(f"🛑 [시그널 가디언] 강제 종료 시그널 {signum} 수신! 배치 세션을 ABORTED로 봉인하고 탈출합니다.")
    
    if global_db_manager and global_job_id:
        try:
            conn = global_db_manager.get_connection()
            with conn.cursor() as cur:
                finished_time = datetime.datetime.now()
                cur.execute("""
                    UPDATE batch_job_runs 
                    SET status = 'ABORTED', finished_at = %s 
                    WHERE job_id = %s;
                """, (finished_time, global_job_id))
                conn.commit()
            logger.info(f"💾 [시그널 가디언] 배치 Job UUID: {global_job_id} 세션 상태를 'ABORTED'로 갱신 완료.")
        except Exception as e:
            logger.error(f"❌ [시그널 가디언] 강제 종료 마킹 트랜잭션 도중 예외 발생: {e}")
        finally:
            try:
                # DB 연결 강제 소멸 -> PostgreSQL 세션 레벨 Advisory Lock 즉시 자동 해제
                global_db_manager.conn.close()
                logger.info("🔓 [시그널 가디언] 데이터베이스 커넥션을 강제 close 처리하여 Advisory Lock 세션을 해제했습니다.")
            except Exception:
                pass

    # [교체 완료] 이메일 비상 알림 송출 (영구 누락 동적 바인딩)
    try:
        failed_list = fetch_permanent_fails_list(global_db_manager)
        send_email_notification(
            status="ABORTED",
            message=f"🚨 [긴급 중단] 배치 엔진 프로세스가 운영체제 및 크론 환경(시그널 {signum})에 의해 강제 ABORTED 봉인되었습니다.",
            job_id=global_job_id,
            failed_tickers_details=failed_list
        )
    except Exception as e_alert:
        logger.error(f"❌ [시그널 가디언] 긴급 경보 메일 발송 실패: {e_alert}")

    sys.exit(1)


def run_daily_ledger_batch():
    """EMDL 일일 증분 동기화 배치 통합 실행기"""
    global global_db_manager, global_job_id

    logger.info("===================================================================")
    logger.info("   Enterprise Market Data Ledger (EMDL) 일별 증분 배치 기동")
    logger.info("===================================================================")

    # 1. 스케줄러 시간/요일 가디언 검증 통과 여부 교차 체크 (화~토 KST 08:40 이후 가동 제한)
    if not verify_run_window():
        logger.info("🛑 [스케줄러 회피] 스케줄 통제 규칙에 의해 오늘 배치는 안전하게 패스되었습니다.")
        sys.exit(0)

    # 2. 모듈 임포트 부작용 제거를 위해 진입부에서 명시적으로 환경설정 로드
    try:
        initialize_environment("setting.env")
    except Exception as e:
        logger.critical(f"💥 환경 설정 동적 초기화 실패: {e}")
        sys.exit(1)

    # 3. 호스트 서버 리소스(디스크 공간 등) 진단
    check_disk_space()

    # 4. 데이터베이스 안전 접속 수립
    db_manager = SafeDatabaseConnection()
    db_manager.connect()
    global_db_manager = db_manager  # 시그널 핸들러 및 최외각 가드레일을 위해 전역 바인딩

    # 5. 스키마 무중단 마이그레이션 및 정정 감사 트리거 정합성 수립
    init_database(db_manager)

    # 6. 분산 동시 실행 통제용 Advisory Lock 획득 (중복 인스턴스 실행 방지)
    lock_manager = DistributedLockManager(db_manager)
    lock_owner_pid, lock_owner_backend_start = lock_manager.acquire_lock()
    logger.info(f"🔓 [Advisory Lock] 세션 소유권 확보 완료. (PID: {lock_owner_pid})")

    # 7. 이전 가동 시 비정상 종료된 세션의 고착 상태(RUNNING)를 PENDING으로 안전 원복
    reset_running_statuses(db_manager)

    # 8. 배치 세션 무결성 검증용 전역 UUID 발행 및 가디언 시그널 매핑
    job_id = str(uuid.uuid4())
    global_job_id = job_id
    logger.info(f"🆔 본 가동 세션 Job UUID: {job_id}")

    # [우선순위 2] 시그널 캐치 가디언 바인딩 (Advisory Lock 해제 연동)
    signal.signal(signal.SIGTERM, handle_termination_signal)
    signal.signal(signal.SIGINT, handle_termination_signal)

    # ==========================================
    # ❌ [치명적 런타임 예외 가드레일 오케스트레이션 수립]
    # ==========================================
    try:
        # 9. KIS 토큰 세션 수립
        token_manager = KISTokenManager()

        # 10. [우선순위 3] 미국 시장 실시간 최종 영업일 감지 및 공휴일 자율 정상 종료
        LATEST_TRADING_DATE = get_latest_trading_date(token_manager, benchmark_ticker="SPY")

        if LATEST_TRADING_DATE is None:
            logger.info("🎉 [미국 공휴일 감지] 오늘 미국 시장은 공식 휴장일입니다. 에러 알림 없이 배치를 조용히 종료합니다.")
            sys.exit(0)

        # 11. batch_job_runs 세션 등록 (컨텍스트 매니저를 활용한 안전한 커서 닫기 보장)
        conn = db_manager.get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO batch_job_runs (job_id, business_date, status, lock_owner_pid, lock_owner_backend_start, host_name, app_version)
                VALUES (%s, %s, 'RUNNING', %s, %s, %s, %s);
            """, (job_id, parse_kis_date(LATEST_TRADING_DATE), lock_owner_pid, lock_owner_backend_start, socket.gethostname(), APP_VERSION))
            conn.commit()

        # 12. 마스터 파일 다운로드 및 종목 풀 대량 동기화 (설정 활성화 시)
        if SYNC_MASTER_ON_START:
            logger.info("📡 [마스터 수급] CDN 서버로부터 전 종목 마스터 실시간 수급 동기화 기동...")
            downloader = KISMasterDownloader(db_manager)
            downloader.download_and_extract()
            downloader.parse_and_sync_db()

        # 13. TimescaleDB 백그라운드 압축 정책 잠시 정지
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT alter_job_schedule(job_id, scheduled => false)
                    FROM timescaledb_information.jobs
                    WHERE proc_name = 'policy_compression';
                """)
                conn.commit()
                logger.info("⚡ [TimescaleDB] 가동 부하 최적화를 위해 압축 스케줄러를 일시적으로 격리했습니다.")
            except Exception as e_compress:
                conn.rollback()
                logger.debug(f"TimescaleDB 압축 정책 정지 우회: {e_compress}")

        # 14. 공시정보(분할/병합/상폐) 동기화 및 2단계 분할 격리 검증 엔진 가동
        sync_corporate_actions_and_master_info(db_manager, token_manager, job_id)

        # 15. 증분 수집 대상 종목 필터링
        logger.info("🔍 증분 데이터 수집 대상 종목 선별 중...")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.ticker_id, t.ticker, t.ticker_raw, t.exchange_code 
                FROM tickers t
                JOIN sync_status ss ON t.ticker_id = ss.ticker_id
                WHERE ss.status = 'PENDING'
                   OR (ss.status = 'COMPLETED' AND ss.last_synced_date < %s)
                   OR (ss.status = 'FAILED' AND COALESCE(ss.retry_count, 0) < 5)
                   OR (ss.status = 'SKIPPED')
                ORDER BY t.ticker ASC;
            """, (parse_kis_date(LATEST_TRADING_DATE),))
            pending_items = cur.fetchall()

        # [치명적 Flaw 2 완벽 교정] 대상 종목이 없어 조기 완료 시 RUNNING 고착화 방지
        if not pending_items:
            logger.info("🎉 [완료] 오늘 적재할 신규 시세 데이터가 없거나 모든 종목이 완전 동기화되었습니다!")
            with conn.cursor() as cur:
                finished_time = datetime.datetime.now()
                cur.execute("SELECT started_at FROM batch_job_runs WHERE job_id = %s;", (job_id,))
                started_row = cur.fetchone()
                duration_sec = int((finished_time - started_row[0]).total_seconds()) if (started_row and started_row[0]) else 0
                
                cur.execute("""
                    UPDATE batch_job_runs 
                    SET status = 'SUCCESS', finished_at = %s, duration_sec = %s 
                    WHERE job_id = %s;
                """, (finished_time, duration_sec, job_id))
                conn.commit()
            logger.info(f"💾 [조기 마감] Job UUID: {job_id} 세션이 성공(SUCCESS) 상태로 안전하게 봉인되었습니다.")
            sys.exit(0)

        logger.info(f"🚀 총 {len(pending_items)}개 종목에 대한 일일 시세 증분 수집을 시작합니다...")

        # 16. 일별 루프 가동 (Probing 최적화 및 무결성 캔들 방화벽 가동)
        success_count = 0
        anomaly_global_count = 0

        for ticker_id, ticker, ticker_raw, exchange_code in pending_items:
            logger.info(f"⏳ 동기화 가동: {ticker} (ID: {ticker_id})")
            
            success = sync_ticker_data(
                db_manager=db_manager,
                token_manager=token_manager,
                job_id=job_id,
                ticker_id=ticker_id,
                ticker=ticker,
                ticker_raw=ticker_raw,
                exchange_code=exchange_code,
                start_date=START_DATE,
                end_date=LATEST_TRADING_DATE
            )
            
            if success:
                success_count += 1
                with conn.cursor() as cur:
                    cur.execute("SELECT status FROM batch_job_items WHERE job_id = %s AND ticker_id = %s;", (job_id, ticker_id))
                    chk_status = cur.fetchone()
                    if chk_status and chk_status[0] == 'PROVIDER_ANOMALY':
                        anomaly_global_count += 1
            else:
                logger.warning(f"⚠️ 경고: {ticker} 수집 중 장애가 발생하여 격리되었습니다. 다음 종목으로 전진합니다.")

        # 17. 공급망 대량 아웃티지 검증
        active_symbols_count = len(pending_items)
        is_systemic_outage = False
        if active_symbols_count > 0:
            anomaly_ratio = anomaly_global_count / active_symbols_count
            if anomaly_ratio >= SYSTEMIC_OUTAGE_RATIO_LIMIT or anomaly_global_count >= SYSTEMIC_OUTAGE_ABS_LIMIT:
                is_systemic_outage = True
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO provider_health_events (business_date, api_endpoint, event_type, affected_exchange, anomaly_count, status)
                        VALUES (%s, 'HHDFS76240000', 'SYSTEMIC_OUTAGE', 'ALL', %s, 'ACTIVE');
                    """, (parse_kis_date(LATEST_TRADING_DATE), anomaly_global_count))
                    conn.commit()
                logger.critical(f"🚨 [CRITICAL OUTAGE] KIS 데이터 공급망 장애 공식 마킹! (빈 응답 종목수: {anomaly_global_count}개)")

        # 18. TimescaleDB 백그라운드 압축 자동 스케줄러 복구
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT alter_job_schedule(job_id, scheduled => true)
                    FROM timescaledb_information.jobs
                    WHERE proc_name = 'policy_compression';
                """)
                conn.commit()
                logger.info("⚡ [TimescaleDB] 자동 압축 스케줄러 가동 복구를 완료했습니다.")
            except Exception as e_compress_r:
                conn.rollback()
                logger.debug(f"TimescaleDB 압축 정책 복원 우회: {e_compress_r}")

        # 19. Triple-Gate Quality Audit (완결성, 품질, 공급망 검증 게이트)
        with conn.cursor() as cur:
            # Gate 1: 완결성 감사 (Completeness Audit)
            cur.execute("""
                SELECT COUNT(DISTINCT ticker_id) 
                FROM daily_prices 
                WHERE xymd = %s;
            """, (parse_kis_date(LATEST_TRADING_DATE),))
            collected_symbols = cur.fetchone()[0]
            
            expected_symbols = active_symbols_count
            completeness_passed = False
            completion_rate = 100.0
            if expected_symbols > 0:
                completion_rate = (collected_symbols / expected_symbols) * 100
                missing_symbols_count = expected_symbols - collected_symbols
                if completion_rate >= 99.95 and missing_symbols_count <= 3:
                    completeness_passed = True
            else:
                completeness_passed = True

            # Gate 2: 품질 검증 감사
            cur.execute("""
                SELECT COUNT(*) 
                FROM price_anomalies 
                WHERE trade_date = %s AND job_id = %s;
            """, (parse_kis_date(LATEST_TRADING_DATE), job_id))
            anomalies_count = cur.fetchone()[0]
            quality_passed = (anomalies_count == 0)

            # Gate 3: 공급망 건전성 감사
            provider_passed = (not is_systemic_outage)

            # 20. [우선순위 1] 배치 마감 트랜잭션의 엄격한 선후관계 통제
            snapshot_success = False
            if completeness_passed and quality_passed and provider_passed:
                snapshot_success = generate_certified_snapshot(db_manager, job_id, LATEST_TRADING_DATE)

            if completeness_passed and quality_passed and provider_passed and snapshot_success:
                final_job_status = 'SUCCESS'
                logger.info("🏆 [TRIPLE GATE & CHECKSUM PASS] 마켓 데이터 원장 무결성 및 암호화 서명 완벽 검증. SUCCESS 마감 처리합니다.")
            else:
                final_job_status = 'PARTIAL_SUCCESS'
                logger.warning("⚠️ [GATE AUDIT FAILED] 원장 정합성 불합격 요인 혹은 암호화 서명 실패 발생. PARTIAL_SUCCESS 처리.")

            # batch_job_runs 세션 최종 봉인 및 소요 시간 기록
            finished_time = datetime.datetime.now()
            cur.execute("SELECT started_at FROM batch_job_runs WHERE job_id = %s;", (job_id,))
            started_at = cur.fetchone()[0]
            duration_sec = int((finished_time - started_at).total_seconds())

            cur.execute("""
                UPDATE batch_job_runs 
                SET status = %s, finished_at = %s, duration_sec = %s 
                WHERE job_id = %s;
            """, (final_job_status, finished_time, duration_sec, job_id))
            conn.commit()

        # 21. [교체 완료] 비상 상황 발생 시에만 네이버 메일 긴급 전송 (성공 알림 노이즈 통제)
        if final_job_status != 'SUCCESS':
            logger.warning(f"🚨 [경보 발송] 배치 비정상 마감({final_job_status})이 감지되어 네이버 메일로 긴급 보고서를 전송합니다.")
            
            permanent_fails_details = fetch_permanent_fails_list(db_manager)
            error_report_message = (
                f"일별 증분 데이터 수집 도중 정합성 검증 예외가 발생했습니다.\n"
                f"• 최종 마감 상태: {final_job_status}\n"
                f"• 수집률: {completion_rate:.2f}% (기대: {expected_symbols}개 / 실제: {collected_symbols}개)\n"
                f"• 당일 감지된 이상 캔들 수: {anomalies_count}건\n"
                f"• 스냅샷 체크섬 발행 여부: {'성공(PASS)' if snapshot_success else '실패(FAIL)'}\n"
                f"• 공급망 아웃티지 여부: {'장애 활성화(ACTIVE)' if is_systemic_outage else '정상(NORMAL)'}\n"
                f"• 동기화 성공 종목 수: {success_count} / 대상 {len(pending_items)}"
            )
            send_email_notification(
                status=final_job_status,
                message=error_report_message,
                job_id=job_id,
                failed_tickers_details=permanent_fails_details
            )
        else:
            logger.info("🎉 [알림 채널 패스] 금일 배치가 완벽하게 성공(SUCCESS)하여 메일 송출을 생략합니다.")

    # ==========================================
    # 💥 [치명적 Flaw 1 해결] 예기치 못한 미처리 런타임 예외 구역 격리
    # ==========================================
    except Exception as e:
        logger.critical(f"💥 [원장 수호 가디언] 배치 실행 중 복구 불가능한 치명적 예외 감지: {e}", exc_info=True)
        
        if global_db_manager and global_job_id:
            try:
                conn = global_db_manager.get_connection()
                conn.rollback()  # 트랜잭션 안전 롤백 후 커밋 세션 갱신
                
                with conn.cursor() as cur:
                    finished_time = datetime.datetime.now()
                    cur.execute("SELECT started_at FROM batch_job_runs WHERE job_id = %s;", (global_job_id,))
                    started_row = cur.fetchone()
                    duration_sec = int((finished_time - started_row[0]).total_seconds()) if (started_row and started_row[0]) else 0
                    
                    cur.execute("""
                        UPDATE batch_job_runs 
                        SET status = 'FAILED', finished_at = %s, duration_sec = %s 
                        WHERE job_id = %s;
                    """, (finished_time, duration_sec, global_job_id))
                    conn.commit()
                logger.info(f"💾 [원장 수호 가디언] 배치 Job UUID: {global_job_id} 세션 상태를 'FAILED' 상태로 긴급 업데이트 완료.")
            except Exception as db_err:
                logger.error(f"❌ [원장 수호 가디언] 예외 봉인 트랜잭션 도중 DB 오류 발생: {db_err}")

        # [교체 완료] 네이버 메일 긴급 오류 경보 송출
        try:
            permanent_fails_details = fetch_permanent_fails_list(global_db_manager)
            failed_alert_message = (
                f"💥 [크래시] 배치 도중 예기치 못한 치명적 시스템 크래시가 발생했습니다!\n\n"
                f"• 에러 메시지: {str(e)}\n"
                f"• 조치 가이드: 서버 로그(logs/emdl_ledger.log)를 확인하고 데이터베이스 락 고착 유무를 모니터링하십시오."
            )
            send_email_notification(
                status="FAILED",
                message=failed_alert_message,
                job_id=global_job_id,
                failed_tickers_details=permanent_fails_details
            )
        except Exception as slack_err:
            logger.error(f"❌ [원장 수호 가디언] 긴급 알림 메일 발송 실패: {slack_err}")

        # DB 커넥션 닫기로 Advisory Lock 세션 강제 반환 유도
        if global_db_manager:
            try:
                global_db_manager.conn.close()
                logger.info("🔓 [원장 수호 가디언] DB 커넥션을 닫아 Advisory Lock을 PG 엔진에 자동 반환했습니다.")
            except Exception:
                pass
        sys.exit(1)

    logger.info("===================================================================")
    logger.info(f"🏆 금일 마켓 데이터 수집 배치가 최종 완료되었습니다. (성공: {success_count} / 대상: {len(pending_items)})")
    logger.info("===================================================================")


if __name__ == "__main__":
    # 시그널 가디언이 SIGINT(Ctrl+C)와 SIGTERM을 모두 대행하여 처리하므로 
    # 최외각 블록의 KeyboardInterrupt 중복 데드 코드는 안전하게 제거하여 심플하게 유지합니다.
    try:
        run_daily_ledger_batch()
    except Exception as e_main:
        logger.critical(f"💥 메인 오케스트레이션 이탈 구역 최외각 예외 발생: {e_main}", exc_info=True)
        sys.exit(1)