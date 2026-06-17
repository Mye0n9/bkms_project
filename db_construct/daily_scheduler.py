"""
EMDL (Enterprise Market Data Ledger) - Daily Batch Scheduler Guardian
한국 시간(KST) 기준 가동 윈도우(오전 8시 40분 이후) 및 요일 제약(일, 월 휴무)을 
엄격하게 사전 검증하는 독립 제어 유틸리티 모듈입니다.

이 모듈은 어떠한 부작용(Side Effect) 없이 안전하게 임포트할 수 있도록 설계되었습니다.
"""

import datetime
import sys
import logging

# EMDL 통합 로거와 연동 (미정의 시 기본 로거 활성화로 유연성 확보)
logger = logging.getLogger("EMDL_Ledger")

# ==========================================
# [CONFIG] 일별 배치 가동 시간 제약 상수
# ==========================================
SAFE_HOUR = 8
SAFE_MINUTE = 40


def get_kst_now() -> datetime.datetime:
    """
    서버의 물리적 운영체제(OS) 시간대와 상관없이 항상 정확한 한국 표준시(KST, UTC+9)를 반환합니다.
    """
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    kst_tz = datetime.timezone(datetime.timedelta(hours=9))
    return utc_now.astimezone(kst_tz)


def verify_run_window() -> bool:
    """
    현재 시간이 미국 주가 데이터를 완벽히 수집할 수 있는 안전 정정 윈도우인지 교차 검증합니다.
    
    [통제 규칙]
    1. 요일 통제: KST 화요일 ~ 토요일만 기동 승인 (일요일, 월요일은 미국 시장 주말 휴장으로 차단)
    2. 시간 통제: KST 오전 08시 40분 이후 기동 승인 (그 이전 시간대 기동 시 원천 데이터 정정 지연으로 차단)
    
    Returns:
        bool: 모든 안전 수집 요건 충족 시 True, 불충족 시 False
    """
    kst_now = get_kst_now()
    weekday = kst_now.weekday()  # 0: 월요일, 1: 화요일, ..., 5: 토요일, 6: 일요일
    weekday_kr = ["월", "화", "수", "목", "금", "토", "일"][weekday]
    
    logger.info(f"🕒 [스케줄러 가디언] 동기화 윈도우 검증 개시 - 현재 KST: {kst_now.strftime('%Y-%m-%d %H:%M:%S')} ({weekday_kr}요일)")

    # 1. 요일 필터 (일요일, 월요일 차단)
    # - 미국 금요일 장 최종 데이터는 한국 시간 '토요일 오전 8시 40분 이후' 수집됩니다.
    # - 미국 주말(토/일) 휴장 여파로 한국 시간 '일요일', '월요일'에는 추가할 데이터가 없습니다.
    if weekday in (0, 6):
        logger.warning(f"🚫 [기동 거부] KST {weekday_kr}요일은 미국 시장 주말 휴장 여파로 배치를 가동하지 않습니다.")
        logger.warning("   └ (일요일, 월요일은 원장 안전을 위해 자율적으로 패스 처리됩니다.)")
        return False

    # 2. 시간 필터 (오전 8시 40분 이전 기동 차단)
    current_time = kst_now.time()
    limit_time = datetime.time(SAFE_HOUR, SAFE_MINUTE, 0)
    
    if current_time < limit_time:
        logger.warning(f"🚫 [기동 거부] 현재 시간({current_time.strftime('%H:%M:%S')})은 일일 동기화 안전 기준시(KST 08:40) 이전입니다.")
        logger.warning("   └ KIS 원천 데이터 최종 정정 작업을 기다리기 위해 배치를 안전하게 자동 중단합니다.")
        return False

    logger.info("✅ [검증 통과] 요일 및 시간 통제 규칙 승인. 일일 증분 데이터 수집을 시작합니다.")
    return True


if __name__ == "__main__":
    # 개별 모듈 단위 가독성 테스트용 디버깅 블록
    log_setup = logging.StreamHandler(sys.stdout)
    log_setup.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s]: %(message)s'))
    logger.addHandler(log_setup)
    logger.setLevel(logging.INFO)
    
    print("\n--- EMDL 스케줄러 가디언 단독 검증 테스트 ---")
    passed = verify_run_window()
    print(f"최종 결과: {'PASS (수집 승인)' if passed else 'FAIL (기동 차단)'}\n")