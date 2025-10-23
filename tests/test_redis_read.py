from postgres_version.modules.db_redis.connect import connect_to_redis
from postgres_version.modules.db_redis.get import get_hwm_from_redis
from postgres_version.modules.db_redis.set import set_hwm_from_redis
from dotenv import load_dotenv
import os

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_DB = os.getenv("REDIS_DB")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

HWM_KEY = os.getenv("HWM_KEY")

# --- 실행 부분 ---
if __name__ == "__main__":
    # 1. Redis 연결
    r = connect_to_redis(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB)

    if r:
        # 2. HWM 값 조회 (이전 수집의 최종 시점 확인)
        current_hwm = get_hwm_from_redis(r, HWM_KEY)

        # 3. 새로운 HWM 값 설정 예시 (파이프라인 성공 후 실행)
        # 이 값은 실제 파이프라인에서 수집한 데이터 중 가장 최신 시각이 됩니다.
        new_time = "2025-10-23 13:48:00.000000"
        set_hwm_from_redis(r, HWM_KEY, new_time)

        # 4. 갱신된 값 확인
        updated_hwm = get_hwm_from_redis(r, HWM_KEY)