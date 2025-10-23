def set_hwm_from_redis(redis_conn, key, new_hwm_value):
    """Redis에 새로운 HWM 값을 설정(갱신)합니다."""
    # SET 명령어를 사용하여 값을 갱신합니다.
    redis_conn.set(key, new_hwm_value)
    print(f"[{key}] HWM 값이 성공적으로 갱신되었습니다: {new_hwm_value}")
