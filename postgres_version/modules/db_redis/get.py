def get_hwm_from_redis(redis_conn, key):
    """Redis에서 HWM 값을 조회합니다."""
    # GET 명령어를 사용하여 값을 조회합니다.
    hwm_value = redis_conn.get(key)
    
    if hwm_value:
        print(f"[{key}] 조회 성공. 현재 HWM 값: {hwm_value}")
    else:
        # 키가 존재하지 않을 경우 None을 반환합니다.
        print(f"[{key}] 키가 존재하지 않거나 값이 설정되지 않았습니다.")
        # 이 경우, 증분 수집을 시작할 초기값 (예: '1970-01-01 00:00:00')을 반환하도록 로직을 추가해야 합니다.
        
    return hwm_value