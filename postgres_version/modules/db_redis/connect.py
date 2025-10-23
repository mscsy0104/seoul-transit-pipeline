import redis

def connect_to_redis(host, port, password,db):
    """Redis 서버에 연결하고 연결 객체를 반환합니다."""
    try:
        # decode_responses=True를 설정하면 Redis에서 받은 바이트(bytes)를
        # 자동으로 문자열(string)로 변환해 줍니다.
        r = redis.StrictRedis(host=host, port=port, db=db, password=password, decode_responses=True)
        r.ping()
        print(f"Redis 연결 성공: {host}:{port}")
        return r
    except redis.exceptions.ConnectionError as e:
        print(f"Redis 연결 실패: {e}")
        return None



