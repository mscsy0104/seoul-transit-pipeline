import os
import time
from functools import wraps

def measure_time(func):
    """함수 실행 시간을 측정하는 데코레이터"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"{func.__name__} completed in {elapsed_time:.2f} seconds")
        return result
    return wrapper


def ensure_dir_exists(filename):
    # with 구문 전에 실행해주어 FileNotFoundError 방지
    dirname = os.path.dirname(filename)
    print(f"dirname: {dirname}")

    if not os.path.exists(dirname):
        os.makedirs(dirname)
        print(f"Created directory: {dirname}")
