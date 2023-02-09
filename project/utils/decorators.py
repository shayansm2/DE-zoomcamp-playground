from typing import Callable
from functools import wraps
import time


def print_logger(function: Callable):
    @wraps(function)
    def wrapper_function(*args, **kwargs):
        print(f"{function.__name__} started")
        result = function(*args, **kwargs)
        print(f"{function.__name__} ended")
        return result

    return wrapper_function


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f'{func.__name__} took {end - start:.6f} seconds to complete')
        return result

    return wrapper
