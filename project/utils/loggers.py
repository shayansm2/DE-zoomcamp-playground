from typing import Callable


def print_logger(function: Callable):
    from functools import wraps

    @wraps(function)
    def wrapper_function(*args, **kwargs):
        print(f"{function.__name__} started")
        result = function(*args, **kwargs)
        print(f"{function.__name__} ended")
        return result

    return wrapper_function
