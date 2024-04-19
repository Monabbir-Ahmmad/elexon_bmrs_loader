import time
from functools import wraps

def retry(retries, delay, exceptions=(Exception,), exception_condition=None):
    """
    Retries the function in case of specified exceptions and condition.
    
    Args:
        retries (int): Number of retries
        delay (int): Delay between retries in seconds
        exceptions (tuple or Exception): Tuple of exception types to retry for
        condition (callable): A callable that takes the exception instance as argument and returns True or False
    """ 
    def decorator_retry(func):
        @wraps(func)
        def wrapper_retry(*args, **kwargs):
            for _ in range(retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if exception_condition and not exception_condition(e):
                        raise  # If condition is provided and not met, raise the exception
                    print(f"Error: {e}. Retrying in {delay} seconds...")
                    time.sleep(delay)
            else:
                raise RuntimeError(f"Function {func.__name__} failed after {retries} retries")
        return wrapper_retry
    return decorator_retry