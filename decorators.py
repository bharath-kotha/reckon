import time


def print_execution_time(method):
    def wrapper(*args, **kwargs):
        t1 = time.perf_counter()
        method(*args, **kwargs)
        t2 = time.perf_counter()
        print("Time taken:", (t2 - t1) * 1000, "ms")

    return wrapper


def get_execution_time(method):
    """ Returns the execution time of the method in ms

    The method decorated should not return any values
    """

    def wrapper(*args, **kwargs):
        t1 = time.perf_counter()
        method(*args, **kwargs)
        t2 = time.perf_counter()
        return (t2 - t1) * 1000

    return wrapper
