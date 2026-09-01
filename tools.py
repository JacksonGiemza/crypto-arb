import time

_stats = {}

def timer(func):
    def wrapper(*args,**kwargs):
        name = func.__name__

        if name not in _stats:
            _stats[name] = {
                "calls": 0,
                "total_ns": 0,
                "min_ns": float("inf"),
                "max_ns": -float("inf"),
            }

        start = time.perf_counter_ns()

        result = func(*args,**kwargs)

        end = time.perf_counter_ns()

        elapsed = end - start

        _stats[name]["calls"] += 1
        _stats[name]["total_ns"] += elapsed

        if elapsed < _stats[name]["min_ns"]:
            _stats[name]["min_ns"] = elapsed

        if elapsed > _stats[name]["max_ns"]:
            _stats[name]["max_ns"] = elapsed

        return result
    return wrapper

def report_stats():
    for func_name in _stats:
        average = _stats[func_name]["total_ns"] / _stats[func_name]["calls"]
        _stats[func_name]["avg_ns"] = round(average, 2)

    print(_stats)