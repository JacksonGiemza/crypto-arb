from functools import wraps
import time

_stats = {}

def timer(func):
    @wraps(func)
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

        if "total_units" in _stats[func_name]:
            avg_per_unit = _stats[func_name]["total_ns"] / _stats[func_name]["total_units"]
            _stats[func_name]["avg_ns_per_unit"] = round(avg_per_unit, 2)

    print(_stats)

def add_update_count(func_name, total_units):
    if not total_units or func_name not in _stats:
        return
    
    if "total_units" not in _stats[func_name]:
        _stats[func_name]["total_units"] = total_units

    else:
        _stats[func_name]["total_units"] += total_units
    