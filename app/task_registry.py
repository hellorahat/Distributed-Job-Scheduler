from typing import Callable, Dict

TASKS: Dict[str, Callable[[dict], None]] = {}


def register_task(name: str):
    def decorator(fn: Callable[[dict], None]):
        TASKS[name] = fn
        return fn
    return decorator
