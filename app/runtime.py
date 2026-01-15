import threading
import time
from app.scheduler.scheduler import scheduler_tick
from app.worker import worker_loop
from app.model import SchedulerContext


def start_scheduler(ctx: SchedulerContext):
    def run():
        while True:
            scheduler_tick(ctx)
            time.sleep(0.2)

    t = threading.Thread(target=run, daemon=True)
    t.start()


def start_worker(ctx: SchedulerContext, worker_id: str):
    t = threading.Thread(
        target=worker_loop,
        args=(ctx, worker_id),
        daemon=True,
    )
    t.start()
