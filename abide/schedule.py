from datetime import datetime, timedelta
from enum import Enum

import attr
from croniter import croniter


class RunState(Enum):
    RUNNING = 1
    COMPLETE = 2
    FAIL = 3
    FAIL_PERMANENT = 4


@attr.s
class ScheduledJobDefinition:
    schedule: str = attr.ib()  # Cron expression
    retries: int = attr.ib(default=1)
    retry_wait: int = attr.ib(default=0)  # seconds
    late_start_cutoff: int = attr.ib(default=0)  # seconds
    retry_cutoff: int = attr.ib(default=None)  # seconds


@attr.s
class ScheduledJobState:
    last_run: datetime = attr.ib()
    last_retry: int = attr.ib()
    last_finish_time: datetime = attr.ib()
    last_state: RunState = attr.ib()


def get_next_run(now: datetime, job_defn: ScheduledJobDefinition, job_state: ScheduledJobState):
    if job_state is not None:
        if job_state.last_state == RunState.FAIL:
            job_time = job_state.last_run
            next_retry = job_state.last_retry + 1
            if next_retry < job_defn.retries:
                next_run_time = job_state.last_finish_time + timedelta(seconds=job_defn.retry_wait)
                if job_defn.retry_cutoff is None or next_run_time < job_time + timedelta(seconds=job_defn.retry_cutoff):
                    return next_run_time, (job_time, next_retry)

    itr = croniter(job_defn.schedule, start_time=now)
    job_time = itr.get_prev(datetime)
    if job_defn.late_start_cutoff is not None and job_time <= now - timedelta(seconds=job_defn.late_start_cutoff):
        job_time = itr.get_next(datetime)

    return job_time, (job_time, 0)
