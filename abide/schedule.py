import json
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from enum import Enum

import attr
import yaml
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
    action = attr.ib(default=None)

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
    while True:
        if job_state is not None:
            if job_time <= job_state.last_run:
                job_time = itr.get_next(datetime)
                continue
        if job_defn.late_start_cutoff is not None:
            if job_time < now - timedelta(seconds=job_defn.late_start_cutoff):
                job_time = itr.get_next(datetime)
                continue
        break

    return job_time, (job_time, 0)


def get_file_obj(path_or_buf, mode=None):
    if isinstance(path_or_buf, str):
        return open(path_or_buf, mode)
    elif isinstance(path_or_buf, Path):
        return path_or_buf.open(mode)
    elif isinstance(path_or_buf, StringIO):
        return path_or_buf
    elif hasattr(path_or_buf, 'read') and hasattr(path_or_buf, 'write'):
        return path_or_buf
    else:
        raise ValueError('Unable to create file object from {}'.format(path_or_buf))


def read_job_definitions(path_or_buf):
    with get_file_obj(path_or_buf, 'r') as f:
        job_definitions = yaml.load(f, Loader=yaml.CLoader)
    res = {}
    for name, job_definition in job_definitions.items():
        res[name] = ScheduledJobDefinition(**job_definition)
    return res


class Scheduler:
    def __init__(self, start_time, job_definitions=None):
        self.job_definitions = job_definitions or {}
        self.job_states = {}
        self.time = start_time

    def write_state(self, path_or_buf):
        with get_file_obj(path_or_buf, 'w') as f:
            d = {}
            for name, job_state in self.job_states.items():
                if job_state.last_finish_time is None:
                    last_finish_time = None
                else:
                    last_finish_time = job_state.last_finish_time.isoformat()
                d[name] = {
                    'last_run': job_state.last_run.isoformat(),
                    'last_retry': job_state.last_retry,
                    'last_finish_time': last_finish_time,
                    'last_state': job_state.last_state.name
                }
            json.dump(d, f, indent=2)

    def read_state(self, path_or_buf):
        with get_file_obj(path_or_buf, 'w') as f:
            d = json.load(f)
        job_states = {}
        for name, job_state_dict in d.items():
            last_finish_time_str = job_state_dict['last_finish_time']
            if last_finish_time_str is None:
                last_finish_time = None
            else:
                last_finish_time = datetime.fromisoformat(last_finish_time_str)
            job_states[name] = ScheduledJobState(
                last_run=datetime.fromisoformat(job_state_dict['last_run']),
                last_retry=job_state_dict['last_retry'],
                last_finish_time=last_finish_time,
                last_state=RunState[job_state_dict['last_state']])
        self.job_states = job_states

    def add_job(self, name, job_definition):
        self.job_definitions[name] = job_definition

    def set_time(self, time):
        self.time = time

    def set_running(self, name, run, retry):
        self.job_states[name] = ScheduledJobState(run, retry, None, RunState.RUNNING)

    def set_complete(self, name, state):
        current_job_state = self.job_states[name]
        self.job_states[name] = ScheduledJobState(current_job_state.last_run, current_job_state.last_retry, self.time, state)

    def get_next_run(self):
        res_when = None
        res_job_name = None
        res_job_time = None
        res_retry = None
        for name, job_definition in self.job_definitions.items():
            job_state = self.job_states.get(name)
            when, (job_time, retry) = get_next_run(self.time, job_definition, job_state)
            if res_when is None or when < res_when:
                res_when = when
                res_job_name = name
                res_job_time = job_time
                res_retry = retry
        return res_job_name, res_when, (res_job_time, res_retry)
