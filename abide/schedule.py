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
    name: str = attr.ib(default=None)
    retries: int = attr.ib(default=0)
    retry_wait: int = attr.ib(default=0)  # seconds
    late_start_cutoff: int = attr.ib(default=None)  # seconds (None = forever)
    run_all_on_late_start: bool = attr.ib(default=False)
    catchup_on_startup: bool = attr.ib(default=False)
    action = attr.ib(default=None)

    def next_run(self, time):
        itr = croniter(self.schedule, time)
        return itr.get_next(datetime)

    def current_or_next_run(self, time):
        itr = croniter(self.schedule, time)
        itr.get_prev(datetime)
        return itr.get_next(datetime)

    def prev_run(self, time):
        itr = croniter(self.schedule, time)
        return itr.get_prev(datetime)

    def current_or_prev_run(self, time):
        itr = croniter(self.schedule, time)
        itr.get_next(datetime)
        return itr.get_prev(datetime)



@attr.s
class ScheduledJobState:
    last_run: datetime = attr.ib()
    last_retry: int = attr.ib()
    last_finish_time: datetime = attr.ib()
    last_state: RunState = attr.ib()


def enum_lookup(enum_class, label):
    for member in enum_class:
        if label.lower() == member.name.lower():
            return member
    raise KeyError("{} is not a member of {}".format(label, enum_class))


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
        res[name] = ScheduledJobDefinition(name=name, **job_definition)
    return res


class Scheduler:
    def __init__(self, start_time, job_definitions=None):
        self.job_definitions = job_definitions or {}
        self.job_states = {}
        self.time = start_time
        self.next_from_startup = {name: job_definition.current_or_next_run(start_time) for name, job_definition in job_definitions.items()}

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
                last_state=enum_lookup(RunState, job_state_dict['last_state']))
        self.job_states = job_states

    def add_job(self, name, job_definition):
        self.job_definitions[name] = job_definition
        self.next_from_startup[name] = job_definition.current_or_next_run(self.time)

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
        first = True
        for name in self.job_definitions.keys():
            for job_time, retry, when in self.get_active_runs_for_job(name):
                newer = res_when is not None and (when is None or when < res_when)
                if first or newer:
                    res_when = when
                    res_job_name = name
                    res_job_time = job_time
                    res_retry = retry
                first = False
        return res_job_name, res_when, (res_job_time, res_retry)

    def get_next_run_for_job(self, job_name):
        res_when = None
        res_job_time = None
        res_retry = None
        for job_time, retry, when in self.get_active_runs_for_job(job_name):
            if res_when is None or when < res_when:
                res_when = when
                res_job_time = job_time
                res_retry = retry
        return res_when, (res_job_time, res_retry)

    def get_active_runs_for_job(self, job_name):
        job_definition: ScheduledJobDefinition = self.job_definitions[job_name]
        job_state: ScheduledJobState = self.job_states.get(job_name)

        next_from_startup = self.next_from_startup[job_name]
        prev_from_now = job_definition.current_or_prev_run(self.time)
        next_from_now = job_definition.current_or_next_run(self.time)

        if job_state is None:
            next_from_last = next_from_startup
        else:
            if job_state.last_state == RunState.FAIL and job_state.last_retry < job_definition.retries:
                next_from_last = job_state.last_run
            else:
                next_from_last = job_definition.next_run(job_state.last_run)

        first_active = next_from_last
        if job_definition.catchup_on_startup:
            first_active = max(first_active, next_from_startup)
        else:
            first_active = max(first_active, prev_from_now)

        if job_definition.late_start_cutoff is not None:
            earliest_possible = self.time - timedelta(seconds=job_definition.late_start_cutoff)
            next_from_earliest_possible = job_definition.current_or_next_run(earliest_possible)
            first_active = max(first_active, next_from_earliest_possible)

        def get_retry_and_run_time_for_job(job):
            if job_state is not None and job == job_state.last_run:
                if job_state.last_state == RunState.RUNNING:
                    retry_ = None
                    run_time_ = None
                else:
                    retry_ = job_state.last_retry + 1
                    run_time_ = job_state.last_finish_time + timedelta(seconds=job_definition.retry_wait)
            else:
                retry_ = 0
                run_time_ = job

            if run_time_ is not None and run_time_ <= self.time:
                run_time_ = None

            return retry_, run_time_

        actives = []
        active = first_active
        while active <= self.time:
            retry, run_time = get_retry_and_run_time_for_job(active)
            if run_time is None:
                run_time_ok = True
            elif job_definition.late_start_cutoff is None:
                run_time_ok = True
            elif run_time < active + timedelta(seconds=job_definition.late_start_cutoff):
                run_time_ok = True
            else:
                run_time_ok = False
            if run_time_ok and retry is not None:
                actives.append((active, retry, run_time))
            active = job_definition.next_run(active)
        if len(actives) == 0:
            job = max(next_from_last, next_from_now)
            retry, run_time = get_retry_and_run_time_for_job(job)
            actives.append((job, retry, run_time))

        return actives
