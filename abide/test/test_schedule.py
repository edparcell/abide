from datetime import timedelta
from io import StringIO

import pandas as pd
from abide.schedule import ScheduledJobDefinition, ScheduledJobState, RunState, Scheduler, \
    read_job_definitions


def test_basic():
    s = Scheduler(pd.to_datetime('9/28/2020 13:06:03'),
                  {'A': ScheduledJobDefinition("* * * * *")})
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == when == pd.to_datetime('9/28/2020 13:07:00')
    assert job_retry == 0


def test_late_start_cutoff_zero():
    s = Scheduler(pd.to_datetime('9/28/2020 13:06:03'),
                  {'A': ScheduledJobDefinition("* * * * *", late_start_cutoff=0)})
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == when == pd.to_datetime('9/28/2020 13:07:00')
    assert job_retry == 0


def test_every_5_mins():
    s = Scheduler(pd.to_datetime('9/28/2020 13:06:03'),
                  {'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)})
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_last_run_complete():
    s = Scheduler(pd.to_datetime('9/28/2020 13:06:03'),
                  {'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)})
    s.job_states['A'] = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.COMPLETE)
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_last_run_running():
    s = Scheduler(pd.to_datetime('9/28/2020 13:06:03'),
                  {'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)})
    s.job_states['A'] = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.RUNNING)
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_last_run_fail():
    s = Scheduler(pd.to_datetime('9/28/2020 13:06:03'),
                  {'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)})
    s.job_states['A'] = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.FAIL)
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_last_run_fail_permanently():
    s = Scheduler(pd.to_datetime('9/28/2020 00:00:00'),
                  {'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=None, retries=3)})
    s.job_states['A'] = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.FAIL_PERMANENT)
    s.set_time(pd.to_datetime('9/28/2020 13:06:03'))
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_three_retries_last_run_complete():
    s = Scheduler(pd.to_datetime('9/28/2020 00:00:00'),
                  {'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=None, retries=3)})
    s.job_states['A'] = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.COMPLETE)
    s.set_time(pd.to_datetime('9/28/2020 13:06:03'))
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_three_retries_last_run_running():
    s = Scheduler(pd.to_datetime('9/28/2020 00:00:00'),
                  {'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=None, retries=3)})
    s.job_states['A'] = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.RUNNING)
    s.set_time(pd.to_datetime('9/28/2020 13:06:03'))
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_three_retries_last_run_fail():
    s = Scheduler(pd.to_datetime('9/28/2020 00:00:00'),
                  {'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=None, retries=3)})
    s.set_time(pd.to_datetime('9/28/2020 13:06:03'))
    s.job_states['A'] = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.FAIL)
    s.set_time(pd.to_datetime('9/28/2020 13:06:03'))
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == pd.to_datetime('9/28/2020 13:05:00')
    assert job_retry == 1


def test_three_retries_last_run_fail_permanently():
    s = Scheduler(pd.to_datetime('9/28/2020 00:00:00'),
                  {'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=None, retries=3)})
    s.set_time(pd.to_datetime('9/28/2020 13:06:03'))
    s.job_states['A'] = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.FAIL_PERMANENT)
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_get_next_if_not_running():
    s = Scheduler(pd.to_datetime('9/28/2020 13:06:03'),
                  {'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)})
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_get_next_if_running():
    s = Scheduler(pd.to_datetime('9/28/2020 13:06:03'),
                  {'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)})
    s.job_states['A'] = ScheduledJobState(pd.to_datetime('9/28/2020 13:10:00'), 0, None, RunState.RUNNING)
    when, (job_time, job_retry) = s.get_next_run_for_job('A')
    assert job_time == when == pd.to_datetime('9/28/2020 13:15:00')
    assert job_retry == 0


def test_scheduler_basic():
    s = Scheduler(pd.to_datetime('9/29/2020 14:05'),
                  {
                      'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0),
                      'B': ScheduledJobDefinition("*/12 * * * *", late_start_cutoff=0),
                  })

    job_name, when, (job_time, retry) = s.get_next_run()
    assert job_name == 'A'
    assert when is None
    assert job_time == pd.to_datetime('9/29/2020 14:05')
    assert retry == 0

    s.set_time(when)
    s.set_running(job_name, job_time, retry)

    s.set_time(pd.to_datetime('9/29/2020 14:06:30'))
    s.set_complete('A', RunState.COMPLETE)

    job_name, when, (job_time, retry) = s.get_next_run()
    assert job_name == 'A'
    assert when == pd.to_datetime('9/29/2020 14:10')
    assert job_time == pd.to_datetime('9/29/2020 14:10')
    assert retry == 0


def test_scheduler_retry_on_fail_no_retries():
    s = Scheduler(pd.to_datetime('9/29/2020 14:05'),
                  {
                      'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0),
                      'B': ScheduledJobDefinition("*/12 * * * *", late_start_cutoff=0),
                  })

    job_name, when, (job_time, retry) = s.get_next_run()
    assert job_name == 'A'
    assert when is None
    assert job_time == pd.to_datetime('9/29/2020 14:05')
    assert retry == 0

    s.set_time(when)
    s.set_running(job_name, job_time, retry)

    s.set_time(pd.to_datetime('9/29/2020 14:06:30'))
    s.set_complete('A', RunState.FAIL)

    job_name, when, (job_time, retry) = s.get_next_run()
    assert job_name == 'A'
    assert when == pd.to_datetime('9/29/2020 14:10')
    assert job_time == pd.to_datetime('9/29/2020 14:10')
    assert retry == 0


def test_scheduler_retry_on_fail_three_retries():
    s = Scheduler(pd.to_datetime('9/29/2020 14:05'),
                  {
                      'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=None, retries=3),
                      'B': ScheduledJobDefinition("*/12 * * * *", late_start_cutoff=None),
                  })

    job_name, when, (job_time, retry) = s.get_next_run()
    assert job_name == 'A'
    assert when is None
    assert job_time == pd.to_datetime('9/29/2020 14:05')
    assert retry == 0

    s.set_time(pd.to_datetime('9/29/2020 14:05'))
    s.set_running(job_name, job_time, retry)

    s.set_time(pd.to_datetime('9/29/2020 14:06:30'))
    s.set_complete('A', RunState.FAIL)

    job_name, when, (job_time, retry) = s.get_next_run()
    assert job_name == 'A'
    assert when is None
    assert job_time == pd.to_datetime('9/29/2020 14:05')
    assert retry == 1


def check_ran(runs, job_name, job_time, retry=None):
    if retry is None:
        label = "{} scheduled at {}".format(job_name, job_time.isoformat())
    else:
        label = "{} scheduled at {} (retry {})".format(job_name, job_time.isoformat(), retry)
    for this_job_name, this_when, (this_job_time, this_retry) in runs:
        if (job_name == this_job_name and
                job_time == this_job_time and
                (retry is None or retry == this_retry)):
            print("{} ran at {}".format(label, this_when.isoformat()))
            return
    raise Exception("Could not find run of {}".format(label))


def check_no_ran(runs, job_name, job_time, retry=None):
    if retry is None:
        label = "{} scheduled at {}".format(job_name, job_time.isoformat())
    else:
        label = "{} scheduled at {} (retry {})".format(job_name, job_time.isoformat(), retry)
    for this_job_name, this_when, (this_job_time, this_retry) in runs:
        if (job_name == this_job_name and
                job_time == this_job_time and
                (retry is None or retry == this_retry)):
            print("{} ran at {}".format(label, this_when.isoformat()))
            raise Exception("Fonud run of {}".format(label))


def get_next_n_runs(scheduler, n, job_duration=0):
    runs = []
    for i in range(n):
        job_name, when, (job_time, retry) = scheduler.get_next_run()
        if when is not None and when > scheduler.time:
            scheduler.set_time(when)
        scheduler.set_running(job_name, job_time, retry)
        runs.append((job_name, scheduler.time, (job_time, retry)))
        scheduler.set_time(scheduler.time + timedelta(seconds=job_duration))
        scheduler.set_complete(job_name, RunState.COMPLETE)
    return runs


def test_scheduler_doesnt_miss_concurrent_runs():
    job_definitions = read_job_definitions(StringIO("""
    A:
      schedule: 0 * * * *
    B:
      schedule: 0 * * * *
    """))

    scheduler = Scheduler(pd.to_datetime("10/1/2020 15:55"), job_definitions)

    runs = get_next_n_runs(scheduler, 4)

    check_ran(runs, "A", pd.to_datetime("10/1/2020 16:00"))
    check_ran(runs, "A", pd.to_datetime("10/1/2020 16:00"))
    check_ran(runs, "B", pd.to_datetime("10/1/2020 17:00"))
    check_ran(runs, "B", pd.to_datetime("10/1/2020 17:00"))


def test_scheduler_doesnt_miss_concurrent_runs_when_jobs_take_time():
    job_definitions = read_job_definitions(StringIO("""
    A:
      schedule: 0 * * * *
    B:
      schedule: 0 * * * *
    """))

    scheduler = Scheduler(pd.to_datetime("10/1/2020 15:55"), job_definitions)

    runs = get_next_n_runs(scheduler, 4, job_duration=1)

    check_ran(runs, "A", pd.to_datetime("10/1/2020 16:00"))
    check_ran(runs, "B", pd.to_datetime("10/1/2020 16:00"))
    check_ran(runs, "A", pd.to_datetime("10/1/2020 17:00"))
    check_ran(runs, "B", pd.to_datetime("10/1/2020 17:00"))


def test_scheduler_start_up_late_start_run_all_on_late_start_true():
    job_definitions = read_job_definitions(StringIO("""
    A:
      schedule: 0 * * * *
      catchup_on_startup: true
      run_all_on_late_start: true
    """))

    scheduler = Scheduler(pd.to_datetime("10/1/2020 13:30"), job_definitions)
    scheduler.job_states['A'] = ScheduledJobState(
        last_run=pd.to_datetime("10/1/2020 13:00"),
        last_retry=0,
        last_finish_time=pd.to_datetime("10/1/2020 13:01"),
        last_state=RunState.COMPLETE)
    scheduler.set_time(pd.to_datetime("10/1/2020 15:55"))

    runs = get_next_n_runs(scheduler, 4, job_duration=1)

    check_ran(runs, "A", pd.to_datetime("10/1/2020 14:00"))
    check_ran(runs, "A", pd.to_datetime("10/1/2020 15:00"))
    check_ran(runs, "A", pd.to_datetime("10/1/2020 16:00"))
    check_ran(runs, "A", pd.to_datetime("10/1/2020 17:00"))


def test_scheduler_start_up_late_start_run_all_on_late_start_false():
    job_definitions = read_job_definitions(StringIO("""
    A:
      schedule: 0 * * * *
      run_all_on_late_start: false
    """))

    scheduler = Scheduler(pd.to_datetime("10/1/2020 15:55"), job_definitions)
    scheduler.job_states['A'] = ScheduledJobState(
        last_run=pd.to_datetime("10/1/2020 13:00"),
        last_retry=0,
        last_finish_time=pd.to_datetime("10/1/2020 13:01"),
        last_state=RunState.COMPLETE)

    runs = get_next_n_runs(scheduler, 4, job_duration=1)

    check_no_ran(runs, "A", pd.to_datetime("10/1/2020 14:00"))
    check_ran(runs, "A", pd.to_datetime("10/1/2020 15:00"))
    check_ran(runs, "A", pd.to_datetime("10/1/2020 16:00"))
    check_ran(runs, "A", pd.to_datetime("10/1/2020 17:00"))
