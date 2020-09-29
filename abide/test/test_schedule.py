import pandas as pd
from abide.schedule import ScheduledJobDefinition, get_next_run, ScheduledJobState, RunState, Scheduler


def test_basic():
    sjd = ScheduledJobDefinition("* * * * *")
    sjs = None
    now = pd.to_datetime('9/28/2020 13:06:03')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
    assert job_time == when == pd.to_datetime('9/28/2020 13:07:00')
    assert job_retry == 0


def test_late_start_cutoff_zero():
    sjd = ScheduledJobDefinition("* * * * *", late_start_cutoff=0)
    sjs = None
    now = pd.to_datetime('9/28/2020 13:06:03')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
    assert job_time == when == pd.to_datetime('9/28/2020 13:07:00')
    assert job_retry == 0


def test_every_5_mins():
    sjd = ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)
    sjs = None
    now = pd.to_datetime('9/28/2020 13:06:03')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_last_run_complete():
    sjd = ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)
    sjs = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.COMPLETE)
    now = pd.to_datetime('9/28/2020 13:06:03')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_last_run_running():
    sjd = ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)
    sjs = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.RUNNING)
    now = pd.to_datetime('9/28/2020 13:06:03')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_last_run_fail():
    sjd = ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)
    sjs = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.FAIL)
    now = pd.to_datetime('9/28/2020 13:06:03')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_last_run_fail_permanently():
    sjd = ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)
    sjs = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.FAIL_PERMANENT)
    now = pd.to_datetime('9/28/2020 13:06:03')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_three_retries_last_run_complete():
    sjd = ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0, retries=3)
    sjs = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.COMPLETE)
    now = pd.to_datetime('9/28/2020 13:06:03')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_three_retries_last_run_running():
    sjd = ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0, retries=3)
    sjs = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.RUNNING)
    now = pd.to_datetime('9/28/2020 13:06:03')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_three_retries_last_run_fail():
    sjd = ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0, retries=3)
    sjs = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.FAIL)
    now = pd.to_datetime('9/28/2020 13:06:03')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
    assert job_time == pd.to_datetime('9/28/2020 13:05:00')
    assert job_retry == 1


def test_three_retries_last_run_fail_permanently():
    sjd = ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0, retries=3)
    sjs = ScheduledJobState(pd.to_datetime('9/28/2020 13:05:00'), 0, pd.to_datetime('9/28/2020 13:05:33'), RunState.FAIL_PERMANENT)
    now = pd.to_datetime('9/28/2020 13:06:03')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_get_next_if_not_running():
    sjd = ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)
    sjs = None
    now = pd.to_datetime('9/28/2020 13:10:00')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
    assert job_time == when == pd.to_datetime('9/28/2020 13:10:00')
    assert job_retry == 0


def test_get_next_if_running():
    sjd = ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0)
    sjs = ScheduledJobState(pd.to_datetime('9/28/2020 13:10:00'), 0, None, RunState.RUNNING)
    now = pd.to_datetime('9/28/2020 13:10:00')
    when, (job_time, job_retry) = get_next_run(now, sjd, sjs)
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
    assert when == pd.to_datetime('9/29/2020 14:05')
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
    assert when == pd.to_datetime('9/29/2020 14:05')
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
                      'A': ScheduledJobDefinition("*/5 * * * *", late_start_cutoff=0, retries=3),
                      'B': ScheduledJobDefinition("*/12 * * * *", late_start_cutoff=0),
                  })

    job_name, when, (job_time, retry) = s.get_next_run()
    assert job_name == 'A'
    assert when == pd.to_datetime('9/29/2020 14:05')
    assert job_time == pd.to_datetime('9/29/2020 14:05')
    assert retry == 0

    s.set_time(when)
    s.set_running(job_name, job_time, retry)

    s.set_time(pd.to_datetime('9/29/2020 14:06:30'))
    s.set_complete('A', RunState.FAIL)

    job_name, when, (job_time, retry) = s.get_next_run()
    assert job_name == 'A'
    assert when == pd.to_datetime('9/29/2020 14:06:30')
    assert job_time == pd.to_datetime('9/29/2020 14:05')
    assert retry == 1