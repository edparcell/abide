import logging
import time
from datetime import datetime

from abide.__main__ import TaskDirectory, execute_task
from abide.schedule import RunState


def run_server_main_loop(task_directory: TaskDirectory, sleep_period=1):
    scheduler = task_directory.get_scheduler(datetime.now())

    while True:
        logging.debug("Waking up")
        now = datetime.now()
        scheduler.set_time(now)
        job_name, when, (job_time, retry) = scheduler.get_next_run()
        if when is None:
            logging.info("Running {} for {} (retry {}) at {}".format(job_name, job_time, retry, now))
            job_definition = scheduler.job_definitions[job_name]
            scheduler.set_running(job_name, job_time, retry)
            execute_task(job_definition, task_directory, job_time)
            scheduler.set_complete(job_name, RunState.COMPLETE)
        else:
            logging.debug("Sleeping for {} seconds".format(sleep_period))
            time.sleep(sleep_period)