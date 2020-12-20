import pathlib
from datetime import datetime
import peewee

from abide.schedule import read_job_definitions, Scheduler


class TaskDirectory:
    def __init__(self, task_directory):
        self.task_directory = pathlib.Path(task_directory)

    def get_job_definitions(self):
        schedule_file = self.task_directory / 'schedule.yaml'
        job_definitions = read_job_definitions(schedule_file)
        return job_definitions

    def get_scheduler(self, start_time) -> Scheduler:
        job_definitions = self.get_job_definitions()
        scheduler = Scheduler(start_time, job_definitions)
        return scheduler

    def get_job_output_dir(self, job_name: str):
        pth = self.task_directory / 'output' / job_name
        return pth.absolute()

    def get_job_run_output_dir(self, job_name: str, job_time: datetime):
        pth = self.get_job_output_dir(job_name) / '{:%Y%m%d-%H%M%S}'.format(job_time)
        return pth.absolute()

    def get_task_run_output_filename(self, job_name: str, job_time: datetime, extension):
        pth = self.get_job_output_dir(job_name) / '{:%Y%m%d-%H%M%S}.{}'.format(job_time, extension)
        return pth.absolute()