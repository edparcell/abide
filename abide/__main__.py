import logging
import os
import pathlib
import subprocess
import sys
import time
from datetime import datetime
from typing import List, Dict

import nbformat
import yaml
from croniter import croniter
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert import PDFExporter

import attr
import click

from abide.schedule import Scheduler, read_job_definitions, ScheduledJobDefinition, RunState

FORMAT_STR = '%(asctime)s %(levelname)8s: P%(process)d T%(thread)d %(name)s %(module)s %(message)s'


def init_logging(verbose):
    if verbose >= 1:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    handler = logging.StreamHandler()

    formatter = logging.Formatter(FORMAT_STR)
    handler.setFormatter(formatter)
    logging.Formatter.converter = time.gmtime
    logging.root.addHandler(handler)
    logging.root.setLevel(loglevel)


def run_notebook(input_notebook: pathlib.Path, run_path: pathlib.Path, output_notebook: pathlib.Path, timeout: int=600):
    with open(input_notebook) as f_in:
        nb = nbformat.read(f_in, as_version=4)
    ep = ExecutePreprocessor(timeout=timeout, kernel_name='python3')
    ep.preprocess(nb, {'metadata': {'path': run_path}})
    with open(output_notebook, 'w', encoding='utf-8') as f_out:
        nbformat.write(nb, f_out)

    pdf_exporter = PDFExporter()
    pdf_exporter.template_name = 'classic'
    pdf_data, resources = pdf_exporter.from_notebook_node(nb)
    with open(output_notebook.with_suffix(".pdf"), 'wb') as f_pdf_out:
        f_pdf_out.write(pdf_data)


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


def execute_task(job: ScheduledJobDefinition, task_directory: TaskDirectory, execute_time: datetime):
    pth_task_dir = task_directory.get_job_output_dir(job.name)
    pth_task_run_dir = task_directory.get_job_run_output_dir(job.name, execute_time)

    env = os.environ.copy()
    env['ABIDE_JOB_PATH'] = str(pth_task_dir)
    env['ABIDE_JOB_RUN_PATH'] = str(pth_task_run_dir)

    task_filename = task_directory.task_directory / job.action
    extn = task_filename.suffix

    if extn == '.py':
        logging.info("Executing {} {}".format(sys.executable, task_filename))
        subprocess.run([sys.executable, str(task_filename)], env=env)
    elif extn == '.ipynb':
        logging.info("Running notebook {}".format(task_filename))
        pth = task_directory.task_directory
        fn_out = task_directory.get_task_run_output_filename(job.name, execute_time, 'ipynb')
        run_notebook(task_filename, pth, fn_out)
    else:
        logging.error("Unexpected extension: {}".format(extn))


def run_main_loop(task_directory: TaskDirectory, sleep_period=1):
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


@click.group()
def top_level():
    pass


@top_level.command('time')
def show_time():
    print(datetime.now())


@top_level.group()
def tasks():
    pass


@tasks.command(name='list')
@click.option('-d', '--task_directory', default='.', type=click.Path(exists=True))
@click.option('-v', '--verbose', count=True)
def list_tasks(task_directory: str, verbose: int):
    init_logging(verbose)
    task_directory = TaskDirectory(task_directory)
    job_definitions = task_directory.get_job_definitions()
    for name, job_definition in job_definitions.items():
        print("{}\t{}\t{}".format(job_definition.name, job_definition.schedule, job_definition.action))


@tasks.command(name='run')
@click.argument('task_name')
@click.option('-d', '--task_directory', default='.', type=click.Path(exists=True))
@click.option('-v', '--verbose', count=True)
def run_task(task_directory: str, verbose: int, task_name: str):
    init_logging(verbose)
    task_directory = TaskDirectory(task_directory)
    scheduler = task_directory.get_scheduler(None)
    job = scheduler.job_definitions[task_name]
    execute_task(job, task_directory, datetime.now())


@top_level.command()
@click.option('-d', '--task_directory', default='.', type=click.Path(exists=True))
@click.option('-v', '--verbose', count=True)
def server(task_directory: str, verbose: int):
    init_logging(verbose)
    task_directory = TaskDirectory(task_directory)

    logging.info("Running tasks from: {}".format(task_directory.task_directory.absolute()))
    logging.info("Verbosity: {}".format(verbose))
    run_main_loop(task_directory)


if __name__ == '__main__':
    top_level()
