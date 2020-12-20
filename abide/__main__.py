import logging
import time
from datetime import datetime

import click

from abide.runners import execute_task
from abide.server import run_server_main_loop
from abide.taskdirectory import TaskDirectory

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
    run_server_main_loop(task_directory)


if __name__ == '__main__':
    top_level()
