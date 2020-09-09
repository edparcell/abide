import logging
import pathlib
import subprocess
import sys
import time
from datetime import datetime
from typing import List

import nbformat
import yaml
from croniter import croniter
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert import PDFExporter

import attr
import click

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


@attr.s(auto_attribs=True)
class TaskDefinition:
    timespec: str
    name: str
    extension: str
    filename: pathlib.Path

    def execute(self, dt):
        if self.extension.lower() == '.py':
            logging.info("Executing {} {}".format(sys.executable, str(self.filename)))
            subprocess.call([sys.executable, str(self.filename)])
        elif self.extension.lower() == '.ipynb':
            logging.info("Running notebook {}".format(self.filename))
            pth = self.filename.parent
            pth_out = pth / self.name
            pth_out.mkdir(exist_ok=True)
            fn_out = pth_out / '{}-{:%Y%m%d}.ipynb'.format(self.name, dt)
            run_notebook(self.filename, pth, fn_out)
        else:
            logging.error("Unexpected extension: {}".format(self.extension))

    def get_last_scheduled_execution(self, current_time: datetime):
        ci = croniter(self.timespec, current_time)
        execution_time = ci.get_prev(ret_type=datetime)
        logging.debug("Current Time: {}, Last Scheduled Execution: {}".format(current_time, execution_time))
        return execution_time


def get_schedule_for_dir(task_directory: pathlib.Path) -> List[TaskDefinition]:
    schedule_file = task_directory / 'schedule.yaml'
    with schedule_file.open() as f:
        schedule_items = yaml.load(f, Loader=yaml.CLoader)
        schedule = []
        for name, task in schedule_items.items():
            pth = pathlib.Path(task['file'])
            td = TaskDefinition(task['schedule'], name, pth.suffix, pth)
            schedule.append(td)
    return schedule


def get_task(task_directory: pathlib.Path, task_name: str):
    schedule = get_schedule_for_dir(task_directory)
    return schedule.get(task_name)


def run_main_loop(task_directory: pathlib.Path, sleep_period=1):
    last_run_time = datetime.utcnow()
    while True:
        logging.debug("Waking up")
        this_run_time = datetime.utcnow()

        schedule = get_schedule_for_dir(task_directory)
        for task in schedule:
            task_last_run_time = last_run_time
            task_last_scheduled_execution = task.get_last_scheduled_execution(this_run_time)
            if task_last_scheduled_execution > task_last_run_time:
                logging.debug("Running Task {}, Last Task Run Time: {}, Last Scheduled Execution: {}".format(
                    task.name, task_last_run_time, task_last_scheduled_execution))
                task.execute(this_run_time)

        last_run_time = this_run_time
        logging.debug("Sleeping for {} seconds".format(sleep_period))
        time.sleep(sleep_period)


@click.group()
def top_level():
    pass


@top_level.command('time')
def show_time():
    print(datetime.utcnow())


@top_level.group()
def tasks():
    pass


@tasks.command(name='list')
@click.option('-d', '--task_directory', default='.', type=click.Path(exists=True))
@click.option('-v', '--verbose', count=True)
def list_tasks(task_directory: str, verbose: int):
    init_logging(verbose)
    task_directory = pathlib.Path(task_directory)
    schedule = get_schedule_for_dir(task_directory)
    for task in schedule:
        print('{}\t{}\t{}'.format(task.name, task.timespec, task.filename))


@tasks.command(name='run')
@click.argument('task_name')
@click.option('-d', '--task_directory', default='.', type=click.Path(exists=True))
@click.option('-v', '--verbose', count=True)
def list_tasks(task_directory: str, verbose: int, task_name: str):
    init_logging(verbose)
    task_directory = pathlib.Path(task_directory)
    task = get_task(task_directory, task_name)
    task.execute(datetime.utcnow())


@top_level.command()
@click.option('-d', '--task_directory', default='.', type=click.Path(exists=True))
@click.option('-v', '--verbose', count=True)
def server(task_directory: str, verbose: int):
    init_logging(verbose)
    task_directory = pathlib.Path(task_directory)

    logging.info("Running tasks from: {}".format(task_directory.absolute()))
    logging.info("Verbosity: {}".format(verbose))
    run_main_loop(task_directory)


if __name__ == '__main__':
    top_level()
