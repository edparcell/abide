import logging
import os
import pathlib
import subprocess
import sys
from datetime import datetime

import nbformat
from nbconvert import PDFExporter
from nbconvert.preprocessors import ExecutePreprocessor

from abide.schedule import ScheduledJobDefinition
from abide.taskdirectory import TaskDirectory


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