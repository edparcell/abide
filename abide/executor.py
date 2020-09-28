import multiprocessing
import uuid
import threading
import queue
import attr
import sys
import os
from typing import Dict, List
import subprocess
from datetime import datetime


local = threading.local()

def get_context():
    return local.context


@attr.s(auto_attribs=True)
class LogMessage:
    sender: str
    text: str


@attr.s(auto_attribs=True)
class CompletionMessage:
    sender: str
    pct_complete: float  # range 0-100


@attr.s(auto_attribs=True)
class Done:
    sender: str


@attr.s
class JobContext:
    ident = attr.ib()
    report_queue = attr.ib()
    cancel_event = attr.ib()
    is_done = attr.ib(default=False)

    def cancel(self):
        self.cancel_event.set()

    def is_canceled(self):
        return self.cancel_event.is_set()

    def wait(self, timeout):
        self.cancel_event.wait(timeout)

    def emit_message(self, msg):
        if not self.is_done:
            self.report_queue.put(msg)

    def emit_log_message(self, text):
        self.emit_message(LogMessage(self.ident, text))

    def emit_completion(self, pct_complete):
        self.emit_message(CompletionMessage(self.ident, pct_complete))

    def emit_done(self):
        self.emit_message(Done(self.ident))
        self.is_done = True


class Job:
    def __init__(self):
        self.context = None
        self.ident = None
        self.thread = None

    def start(self, context):
        if self.context is not None:
            raise Exception("Can only start job once")
        self.context = context
        self.thread = threading.Thread(target=self._run, name='Job-Thread-' + self.context.ident, daemon=True)
        self.thread.start()

    def _run(self):
        self.run()
        self.context.emit_done()

    def run(self):
        pass


class ThreadFunctionJob(Job):
    def __init__(self, target, *args, **kwargs):
        super().__init__()
        self.target = target
        self.args = args
        self.kwargs = kwargs

    def run(self):
        local.context = self.context
        self.target(*self.args, **self.kwargs)


def _run_multiprocessing(target, context, args, kwargs):
    local.context = context
    target(*args, **kwargs)


class MultiprocessingJob(Job):
    def __init__(self, target, *args, **kwargs):
        super().__init__()
        self.target = target
        self.args = args
        self.kwargs = kwargs

    def run(self):
        process_report_queue = multiprocessing.Queue()
        process_cancel_event = multiprocessing.Event()
        process_context = JobContext(self.context.ident, process_report_queue, process_cancel_event)
        p = multiprocessing.Process(target=_run_multiprocessing, args=(self.target, process_context, self.args, self.kwargs))
        p.start()
        while p.is_alive() or not process_report_queue.empty():
            if self.context.cancel_event.is_set(): # TODO: It would be nice not to wait for this
                process_cancel_event.set()
            try:
                o = process_report_queue.get(timeout=0.1)
                self.context.emit_message(o)
            except queue.Empty:
                pass


class PythonProcessJob(Job):
    def __init__(self, filename, args: [str] = None, env: Dict[str, str] = None, cwd=None):
        super().__init__()
        self.filename = filename
        self.args = args
        self.env = env
        self.cwd = cwd
        self.process = None

    def run(self):
        args = [sys.executable, str(self.filename)]
        if self.args is not None:
            args = args + self.args

        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'
        if self.env is not None:
            env.update(self.env)

        self.process = subprocess.Popen(args=args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=self.cwd,
                                        env=env)

        for line in iter(self.process.stdout):
            self.context.emit_log_message(line)
        self.process.stdout.close()


class Executor:
    def __init__(self):
        self.jobs = {}
        self.active_jobs = []
        self.job_table_lock = threading.Lock()
        self.cancel_event = threading.Event()
        self._internal_report_q = queue.SimpleQueue()
        self.report_q = queue.SimpleQueue()
        self.thread = threading.Thread(target=self._background_run, name='Executor-Thread')
        self.thread.start()

    def run(self, job):
        jid = str(uuid.uuid4())
        with self.job_table_lock:
            self.jobs[jid] = job
            self.active_jobs.append(jid)
        context = JobContext(jid, self._internal_report_q, threading.Event())
        job.start(context)

    def _background_run(self):
        while True:
            try:
                o = self._internal_report_q.get_nowait()
                if isinstance(o, Done):
                    self.active_jobs.remove(o.sender)
                self.report_q.put(o)
            except queue.Empty:
                pass

            if self.cancel_event.is_set():
                with self.job_table_lock:
                    for j in self.jobs.values():
                        j.context.cancel()
                break

    def stop(self):
        self.cancel_event.set()

    def any_alive(self):
        return len(self.active_jobs) > 0

    def print_messages_until_done(self, *, timeout=0.1, print_if_no_message=False, print_function=print):
        while True:
            try:
                o = self.report_q.get(timeout=timeout)
                print_function(datetime.now().isoformat(), o)
            except queue.Empty:
                if print_if_no_message:
                    print_function(datetime.now().isoformat(), "No Message")
            if self.report_q.empty() and not self.any_alive():
                break
        self.stop()
