import uuid
from threading import Thread, Event, Lock
from queue import SimpleQueue, Empty
from enum import Enum
import attr


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


class Job:
    def __init__(self):
        self.executor = None
        self.ident = None
        self.cancel_event = None
        self.thread = None
        self.is_done = False

    def start(self, executor, ident):
        if self.executor is not None:
            raise Exception("Can only start job once")
        self.executor = executor
        self.ident = ident
        self.cancel_event = Event()
        self.thread = Thread(target=self._run, name='Job-Thread-' + ident, daemon=True)
        self.thread.start()

    def cancel(self):
        self.cancel_event.set()

    def is_canceled(self):
        return self.cancel_event.is_set()

    def wait(self, timeout):
        self.cancel_event.wait(timeout)

    def _emit_message(self, msg):
        self.executor._internal_report_q.put(msg)

    def emit_log_message(self, text):
        self._emit_message(LogMessage(self.ident, text))

    def emit_completion(self, pct_complete):
        self._emit_message(CompletionMessage(self.ident, pct_complete))

    def emit_done(self):
        if not self.is_done:
            self._emit_message(Done(self.ident))
        self.is_done = True

    def _run(self):
        self.run()
        self.emit_done()

    def run(self):
        pass


class Executor:
    def __init__(self):
        self.jobs = {}
        self.active_jobs = []
        self.job_table_lock = Lock()
        self.cancel_event = Event()
        self._internal_report_q = SimpleQueue()
        self.report_q = SimpleQueue()
        self.thread = Thread(target=self._background_run, name='Executor-Thread')
        self.thread.start()

    def run(self, job):
        jid = str(uuid.uuid4())
        with self.job_table_lock:
            self.jobs[jid] = job
            self.active_jobs.append(jid)
        job.start(self, jid)

    def _background_run(self):
        while True:
            try:
                o = self._internal_report_q.get_nowait()
                if isinstance(o, Done):
                    self.active_jobs.remove(o.sender)
                self.report_q.put(o)
            except Empty:
                pass

            if self.cancel_event.is_set():
                with self.job_table_lock:
                    for j in self.jobs.values():
                        j.cancel()
                break

    def stop(self):
        self.cancel_event.set()

    def any_alive(self):
        return len(self.active_jobs) > 0
