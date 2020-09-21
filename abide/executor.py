import io
import os
import subprocess
import sys
import uuid
from typing import Dict, List
import attr
from threading import Thread


@attr.s
class Process:
    upid: str = attr.ib()
    process_object: object = attr.ib()
    stdout: io.TextIOBase = attr.ib(default=attr.Factory(io.BytesIO))
    stderr: io.TextIOBase = attr.ib(default=attr.Factory(io.BytesIO))


class Executor:
    def __init__(self):
        self.process_table = {}

    def execute_python_script(self, filename, args: [str] = None, env_vars: Dict[str, str] = None,
                              stdout_file: io.TextIOBase = None, stderr_file: io.TextIOBase = None, cwd=None):
        upid = str(uuid.uuid4())
        process = Process(upid, None)

        args_popen = [sys.executable, str(filename)]
        if args is not None:
            args_popen = args_popen + args

        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'
        if env_vars is not None:
            env.update(env_vars)

        process.process_object = subprocess.Popen(args=args_popen, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd, env=env)

        def log_output(fh, *outfiles):
            for line in iter(fh.readline, b''):
                for f in outfiles:
                    if f is not None:
                        f.write(line)
            fh.close()

        t_stdout = Thread(target=log_output, args=(process.process_object.stdout, process.stdout, stdout_file), daemon=True)
        t_stderr = Thread(target=log_output, args=(process.process_object.stderr, process.stderr, stderr_file), daemon=True)
        t_stdout.start()
        t_stderr.start()

        self.process_table[upid] = process
        return process

    def get_process_by_id(self, upid):
        return self.process_table[upid]