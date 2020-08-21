import attr
import heapq


@attr.s
class CallBackRequest:
    time = attr.ib()
    ctx = attr.ib()


@attr.s
class StopRequest:
    ctx = attr.ib()


class FakeClock:
    def __init__(self, start_time):
        self.time_ = start_time

    def set_time(self, time):
        self.time_ = time

    @property
    def time(self):
        return self.time_


@attr.s
class TaskEnvironment:
    clock = attr.ib()


class AbideTaskManager:
    def __init__(self, clock):
        self.clock = clock
        self.task_states = {}
        self.task_functions = {}
        self.task_contexts = {}
        self.schedule = []

    def add_task(self, name, task_f):
        self.task_functions[name] = task_f
        self.task_states[name] = 1
        self.run_task_once(name)

    def run_task_once(self, name):
        task_f = self.task_functions[name]
        env = TaskEnvironment(self.clock)
        ctx = self.task_contexts.get(name)
        res = task_f(env, ctx)
        if isinstance(res, CallBackRequest):
            self.task_contexts[name] = res.ctx
            heapq.heappush(self.schedule, (res.time, name))
        elif isinstance(res, StopRequest):
            self.task_contexts[name] = res.ctx
            self.task_states[name] = 0
        else:
            raise Exception("Unexpected response from task_function for {}: {}".format(name, res))

    def flush(self):
        time = self.clock.time
        while len(self.schedule) > 0:
            task_time, task_name = self.schedule[0]
            if task_time > time:
                break
            heapq.heappop(self.schedule)
            self.run_task_once(task_name)

    def get_context(self, name):
        return self.task_contexts.get(name)
