from abide.taskmanager import CallBackRequest, StopRequest, FakeClock, AbideTaskManager


def test_hello_world():
    def task_f(env, ctx):
        current_time = env.clock.time
        if ctx is None:
            return CallBackRequest(time=1, ctx={'task_has_run': False})
        elif current_time >= 1:
            return StopRequest(ctx={'task_has_run': True})
        else:
            raise Exception("Unexpected")

    clock = FakeClock(start_time=0)
    mgr = AbideTaskManager(clock=clock)
    mgr.add_task('FirstTask', task_f)

    ctx = mgr.get_context('FirstTask')
    assert not ctx['task_has_run']

    clock.set_time(0)
    mgr.flush()
    ctx = mgr.get_context('FirstTask')
    assert not ctx['task_has_run']

    clock.set_time(1)
    mgr.flush()
    ctx = mgr.get_context('FirstTask')
    assert ctx['task_has_run']
