
from vsat.task import task, TASK_REGISTRY

@task
def my_task(arg1, kwarg1=3):
    """
    Task for testing.
    """
    return (arg1, kwarg1)


def test_task_inserts_in_registry():
    """
    Test that the task decorator inserts the new task into the registry.
    """
    assert 'vsat.tests.test_task.my_task' in TASK_REGISTRY
    assert TASK_REGISTRY['vsat.tests.test_task.my_task']


def test_task_creates_callable():
    """
    Test that the task decorator actually creates a Task class.
    """
    assert getattr(my_task, "run", None) is not None


def test_task_can_still_be_run():
    """
    Tests that the decorated function can still be executed.
    """
    assert my_task(2) == (2, 3)
