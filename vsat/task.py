
# Disable a couple of Lint warnings here because we need to do a bit of
# magic
# pylint: disable=C0111
# pylint: disable=W0142


import uuid
import json

from os import remove
from os.path import join, exists

# Will be used to store tasks as they are registered.
TASK_REGISTRY = {}

RESULT_LOCATION = "/tmp/"


class NotReady(Exception):
    """
    Raised if a result it not ready yet.
    """


class InvalidTask(Exception):
    """
    Raised if a there us no current task for a particular uuid.
    """


class AsyncResult(object):
    """
    Contains the promise of a result from Task once it is executed.
    """
    def __init__(self, task_uuid=None):
        """
        Creates a new result.
        """
        self.task_uuid = str(uuid.uuid4()) if task_uuid is None else task_uuid
        self.state = "QUEUED"

        if task_uuid is None:
            self._save_state("task_uuid", self.task_uuid)
            self._save_state("state", self.state)
            self._save_state("result", None)
        else:
            if not exists(self.path):
                raise InvalidTask()

    @property
    def path(self):
        """
        Returns the path for this results state.
        """
        return join(RESULT_LOCATION, str(self.task_uuid))

    def _load_state(self):
        """
        Returns a dictionary containing this results state.
        """
        state = {}

        if exists(self.path):
            with open(self.path, "r") as state_file:
                state = json.loads(state_file.read())
    
        return state

    def _save_state(self, key, value):
        """
        Returns a dictionary containing this results state.
        """
        state = self._load_state()
        state[key] = value

        with open(self.path, "w+") as state_file:
            state_file.write(json.dumps(state))

    def set_state(self, state):
        """
        Sets the state of the remote task.
        """
        self._save_state("state", state)

    def set_result(self, result):
        """
        Sets the result of the remote task.
        """
        self._save_state("result", result)
        self._save_state("state", "FINISHED")

    def get_result(self):
        """
        Attempts to get the result, an exception is raises if it is not
        ready.
        """
        if self._load_state()['state'] != "FINISHED":
            raise NotReady()

        state = self._load_state()
        remove(self.path)

        return state['result']

    def get_state(self):
        """
        Returns the state of the task.
        """
        return self._load_state()['state']


class Task(object):
    """
    A task that can be called asynchronously.
    """
    run = {"func": lambda: None}
    name = "task"

    def to_json(self):
        """
        Turns this task into JSON so that it may be queued for a worker.
        """
        return json.dumps({
            "name": self.name
        })

    @classmethod
    def from_json(cls, task_json):
        """
        Returns a Task object that maybe called by a worker.
        """
        return TASK_REGISTRY[json.dumps(task_json)["name"]]

    def apply(self, *args, **kwargs):
        """
        Applies the task locally.
        """
        return self.run['func'](*args, **kwargs)

    def apply_async(self, *args, **kwargs):
        """
        Applies the task remotely and returns an AsyncResult object.
        """
        return self.run['func'](*args, **kwargs)

    def __call__(self, *args, **kwargs):
        """
        Leaves the task callable as a normal function.
        """
        return self.apply(*args, **kwargs)

    @classmethod
    def func_to_name(cls, func):
        """
        Returns a standard name for a task function.
        """
        return ".".join([func.__module__, func.__name__])


def task(*args, **options):
    """Creates new task class from any callable."""

    def inner_create_task_cls(**options):

        def _create_task_cls(func):

            return task_from_func(func, **options)

        return _create_task_cls

    if len(args) == 1:
        if callable(args[0]):
            return inner_create_task_cls(**options)(*args)
        raise TypeError('argument 1 to @task() must be a callable')
    if args:
        raise TypeError(
            '@task() takes exactly 1 argument ({0} given)'.format(
                sum([len(args), len(options)])))

    return inner_create_task_cls(**options)


def task_from_func(func, **options):
    """
    Creates a Callable class from any Callable.
    """
    name = Task.func_to_name(func)

    if name not in TASK_REGISTRY:
        _task = type(func.__name__, (Task, ), dict({
            'name': name,
            'run': {'func': func},
            '__doc__': func.__doc__,
            '__module__': func.__module__}, **options))()
        TASK_REGISTRY[name] = _task

    else:
        _task = TASK_REGISTRY[name]

    return _task
