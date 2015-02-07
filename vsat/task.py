
# Disable a couple of Lint warnings here because we need to do a bit of
# magic
# pylint: disable=C0111
# pylint: disable=W0142


import json

# Will be used to store tasks as they are registered.
TASK_REGISTRY = {}


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
            '__module__': func.__module__,
            #'__header__': staticmethod(head_from_fun(fun, bound=bind)),
            '__wrapped__': func}, **options))()
        TASK_REGISTRY[name] = _task

    else:
        _task = TASK_REGISTRY[name]

    return _task
