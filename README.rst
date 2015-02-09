
Very Simple Asynchronous Tasks
------------------------------

A small library that allows asynchronous tasks to be executed.  Useful for
web applications when you need a task to run in the background.  The API
is modelled loosely on Celery_.

.. _Celery: https://github.com/celery/celery

Example
-------

Install the library

::

    pip install vsat

Create and run task.

::

    In [1]: from vsat.task import task

    In [2]: from vsat.worker import WorkerPool

    In [3]: worker_pool = WorkerPool.init(num_workers=1)

    In [4]: @task
       ...: def my_function(arg1):
       ...:     return arg1 * arg1
       ...:

    In [5]: worker_pool.start()

    In [6]: result = my_function.apply_async(2)

    In [7]: print result.get_result(block=True)
    4
