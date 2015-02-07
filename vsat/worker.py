"""
Contains the task worker that executes queued tasks.
"""
import logging

from multiprocessing import Process, Queue

LOG = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30


def _process(queue):
    """
    Executes the tasks for a worker.
    """
    finished = False
    while not finished:
        item = queue.get()
        print item

        if item == "stop":
            finished = True


class WorkerPool(object):
    """
    Looks after the pool of workers.
    """
    def __init__(self, num_workers=1):
        """
        Creates a WorkerPool.
        """
        self.queue = Queue()
        self.workers = []

        for _ in xrange(num_workers):
            self.workers.append(Process(target=_process, args=(self.queue, )))

    def start(self):
        """
        Starts this worker pool.
        """
        for worker in self.workers:
            worker.start()

    def stop(self, timeout=DEFAULT_TIMEOUT):
        """
        Shuts down all workers.
        """
        LOG.info("Shutting down %d workers.", len(self.workers))
        for worker in self.workers:
            # Inform each worker to stop and then wait for them
            self.queue.put("stop")

        for worker in self.workers:
            worker.join(timeout=timeout)

    def schedule_task(self, task):
        """
        Queues a Task to be executed by a Worker.
        """
        pass
