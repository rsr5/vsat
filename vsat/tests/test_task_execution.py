
import time

from vsat.worker import WorkerPool
from vsat.task import task


@task
def atask(a, b):
    """
    Simple task for testing
    """
    return a * b


def test_execute_one_task():
    """
    Tests that a worker pool has workers.
    """
    num_workers = 1
    
    worker_pool = WorkerPool.init(num_workers=num_workers)

    assert len(worker_pool.workers) == num_workers

    worker_pool.start()

    assert len([w for w in worker_pool.workers if w.is_alive()]) == num_workers

    result = atask.apply_async(3, 3)

    assert result.get_result(block=True) == 9

    worker_pool.stop()

    assert len([w
                for w in worker_pool.workers
                if not w.is_alive()]) == num_workers


def test_execute_multiple_tasks():
    """
    Tests that a worker pool has workers.
    """
    num_workers = 20
    
    worker_pool = WorkerPool.init(num_workers=num_workers)

    assert len(worker_pool.workers) == num_workers

    worker_pool.start()

    assert len([w for w in worker_pool.workers if w.is_alive()]) == num_workers

    results = [atask.apply_async(y, y) for y in xrange(10000)]

    correct_results = [y * y for y in xrange(10000)]

    actual_results = [r.get_result(block=True) for r in results]

    assert set(correct_results) == set(actual_results)

    worker_pool.stop()

    assert len([w
                for w in worker_pool.workers
                if not w.is_alive()]) == num_workers
