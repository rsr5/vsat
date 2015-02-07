import pytest

from vsat.worker import WorkerPool, PoolNotInitialised


def test_exception_on_pool_non_init():
    """
    Test that it is not possible to get a non initialised worker_pool
    """
    with pytest.raises(PoolNotInitialised):
        WorkerPool.get()


def test_pool_creates_workers():
    """
    Tests that a worker pool has workers.
    """
    num_workers = 20
    
    worker_pool = WorkerPool.init(num_workers=num_workers)

    assert len(worker_pool.workers) == num_workers

    worker_pool.start()

    assert len([w for w in worker_pool.workers if w.is_alive()]) == num_workers

    worker_pool.schedule_task("Hello")

    worker_pool.stop()

    assert len([w
                for w in worker_pool.workers
                if not w.is_alive()]) == num_workers
