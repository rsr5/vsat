
from vsat.worker import WorkerPool


def test_pool_creates_workers():
    """
    Tests that a worker pool has workers.
    """
    num_workers = 20
    worker_pool = WorkerPool(num_workers=num_workers)

    assert len(worker_pool.workers) == num_workers

    worker_pool.start()

    assert len([w for w in worker_pool.workers if w.is_alive()]) == num_workers

    worker_pool.schedule_task("Hello")

    worker_pool.stop()

    assert len([w
                for w in worker_pool.workers
                if not w.is_alive()]) == num_workers
