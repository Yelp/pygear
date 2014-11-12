import multiprocessing
import pytest
import pygear

from . import TEST_SERVER_HOST
from . import TEST_SERVER_PORT
from . import TEST_TIMEOUT_MSEC


class TestError(Exception):
    pass


@pytest.fixture
def c():
    client = pygear.Client()
    client.add_server(TEST_SERVER_HOST, TEST_SERVER_PORT)
    client.set_timeout(TEST_TIMEOUT_MSEC)
    return client


@pytest.fixture
def w():
    worker = pygear.Worker()
    worker.add_server(TEST_SERVER_HOST, TEST_SERVER_PORT)
    worker.set_timeout(TEST_TIMEOUT_MSEC)
    return worker


TEST_FUNCTION_NAME = 'test_job_methods'
TEST_WORKLOAD = 'this is a dummy workload'
TEST_UNIQUE = 'test unique string'


def job_test_function(job):
    assert type(job.handle()) is str
    assert job.function_name() == TEST_FUNCTION_NAME
    assert job.unique() == TEST_UNIQUE
    assert job.workload() == TEST_WORKLOAD
    assert job.workload_size() == len(TEST_WORKLOAD)


def thread_worker():
    worker = w()
    worker.add_function(TEST_FUNCTION_NAME, 0, job_test_function)
    try:
        while True:
            worker.work()
    except pygear.TIMEOUT:
        pass


def test_job_methods(c):
    c.add_task(TEST_FUNCTION_NAME, TEST_WORKLOAD, unique=TEST_UNIQUE)
    worker_thread = multiprocessing.Process(target=thread_worker)
    worker_thread.start()
    c.run_tasks()
    worker_thread.join()
