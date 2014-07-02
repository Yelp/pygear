import pytest
import pygear
import multiprocessing
from mock import Mock
import sys

TEST_GEARMAN_SERVERS = ["srv1-devc"]
TEST_TIMEOUT_SECONDS = 15

class TestError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

@pytest.fixture
def c():
    client = pygear.Client()
    client.add_servers(TEST_GEARMAN_SERVERS)
    client.set_timeout(TEST_TIMEOUT_SECONDS * 1000)
    return client

@pytest.fixture
def w():
    worker = pygear.Worker()
    worker.add_servers(TEST_GEARMAN_SERVERS)
    worker.set_timeout(1 * 1000)
    return worker

def worker_function_echo(Job):
    return Job.workload()

def thread_worker_echo():
    worker = w()
    worker.add_function("test_integration_echo", 0, worker_function_echo)
    sys.stderr.write("Worker starting...\n")
    try:
        while True:
            worker.work()
    except pygear.TIMEOUT:
        pass
    sys.stderr.write("Worker done\n")

def thread_worker_data():
    def worker_fn_data(J):
        J.send_data("test_worker_data")

    worker = w()
    worker.add_function("test_integration_data", 0, worker_fn_data)
    sys.stderr.write("Worker starting...\n")
    try:
        while True:
            worker.work()
    except pygear.TIMEOUT:
        pass
    sys.stderr.write("Worker done\n")

def thread_worker_fail():
    def worker_fn_fail(J):
        J.send_fail()

    worker = w()
    worker.add_function("test_integration_fail", 0, worker_fn_fail)
    sys.stderr.write("Worker starting...\n")
    try:
        while True:
            worker.work()
    except pygear.TIMEOUT:
        pass
    sys.stderr.write("Worker done\n")

def thread_worker_except():
    def throw_exn(J):
        raise TestError

    worker = w()
    worker.add_function("test_integration_except", 0, throw_exn)
    sys.stderr.write("Worker starting...\n")
    try:
        while True:
            worker.work()
    except pygear.TIMEOUT:
        pass
    sys.stderr.write("Worker done\n")


def thread_client_echo():
    client = c()
    sys.stderr.write("Client running...\n")
    do_result = client.do("test_integration_echo", "Test string!")
    sys.stderr.write("Client done\n")
    assert do_result == "Test string!"

def test_do_success(c):
    worker_thread = multiprocessing.Process(target=thread_worker_echo)
    worker_thread.start()

    client_thread = multiprocessing.Process(target=thread_client_echo)
    client_thread.start()

    client_thread.join()
    worker_thread.join()

def test_callback_created(c):
    cb_test = Mock()
    c.set_created_fn(cb_test)
    c.add_task("test_integration_echo", "Some string")
    worker_thread = multiprocessing.Process(target=thread_worker_echo)
    worker_thread.start()
    c.run_tasks()
    worker_thread.join()
    assert cb_test.called

def test_callback_complete(c):
    cb_test = Mock()
    c.set_complete_fn(cb_test)
    c.add_task("test_integration_echo", "Some string")
    worker_thread = multiprocessing.Process(target=thread_worker_echo)
    worker_thread.start()
    c.run_tasks()
    worker_thread.join()
    assert cb_test.called

def test_callback_data(c):
    cb_test = Mock()
    c.set_data_fn(cb_test)
    c.add_task("test_integration_data", "Some string")
    worker_thread = multiprocessing.Process(target=thread_worker_data)
    worker_thread.start()
    c.run_tasks()
    worker_thread.join()
    assert cb_test.called

def test_callback_fail(c):
    cb_test = Mock()
    c.set_fail_fn(cb_test)
    c.add_task("test_integration_fail", "Some string")
    worker_thread = multiprocessing.Process(target=thread_worker_fail)
    worker_thread.start()
    c.run_tasks()
    worker_thread.join()
    assert cb_test.called

def test_callback_exception(c):
    cb_test = Mock()
    c.set_exception_fn(cb_test)
    c.add_task("test_integration_except", "Some string")
    worker_thread = multiprocessing.Process(target=thread_worker_except)
    worker_thread.start()
    c.run_tasks()
    worker_thread.join()
    assert cb_test.called
