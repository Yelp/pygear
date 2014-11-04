import mock
import multiprocessing
import pytest
import pygear
import sys

from . import TEST_GEARMAN_SERVERS
from . import TEST_TIMEOUT_MSEC
from . import cat_serializer


class TestError(Exception):
    pass


@pytest.fixture
def c():
    client = pygear.Client()
    client.add_servers(TEST_GEARMAN_SERVERS)
    client.set_timeout(TEST_TIMEOUT_MSEC)
    return client


@pytest.fixture
def w():
    worker = pygear.Worker()
    worker.add_servers(TEST_GEARMAN_SERVERS)
    worker.set_timeout(TEST_TIMEOUT_MSEC)
    return worker


def thread_worker_echo():
    def worker_fn_echo(job):
        return job.workload()

    worker = w()
    worker.add_function("test_integration_echo", 0, worker_fn_echo)
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


def test_client_do(c):
    worker_thread = multiprocessing.Process(target=thread_worker_echo)
    worker_thread.start()
    client_thread = multiprocessing.Process(target=thread_client_echo)
    client_thread.start()
    client_thread.join()
    worker_thread.join()


def test_callback_complete(c):
    cb_test = mock.Mock()
    c.set_complete_fn(cb_test)
    c.add_task("test_integration_echo", "Some string")
    worker_thread = multiprocessing.Process(target=thread_worker_echo)
    worker_thread.start()
    c.run_tasks()
    worker_thread.join()
    assert cb_test.called


def test_callback_created(c):
    cb_test = mock.Mock()
    c.set_created_fn(cb_test)
    c.add_task("test_integration_echo", "Some string")
    worker_thread = multiprocessing.Process(target=thread_worker_echo)
    worker_thread.start()
    c.run_tasks()
    worker_thread.join()
    assert cb_test.called


def thread_worker_data():
    def worker_fn_data(job):
        job.send_data("test_worker_data")

    worker = w()
    worker.add_function("test_integration_data", 0, worker_fn_data)
    sys.stderr.write("Worker starting...\n")
    try:
        while True:
            worker.work()
    except pygear.TIMEOUT:
        pass
    sys.stderr.write("Worker done\n")


def test_callback_data(c):
    cb_test = mock.Mock()
    c.set_data_fn(cb_test)
    c.add_task("test_integration_data", "Some string")
    worker_thread = multiprocessing.Process(target=thread_worker_data)
    worker_thread.start()
    c.run_tasks()
    worker_thread.join()
    assert cb_test.called


def thread_worker_except():
    def throw_exn(job):
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


def test_callback_exception(c):
    cb_test = mock.Mock()
    c.set_exception_fn(cb_test)
    c.add_task("test_integration_except", "Some string")
    worker_thread = multiprocessing.Process(target=thread_worker_except)
    worker_thread.start()
    c.run_tasks()
    worker_thread.join()
    assert cb_test.called


def thread_worker_fail():
    def worker_fn_fail(job):
        job.send_fail()

    worker = w()
    worker.add_function("test_integration_fail", 0, worker_fn_fail)
    sys.stderr.write("Worker starting...\n")
    try:
        while True:
            worker.work()
    except pygear.TIMEOUT:
        pass
    sys.stderr.write("Worker done\n")


def test_callback_fail(c):
    cb_test = mock.Mock()
    c.set_fail_fn(cb_test)
    c.add_task("test_integration_fail", "Some string")
    worker_thread = multiprocessing.Process(target=thread_worker_fail)
    worker_thread.start()
    c.run_tasks()
    worker_thread.join()
    assert cb_test.called


def thread_worker_cat_serializer():
    def worker_fn_data(job):
        jobdata = job.workload()
        assert jobdata == "meow"
        job.send_data("will be encoded to purr")

    worker = w()
    worker.set_serializer(cat_serializer())
    worker.add_function("test_integration_serializer", 0, worker_fn_data)
    sys.stderr.write("Worker starting...\n")
    try:
        while True:
            worker.work()
    except pygear.TIMEOUT:
        pass
    sys.stderr.write("Worker done\n")


def test_change_serializer(c):
    def expect_meow(t):
        assert t.result() == 'meow'

    def fail_test(*args):
        assert False

    c.set_complete_fn(expect_meow)
    c.set_exception_fn(fail_test)
    c.set_serializer(cat_serializer())
    worker_thread = multiprocessing.Process(target=thread_worker_cat_serializer)
    worker_thread.start()
    c.add_task("test_integration_serializer", "Woof")
    c.run_tasks()
    worker_thread.join()
