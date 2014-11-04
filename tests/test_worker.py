import pytest
import pygear
from . import TEST_GEARMAN_SERVER, TEST_GEARMAN_PORT


@pytest.fixture
def w():
    return pygear.Worker()


def noop_function(job):
    return job


def reverse_function(job):
    workload = job.workload()
    job.send_complete(workload[::-1])


def test_worker_add_function_and_function_exists(w):
    assert not w.function_exists("noop_function")
    w.add_function("noop_function", 10, noop_function)
    assert w.function_exists("noop_function")


def test_worker_add_server(w):
    w.add_server(TEST_GEARMAN_SERVER, TEST_GEARMAN_PORT)
    with pytest.raises(pygear.GETADDRINFO):
        w.add_server('invalid host name', -1)


def test_worker_add_servers(w):
    w.add_servers(['localhost:4730'])
    with pytest.raises(pygear.GETADDRINFO):
        w.add_servers(["invalidhosturi"])


def test_worker_clone(w):
    w.set_timeout(30)
    wc = w.clone()
    assert wc.timeout() == 30
    assert wc.get_options() == w.get_options()


def test_worker_echo(w):
    # test_integration.py
    pass


def test_worker_function_exists(w):
    assert not w.function_exists("test_method")
    w.register("test_method", 10)
    assert w.function_exists("test_method")


def test_worker_get_and_set_options(w):
    # get_options, set_options
    pass


def test_worker_grab_job(w):
    pass


def test_worker_set_identifier(w):
    w.set_identifier('cool')


def test_worker_job_free_all(w):
    pass


def test_worker_namespace(w):
    pass


def test_worker_register(w):
    pass


def test_worker_remove_servers(w):
    pass


def test_worker_set_log_fn(w):
    pass


def test_worker_set_namespace(w):
    pass


def test_worker_wait(w):
    pass


def test_worker_work(w):
    pass


def test_work_no_functions(w):
    w.add_server(TEST_GEARMAN_SERVER, TEST_GEARMAN_PORT)
    with pytest.raises(pygear.NO_REGISTERED_FUNCTIONS):
        w.work()


class noop_serializer(object):
    def loads(self, s):
        return s

    def dumps(self, s):
        return s


def test_set_serializer(w):
    w.set_serializer(noop_serializer())
    with pytest.raises(AttributeError):
        w.set_serializer("a string doesn't implement loads.")


def test_worker_set_timeout(w):
    assert w.timeout() == 10000
    w.set_timeout(30)
    assert w.timeout() == 30


def test_work_timeout(w):
    w.add_function("test_method", 60, reverse_function)
    w.add_server(TEST_GEARMAN_SERVER, TEST_GEARMAN_PORT)
    w.set_timeout(30)
    with pytest.raises(pygear.TIMEOUT):
        w.work()


def test_worker_unregister(w):
    assert not w.function_exists("test_method")
    w.register("test_method", 10)
    assert w.function_exists("test_method")
    w.unregister("test_method")
    assert not w.function_exists("test_method")


def test_worker_unregister_all(w):
    assert not w.function_exists("test_method")
    w.register("test_method", 10)
    assert w.function_exists("test_method")
    w.unregister_all()
    assert not w.function_exists("test_method")


def test_worker_misc(w):
    w.id()
    w.error()
    w.errno()
