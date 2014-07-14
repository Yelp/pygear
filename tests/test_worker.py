import pytest
import pygear

@pytest.fixture
def w():
    return pygear.Worker()

def worker_rev_fn(job):
    workload = job.workload()
    job.send_complete(workload[::-1])

def test_work_no_functions(w):
    with pytest.raises(pygear.NO_REGISTERED_FUNCTIONS):
        w.work()

def test_work_timeout(w):
    w.add_function("reverse", 60, worker_rev_fn)
    w.add_server("localhost", 4730)
    w.set_timeout(30)
    with pytest.raises(pygear.TIMEOUT):
        w.work()

def test_worker_timeout(w):
    assert w.timeout() == -1
    w.set_timeout(30)
    assert w.timeout() == 30

def test_worker_function_exists(w):
    assert w.function_exists("test_method") == False
    w.register("test_method", 10)
    assert w.function_exists("test_method") == True

def test_worker_unregister(w):
    assert w.function_exists("test_method") == False
    w.register("test_method", 10)
    assert w.function_exists("test_method") == True
    w.unregister("test_method")
    assert w.function_exists("test_method") == False

def test_worker_unregister_all(w):
    assert w.function_exists("test_method") == False
    w.register("test_method", 10)
    assert w.function_exists("test_method") == True
    w.unregister_all()
    assert w.function_exists("test_method") == False

def test_worker_add_servers(w):
    with pytest.raises(pygear.GETADDRINFO):
        w.add_servers(["invalidhosturi"])

def test_clone(w):
    w.set_timeout(30)
    wl = w.clone()
    assert wl.timeout() == 30
    assert wl.get_options() == w.get_options()

def test_invalid_serializer(w):
    with pytest.raises(AttributeError):
        w.set_serializer("a string doesn't implement loads.")

class noop_serializer(object):
    def loads(self, s):
        return s

    def dumps(self, s):
        return s

def test_valid_serializer(w):
    w.set_serializer(noop_serializer())
