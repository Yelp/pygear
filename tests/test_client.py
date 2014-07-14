import pytest
import pygear

@pytest.fixture
def c():
    return pygear.Client()

def test_execute_no_servers(c):
    with pytest.raises(pygear.UNKNOWN_STATE):
        c.execute("reverse", "Jackdaws love my big sphynx of quartz")

def test_do_no_servers(c):
    with pytest.raises(pygear.NO_SERVERS):
        c.do("reverse", "Jackdaws love my big sphynx of quartz")

def test_add_task_no_servers(c):
    with pytest.raises(pygear.NO_SERVERS):
        c.add_task("reverse", "Jackdaws love my big sphynx of quartz")
        c.run_tasks()

def test_client_timeout(c):
    assert c.timeout() == -1
    c.set_timeout(30)
    assert c.timeout() == 30

def test_client_add_servers(c):
    with pytest.raises(pygear.GETADDRINFO):
        c.add_servers(["invalidhosturi"])
    with pytest.raises(pygear.GETADDRINFO):
        c.add_server("invalidhosturi", 4730)

def test_clone(c):
    c.set_timeout(30)
    cl = c.clone()
    assert cl.timeout() == 30
    assert cl.get_options() == c.get_options()

def test_invalid_serializer(c):
    with pytest.raises(AttributeError):
        c.set_serializer("a string doesn't implement loads.")

class noop_serializer(object):
    def loads(self, s):
        return s

    def dumps(self, s):
        return s

def test_valid_serializer(c):
    c.set_serializer(noop_serializer())
