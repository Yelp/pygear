import pytest
import pygear

from . import cat_serializer
from . import noop_serializer


@pytest.fixture
def c():
    return pygear.Client()


def test_client_add_server(c):
    c.add_server('localhost', 4730)  # valid
    with pytest.raises(pygear.GETADDRINFO):  # invalid
        c.add_server("invalidhosturi", -1)


def test_client_add_servers(c):
    c.add_servers(['localhost:4730', 'srv1-devc', '192.168.0.1', '192.168.0.2:1234'])  # valid
    with pytest.raises(pygear.GETADDRINFO):  # invalid
        c.add_servers(["invalidhosturi"])


def test_client_add_task(c):
    # add task without server
    with pytest.raises(pygear.NO_SERVERS):
        c.add_task("reverse", "Jackdaws love my big sphynx of quartz")
        c.run_tasks()


def test_client_add_task_background(c):
    pass


def test_client_add_task_high(c):
    pass


def test_client_add_task_high_background(c):
    pass


def test_client_add_task_low(c):
    pass


def test_client_add_task_low_background(c):
    pass


def test_client_add_task_status(c):
    pass


def test_client_clear_fn(c):
    pass


def test_client_clone(c):
    c.set_timeout(30)
    cl = c.clone()
    assert cl.timeout() == 30
    assert cl.get_options() == c.get_options()


def test_client_do(c):
    # do without server
    with pytest.raises(pygear.NO_SERVERS):
        c.do("reverse", "Jackdaws love my big sphynx of quartz")
    # see test_integration.py for valid case


def test_client_do_background(c):
    pass


def test_client_do_high(c):
    pass


def test_client_do_high_background(c):
    pass


def test_client_do_job_handle(c):
    pass


def test_client_do_low(c):
    pass


def test_client_do_low_background(c):
    pass


def test_client_do_status(c):
    pass


def test_client_echo(c):
    pass


def test_client_execute(c):
    # execute without server
    with pytest.raises(pygear.UNKNOWN_STATE):
        c.execute("reverse", "Jackdaws love my big sphynx of quartz")


def test_client_get_options(c):
    pass


def test_client_job_status(c):
    pass


def test_client_remove_servers(c):
    pass


def test_client_run_tasks(c):
    pass


def test_client_set_complete_fn(c):
    # see test_integration.py
    pass


def test_client_set_created_fn(c):
    # see test_integration.py
    pass


def test_client_set_data_fn(c):
    # see test_integration.py
    pass


def test_client_set_exception_fn(c):
    # see test_integration.py
    pass


def test_client_set_fail_fn(c):
    # see test_integration.py
    pass


def test_client_set_log_fn(c):
    pass


def test_client_set_options(c):
    pass


def test_client_set_serializer(c):
    c.set_serializer(noop_serializer())  # valid
    with pytest.raises(AttributeError):  # invalid
        c.set_serializer("a string doesn't implement loads.")


def test_client_set_status_fn(c):
    pass


def test_client_set_and_get_timeout(c):
    assert c.timeout() == -1
    c.set_timeout(30)
    assert c.timeout() == 30


def test_client_set_warning_fn(c):
    pass


def test_client_set_workload_fn(c):
    pass


def test_client_unique_status(c):
    pass


def test_client_wait(c):
    pass


def test_client_error(c):
    c.errno()
    c.error()
    c.error_code()
