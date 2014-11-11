import pytest
import pygear

from . import TEST_SERVER_HOST
from . import TEST_SERVER_PORT
from . import TEST_SERVER_VERSION


@pytest.fixture
def a():
    return pygear.Admin(TEST_SERVER_HOST, TEST_SERVER_PORT)


def test_admin_cancel_job(a):
    pass


def test_admin_clone(a):
    pass


def test_admin_create_function(a):
    pass


def test_admin_drop_function(a):
    pass


def test_admin_getpid(a):
    # ok
    pass


def test_admin_maxqueue(a):
    pass


def test_admin_set_server(a):
    pass


def test_admin_set_timeout(a):
    pass


def test_admin_show_jobs(a):
    pass


def test_admin_show_unique_jobs(a):
    pass


def test_admin_shutdown(a):
    pass


def test_admin_status(a):
    # ok
    pass


def test_admin_verbose(a):
    # ok
    pass


def test_admin_version(a):
    assert a.version() == TEST_SERVER_VERSION


def test_admin_workers(a):
    pass
