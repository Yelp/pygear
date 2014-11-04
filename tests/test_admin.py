import pytest
import pygear

from . import TEST_GEARMAN_SERVER
from . import TEST_GEARMAN_PORT


@pytest.fixture
def a():
    return pygear.Admin(TEST_GEARMAN_SERVER, TEST_GEARMAN_PORT)


def test_admin_cancel_job(a):
    pass


def test_admin_clone(a):
    pass


def test_admin_create_function(a):
    pass


def test_admin_drop_function(a):
    pass


def test_admin_getpid(a):
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
    pass


def test_admin_verbose(a):
    pass


def test_admin_version(a):
    pass


def test_admin_workers(a):
    pass
