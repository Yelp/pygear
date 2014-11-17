import pytest
import pygear

import testifycompat as T
from sandbox import gearmand_sandbox
from . import TEST_SERVER_HOST
from . import TEST_SERVER_PORT
from . import TEST_SERVER_VERSION


@pytest.fixture
def a():
    return pygear.Admin(TEST_SERVER_HOST, TEST_SERVER_PORT)

class TestPygearAdminClient(object):

    @T.setup_teardown
    def setup_sandbox(self):
        with gearmand_sandbox() as port:
            self.admin = pygear.Admin('localhost', port)
            yield

    def test_admin_cancel_job(self):
        pass

    def test_admin_clone(self):
        ac = self.admin.clone()
        assert ac.version() == TEST_SERVER_VERSION

    def test_admin_create_function(self):
        pass

    def test_admin_drop_function(self):
        pass

    def test_admin_getpid(self):
        pass

    def test_admin_maxqueue(self):
        pass

    def test_admin_set_server(self):
        pass

    def test_admin_set_timeout(self):
        pass

    def test_admin_show_jobs(self):
        pass

    def test_admin_show_unique_jobs(self):
        pass

    def test_admin_shutdown(self):
        pass

    def test_admin_status(self):
#        self.admin_client.status()
        pass

    def test_admin_verbose(self):
#        self.admin_client.verbose()
        pass

    def test_version(self):
        assert self.admin.version() == TEST_SERVER_VERSION

    def test_admin_workers(self):
#        self.admin_client.workers()
        pass
