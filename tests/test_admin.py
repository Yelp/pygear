import pygear
import pytest

from . import TEST_SERVER_VERSION
from sandbox import gearmand_sandbox


class TestPygearAdminCommands(object):

    @pytest.yield_fixture(autouse=True)
    def setup_sandbox(self):
        with gearmand_sandbox() as sb:
            if not sb['success']:
                raise Exception("Gearmand sandbox setup error.")
            self.sb_host = sb['host']
            self.sb_port = sb['port']
            self.sb_pid = sb['pid']
            self.admin = pygear.Admin(self.sb_host, self.sb_port)
            yield

    def test_defaults_from_new(self):
        admin = pygear.Admin.__new__(pygear.Admin)
        assert admin.info()['host'] is None
        assert admin.info()['port'] == 4730
        assert admin.info()['timeout'] == 60

    def test_admin_status(self):
        assert self.admin.status() == []

    def test_admin_clone(self):
        ac = self.admin.clone()
        assert ac.info() == self.admin.info()

    def test_admin_maxqueue(self):
        self.admin.create_function('foo')
        self.admin.maxqueue('foo', 100)  # set max queue size for foo as 100

    def test_admin_set_server(self):
        self.admin.set_server('host', 1234)
        assert self.admin.info()['host'] == 'host'
        assert self.admin.info()['port'] == 1234

    def test_admin_set_timeout(self):
        assert self.admin.info()['timeout'] == 60  # default timeout
        self.admin.set_timeout(10)
        assert self.admin.info()['timeout'] == 10

    def test_admin_shutdown(self):
        self.admin.shutdown(False)

    def test_admin_shutdown_gracefully(self):
        self.admin.shutdown(True)

    def test_version(self):
        assert self.admin.version() == TEST_SERVER_VERSION

    def test_admin_workers(self):
        self.admin.workers()

    def test_admin_cancel_job(self):
        # unknown command in 0.24 gearmand; needs to be tested on newer gearmand or removed
        pass

    def test_admin_create_and_drop_function(self):
        assert self.admin.status() == []
        self.admin.create_function('foo')
        status = self.admin.status()
        assert len(status) == 1
        assert status[0] == {
            'function': 'foo',
            'available_workers': 0,
            'running': 0,
            'total': 0
        }
        self.admin.drop_function('foo')
        assert self.admin.status() == []

    def test_admin_getpid(self):
        assert self.admin.getpid() == self.sb_pid

    def test_admin_show_jobs(self):
        # unknown command in 0.24 gearmand; needs to be tested on newer gearmand or removed
        pass

    def test_admin_show_unique_jobs(self):
        # unknown command in 0.24 gearmand; needs to be tested on newer gearmand or removed
        pass

    def test_admin_verbose(self):
        assert self.admin.verbose() in ['FATAL', 'ERROR', 'INFO', 'DEBUG']
