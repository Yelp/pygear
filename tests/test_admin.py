import pygear
import testifycompat as T

from sandbox import gearmand_sandbox


class TestPygearAdminBasicCommands(object):

    @T.setup_teardown
    def setup_sandbox(self):
        with gearmand_sandbox() as port:
            self.admin = pygear.Admin('localhost', port)
            yield

    def test_admin_status(self):
        self.admin.status()

    def test_admin_clone(self):
        ac = self.admin.clone()
        assert ac.info() == self.admin.info()

    def test_admin_maxqueue(self):
        pass

    def test_admin_set_server(self):
        self.admin.set_server('host', 1234)
        assert self.admin.info()['host'] == 'host'
        assert self.admin.info()['port'] == 1234

    def test_admin_set_timeout(self):
        assert self.admin.info()['timeout'] == 60  # default timeout
        self.admin.set_timeout(10)
        assert self.admin.info()['timeout'] == 10

    def test_admin_shutdown(self):
        self.admin.shutdown(0)

    def test_admin_shutdown_gracefully(self):
        self.admin.shutdown(1)

    def test_version(self):
        assert type(self.admin.version()) is str

    def test_admin_workers(self):
        self.admin.workers()


# TODO: the following are only supported on latest gearmand server
class TestPygearAdminExtensionCommands(object):

    @T.setup_teardown
    def setup_sandbox(self):
        with gearmand_sandbox() as port:
            self.admin = pygear.Admin('localhost', port)
            yield

    def test_admin_cancel_job(self):
        pass

    def test_admin_create_function(self):
        pass

    def test_admin_drop_function(self):
        pass

    def test_admin_getpid(self):
        pass

    def test_admin_show_jobs(self):
        pass

    def test_admin_show_unique_jobs(self):
        pass

    def test_admin_verbose(self):
        pass
