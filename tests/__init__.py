TEST_SERVER_HOST = 'srv1-devc'
TEST_SERVER_PORT = 4730
TEST_SERVER_VERSION = '0.24'
LOCALHOST = 'localhost'
TEST_TIMEOUT_MSEC = 2000


class noop_serializer(object):
    def dumps(self, s):
        return s

    def loads(self, s):
        return s


class cat_serializer(object):
    def dumps(self, s):
        return 'purr'

    def loads(self, s):
        return 'meow'


def echo_function(job):
    return job.workload()


def reverse_function(job):
    workload = job.workload()
    job.send_complete(workload[::-1])
