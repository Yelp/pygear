TEST_SERVER_HOST = 'srv1-devc'
TEST_SERVER_PORT = 4730
TEST_SERVER_VERSION = '0.24'
LOCALHOST = 'localhost'
CLIENT_TIMEOUT_MSEC = 1000
WORKER_TIMEOUT_MSEC = 500
MAX_RETRIES = 5
RETRY_WAIT_MSEC = 500


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
