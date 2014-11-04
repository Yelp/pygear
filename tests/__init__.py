TEST_GEARMAN_SERVERS = ['srv1-devc:4730']
TEST_GEARMAN_SERVER = 'localhost'
TEST_GEARMAN_PORT = 4730
TEST_TIMEOUT_MSEC = 1000  # one second


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
