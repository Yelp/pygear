import datetime
import time
import sys
from optparse import OptionParser

import pygear


def reverse(job):
    print '[Pygear Worker] Reverse: ' + job.workload()['msg']
    return job.workload()["msg"][::-1]


def noop(job):
    print '[Pygear Worker] No-op' + job.workload()['msg']
    return job.workload()['msg']


def log_func(line):
    print line


def parse_options_and_args():
    usage = 'usage: %prog [options] arg'
    parser = OptionParser(usage)
    parser.add_option('-s', '--seconds', type='int', dest='seconds', default=10,
        help='set the seconds this worker will run (default 10)')
    parser.add_option('-p', '--port', type='int', dest='server_port', default=4730,
        help='set server port (default 4730)')
    parser.add_option('-H', '--host', dest='server_host', default='localhost',
        help='set server host (default \'localhost\')')
    parser.add_option('-l', '--logging', action='store_true', dest='logging', default=False,
        help='turn on set_log_fn to print the exception lines')
    return parser.parse_args()


def main():
    [opt, args] = parse_options_and_args()

    w = pygear.Worker()
    w.add_server(opt.server_host, opt.server_port)
    w.add_function('reverse', 0, reverse)
    w.add_function('noop', 0, noop)

    print pygear.__file__
    start_datetime = datetime.datetime.now()
    stop_time = time.time() + opt.seconds

    if opt.logging:
        w.set_log_fn(log_func, pygear.PYGEAR_VERBOSE_INFO)

    while time.time() < stop_time:
        try:
            w.set_timeout(1000) # yield every second
            w.work()
        except pygear.TIMEOUT:
            print 'Server:%s:%r Since:%s %d seconds left.' \
                % (opt.server_host, opt.server_port, start_datetime, stop_time - time.time())
        except:
            print sys.exc_info()[0]

    print 'End working'


if __name__ == '__main__':
    main()
