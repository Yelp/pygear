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


def main():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-s", "--seconds", type="int", dest="seconds",
        default=10,
        help="set the number of seconds this worker runs")
    parser.add_option("-p", "--port", type="int", dest="server_port",
        default=4730,
        help="set server port, default is 4730")
    [options, args] = parser.parse_args()

    w = pygear.Worker()
    w.add_server('localhost', options.server_port)
    w.add_function('reverse', 0, reverse)
    w.add_function('noop', 0, noop)

    print pygear.__file__
    start_datetime = datetime.datetime.now()
    stop_time = time.time() + options.seconds

    while time.time() < stop_time:
        try:
            w.set_timeout(1000)  # yield
            w.work()
        except pygear.TIMEOUT:
            print 'Server:%s:%r Since:%s %d seconds left.' \
                % ('localhost', options.server_port, start_datetime, stop_time - time.time())
        except:
            print sys.exec_info()[0]

    print 'End working'


if __name__ == '__main__':
    main()
