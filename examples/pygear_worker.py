import datetime
import time
import sys

import pygear

from util import parse_options_and_args
from util import test_reverse
from util import test_echo
from util import test_job_status
from util import logging


FUNCTION_TIMEOUT = 0


def main():
    [opt, args] = parse_options_and_args('worker')

    w = pygear.Worker()
    w.add_server(opt.server_host, opt.server_port)
    w.add_function('test_reverse', FUNCTION_TIMEOUT, test_reverse)
    w.add_function('test_echo', FUNCTION_TIMEOUT, test_echo)
    w.add_function('test_job_status', FUNCTION_TIMEOUT, test_job_status)

    print pygear.__file__
    start_datetime = datetime.datetime.now()
    stop_time = time.time() + opt.seconds

    if opt.logging:
        w.set_log_fn(logging, pygear.PYGEAR_VERBOSE_INFO)

    while time.time() < stop_time:
        try:
            w.set_timeout(1000)  # yield every second
            w.work()
        except pygear.TIMEOUT:
            print 'Server:%s:%r Since:%s %d seconds left.' \
                % (opt.server_host, opt.server_port, start_datetime, stop_time - time.time())
        except:
            print sys.exc_info()[0]

    print 'End working'


if __name__ == '__main__':
    main()
