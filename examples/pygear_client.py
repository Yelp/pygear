import datetime
import os
from optparse import OptionParser
import time

import pygear


def oncomplete_callback(task):
    print 'task completed!!'
    print task.result()


def parse_options_and_args():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-s", "--seconds", type="int", dest="seconds", default=0,
        help="set the seconds this client will submits tasks continuously")
    parser.add_option("-n", "--num-of-tasks", type="int", dest="num_of_tasks", default=0,
        help="set the number of tasks this client submits")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False,
        help="shush the result from the worker")
    parser.add_option("-b", "--background", action="store_true", dest="background", default=False,
        help="send background jobs (do_background)")
    parser.add_option("-a", "--asynchronous", action="store_true", dest="async", default=False,
        help="send asynchronous jobs (add_task/add_task_background + run_tasks)")
    parser.add_option("-o", "--output", dest="output", default=None,
        help="append 'second, num_of_tasks' to a specified filename")
    parser.add_option("-p", "--port", type="int", dest="server_port", default=4730,
        help="set server port (default 4730)")
    parser.add_option("-t", "--timeout", type="int", dest="timeout", default=5,
        help="set the seconds this client will run (default 5)")
    parser.add_option("-H", "--host", dest="server_host", default='localhost',
        help="set server host (default 'localhost')")
    return parser.parse_args()


def main():
    [opt, args] = parse_options_and_args()

    c = pygear.Client()
    c.add_server(opt.server_host, opt.server_port)
    c.set_complete_fn(oncomplete_callback)

    print pygear.__file__
    start_datetime = datetime.datetime.now()
    stop_time = time.time() + opt.seconds
    print 'Server:%s:%r Since:%s %d seconds left.' \
        % (opt.server_host, opt.server_port, start_datetime, stop_time - time.time())

    if not opt.background:
        print 'Foreground jobs - waiting results from server.'
        timeout_millis = opt.timeout * 1000
        c.set_timeout(timeout_millis)

    data = {}
    n = 0
    if opt.seconds > 0:
        while time.time() < stop_time:
            data['msg'] = 'Hello second ' + repr(n)
            unique_key = os.urandom(16).encode('hex')

            if opt.async:
                if opt.background:
                    c.add_task_background('reverse', data, unique_key)
                else:
                    c.add_task('reverse', data, unique_key)
            else:
                if opt.background:
                    print c.do_background('reverse', data, unique_key)
                else:
                    res = c.do('reverse', data, unique_key)
                    if not opt.quiet:
                        print res
            n += 1

    elif opt.num_of_tasks > 0:
        n = opt.num_of_tasks
        for i in range(n):
            data['msg'] = 'Hello tasks ' + repr(i)
            unique_key = os.urandom(16).encode('hex')

            if opt.async:
                if opt.background:
                    c.add_task_background('reverse', data, unique_key)
                else:
                    c.add_task('reverse', data, unique_key)
            else:
                if opt.background:
                    print c.do_background('reverse', data, unique_key)
                else:
                    res = c.do('reverse', data, unique_key)
                    if not opt.quiet:
                        print res
    else:
        print 'Number of seconds/tasks unspecified.'

    if opt.async:
        print 'Run all tasks in the client queue.'
        c.run_tasks()

    print '[PyGear Client] Total %s tasks processed.' % n
    if opt.background:
        print '(Background tasks)'
    if opt.async:
        print '(Asynchronous tasks)'
    if opt.output:
        f = open(opt.output, 'a')
        res = repr(opt.seconds) + ',' + repr(n) + '\n'
        f.write(res)
        f.close()


if __name__ == '__main__':
    main()
