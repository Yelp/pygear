import datetime
import os
from optparse import OptionParser
import sys
import time

import pygear


def oncomplete_callback(task):
    print 'task completed!!'
    print task.result()


def main():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-s", "--seconds", type="int", dest="seconds",
        default=0,
        help="set the number of seconds this client submits tasks")
    parser.add_option("-n", "--num-of-tasks", type="int", dest="num_of_tasks",
        default=0,
        help="set the number of tasks this client submits")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet",
        default=False,
        help="show result from worker")
    parser.add_option("-b", "--background", action="store_true", dest="background",
        default=False,
        help="send background jobs")
    parser.add_option("-a", "--asynchronous", action="store_true", dest="async",
        default=False,
        help="send asynchronous jobs")
    parser.add_option("-o", "--output", dest="output",
        default=None,
        help="append 'second,num_of_tasks' to a specified filename")
    parser.add_option("-p", "--port", type="int", dest="server_port",
        default=4730,
        help="set server port, default is 4730")
    [options, args] = parser.parse_args()

    c = pygear.Client()
    c.add_server('localhost', options.server_port)
    c.set_complete_fn(oncomplete_callback)

    print pygear.__file__
    start_datetime = datetime.datetime.now()
    stop_time = time.time() + options.seconds
    print 'Server:%s:%r Since:%s %d seconds left.' \
        % ('localhost', options.server_port, start_datetime, stop_time - time.time())

    if not options.background:
        print 'Foreground jobs - waiting results from server.'
#        timeout_millis = options.seconds * 1000
#        c.set_timeout(timeout_millis)

    data = {}
    n = 0
    if options.seconds > 0:
        while time.time() < stop_time:
            data['msg'] = 'Hello second ' + repr(n)
            unique_key = os.urandom(16).encode('hex')

            if options.async:
                if options.background:
                    c.add_task_background('reverse', data, unique_key)
                else:
                    c.add_task('reverse', data, unique_key)
            else:
                if options.background:
                    print c.do_background('reverse', data, unique_key)
                else:
                    res = c.do('reverse', data, unique_key)
                    if not options.quiet:
                        print res
            n += 1

    elif options.num_of_tasks > 0:
        n = options.num_of_tasks
        for i in range(n):
            data['msg'] = 'Hello tasks ' + repr(i)
            unique_key = os.urandom(16).encode('hex')

            if options.async:
                if options.background:
                    c.add_task_background('reverse', data, unique_key)
                else:
                    c.add_task('reverse', data, unique_key)
            else:
                if options.background:
                    print c.do_background('reverse', data, unique_key)
                else:
                    res = c.do('reverse', data, unique_key)
                    if not options.quiet:
                        print res
    else:
        print 'Number of seconds/tasks unspecified.'

    if options.async:
        print 'Run all tasks in the client queue.'
        c.run_tasks()

    print '[PyGear Client] Total %s tasks processed.' % n
    if options.background:
        print '(Background tasks)'
    if options.async:
        print '(Asynchronous tasks)'
    if options.output:
        f = open(options.output, 'a')
        res = repr(options.seconds) + ',' + repr(n) + '\n'
        f.write(res)
        f.close()


if __name__ == '__main__':
    main()
