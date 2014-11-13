import datetime
import time
import uuid

import pygear
print pygear.__file__

from util import parse_options_and_args
from util import oncomplete_callback


def main():
    [opts, args] = parse_options_and_args('client')

    c = pygear.Client()
    c.add_server(opts.server_host, opts.server_port)
    c.set_complete_fn(oncomplete_callback)  # complete callback for foreground job

    start_datetime = datetime.datetime.now()
    stop_time = time.time() + opts.seconds
    print 'Server:%s:%r Since:%s %d seconds left.' \
        % (opts.server_host, opts.server_port, start_datetime, stop_time - time.time())

    if not opts.background:
        print 'Foreground jobs - waiting results from server.'
        if opts.timeout > 0:
            timeout_millis = opts.timeout * 1000
            c.set_timeout(timeout_millis)

    n = 0
    if opts.seconds > 0:
        while time.time() < stop_time:
            data = 'Hello %r' % n
            unique = uuid.uuid4().hex
            if opts.async:
                if opts.background:
                    res = c.add_task_background('reverse', data, unique)
                else:
                    res = c.add_task('reverse', data, unique)
            else:
                if opts.background:
                    res = c.do_background('reverse', data, unique)
                else:
                    res = c.do('reverse', data, unique)
            if not opts.quiet:
                print res
            n += 1

    elif opts.num_of_tasks > 0:
        n = opts.num_of_tasks
        for i in range(n):
            data = 'Hello %r' % i
            unique = uuid.uuid4().hex
            if opts.async:
                if opts.background:
                    res = c.add_task_background('reverse', data, unique)
                else:
                    res = c.add_task('reverse', data, unique)
            else:
                if opts.background:
                    res = c.do_background('reverse', data, unique)
                else:
                    res = c.do('reverse', data, unique)
            if not opts.quiet:
                print res
    else:
        print 'Number of seconds or tasks unspecified. No job was sent.'

    if opts.async:
        print 'Run all tasks from the client queue together.'
        c.run_tasks()

    print '[PyGear Client] Total %s tasks processed.' % n
    if opts.background:
        print '(Background tasks)'
    if opts.async:
        print '(Asynchronous tasks)'
    if opts.output:
        f = open(opts.output, 'a')
        res = '%r, %r\n' % (opts.seconds, n)
        f.write(res)
        f.close()


if __name__ == '__main__':
    main()
