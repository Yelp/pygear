from optparse import OptionParser
import time


def parser_base():
    usage = 'usage: %prog [options] arg'
    parser = OptionParser(usage)
    parser.add_option('-p', '--port', type='int', dest='server_port', default=4730,
                      help='set server port (default 4730)')
    parser.add_option('-H', '--host', dest='server_host', default='localhost',
                      help='set server host (default \'localhost\')')
    return parser


def parse_options_and_args(my_type):
    parser = parser_base()

    if my_type == 'worker':
        parser.add_option('-s', '--seconds', type='int', dest='seconds', default=10,
                          help='set the seconds this worker will run (default 10)')
        parser.add_option('-l', '--logging', action='store_true', dest='logging', default=False,
                          help='turn on set_log_fn to print the exception lines')

    elif my_type == 'client':
        parser.add_option("-s", "--seconds", type="int", dest="seconds", default=0,
            help="set the seconds this client will submits tasks continuously")
        parser.add_option("-n", "--num-of-tasks", type="int", dest="num_of_tasks", default=0,
            help="set the number of tasks this client submits")
        parser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False,
            help="shush the result from the worker")
        parser.add_option("-b", "--background", action="store_true", dest="background",
            default=False,
            help="send background jobs (do_background)")
        parser.add_option("-a", "--asynchronous", action="store_true", dest="async", default=False,
            help="send asynchronous jobs (add_task/add_task_background + run_tasks)")
        parser.add_option("-o", "--output", dest="output", default=None,
            help="append 'second, num_of_tasks' to a specified filename")
        parser.add_option("-t", "--timeout", type="int", dest="timeout", default=0,
            help="set the seconds this client will run (default no timeout)")
        parser.add_option("--shimgear", action="store_true", dest="", default=0,
            help="set the seconds this client will run (default no timeout)")

    return parser.parse_args()


def test_reverse(job):
    print "Test reverse - %s %s" % (job.handle(), job.workload())
    return job.workload()[::-1]


def test_echo(job):
    print "Test echo - %s %s" % (job.handle(), job.workload())
    return job.workload()


def test_job_status(job):
    print 'Test job status - %s %s' % (job.handle(), job.workload())
    job.send_status(0, 100)  # 0 %
    time.sleep(1)
    job.send_status(50,100)  # 50 %
    time.sleep(1)
    job.send_status(100, 100)  # 100 %
    return job.workload()


def logging(line):
    print line


def oncomplete_callback(task):
    print 'task completed: ', task.result()


def check_result(r):
    print "Job unique:%s handle:%s state:%s result:%s" \
        % (r.job.unique, r.job.handle, r.state, r.result)
