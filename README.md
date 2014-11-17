# pygear
#### A python Gearman client that wraps [libgearman](http://gearman.info/libgearman/)

## About

`pygear` is a python module that exposes Gearman worker and client
functionality with an interface that closely mirrors that of
[libgearman](http://gearman.info/libgearman/) with minor modifications
to make it more python-friendly.

The intention with pygear is to provide a simpler implementation than
[python-gearman](https://github.com/Yelp/python-gearman) so that
changes made to libgearman upstream can be added quickly, and to avoid
the overhead of reimplementing the entire gearman client protocol.

This package has been built against **libgearman 1.0.6**, and as the
interface seems to change fairly rapidly may not compile against other
versions of libgearman without some modification.

## Installing

`pygear` requires that you have the development headers for libgearman,
python and GCC installed. It may be necessary to modify the include
paths in setup.py to match your exact configuration.

- `make debug` Produces a local version of pygear.so
- `make dist` Generates a python wheel and tar package
- `make install` Directly installs the module
- `make test` Run tests against the module. Requires tox and pytest.

## Notes

As of version 0.4, the default internal serializer for pygear is JSON
instead of pickle. In order to send complex objects in pygear jobs, it
is now necessary to specify your own serializer using the .set_serializer
method on the Client / Worker. The parameter to set_serializer must be
an object that implements the loads(string) and dumps(object) methods.

Since Python signal handlers can only occur between the "atomic" instructions
of the Python interpreter, signals arriving during the execution of
libgearman maybe delayed for an arbitrary amount of time. In the worst case,
libgearman hangs and the users are not able to terminate the program using
Ctrl-C (KeyboardInterrupt). Thus, it is highly recommended that pygear
users explicitly call .set_timeout for both workers and blocking clients.


## Examples

### Reverse

**Worker:**

    import pygear

    def reverse(job):
        workload = job.workload()
        return workload[::-1]

    w = pygear.Worker()
    w.add_server('localhost', 4730)
    w.add_function("reverse", 0, reverse)

    while True:
        try:
            w.set_timeout(1000)  # yield
            w.work()
        except pygear.TIMEOUT:
            pass


**Blocking Client:**

    import pygear

    c = pygear.Client()
    c.add_server('localhost', 4730)

    # submit foreground job to server and wait for the result
    try:
        c.set_timeout(5000)  # wait at most 5 seconds
        result = c.do('reverse', 'Hello pygear!')
        print result
    except pygear.TIMEOUT:
        print 'Wait for too long!'


**Non-blocking Client:**

    import pygear
    
    c = pygear.Client()
    c.add_server('localhost', 4730)

    # submit background job to server and return immediately
    res = c.do_background('reverse', 'Hello pygear!')
    result = res['result']  # NULL if submission fails; None if success
    job_handle = res['job_handle']

    # check job status if needed
    print c.job_status(job_handle)


**Asynchronous Client:**

    import pygear

    def oncomplete_callback(task):
        print task.result()

    c = pygear.Client()
    c.add_server('localhost', 4730)
    c.set_complete_fn(oncomplete_callback)

    # add tasks to local queue
    c.add_task('reverse', 'Hello pygear task 1!')
    c.add_task('reverse', 'Hello pygear task 2!')

    # background jobs will not trigger oncomplete_callback function
    c.add_task_background('reverse', 'Hello pygear task 3!')
    ...

    # submit to server and run tasks
    c.run_tasks()

### Admin Client

The gearman job server supports a text-based protocol to pull information and
to run administrative tasks. Arguments of the commands are separated by
whitespace. Commands are terminated with a CRLF('\r\n'), although just an
LF('\n') will be accepted as well.
See [protocol](https://github.com/paiweilai1984/gearman-packet/blob/master/PROTOCOL.md)
for details of latest protocols.

The following basic commands are supported:
- workers
- status
- maxqueue [function_name] [max_queue_size]
- shutdown [graceful]
- version

The following commands are supported in C++ Gearmand Extension (undocumented):
- getpid
- verbose
- cancel job [job_id]
- create function [function_name]
- drop function [function_name]
- show jobs
- show unique jobs

### Failure Handling

**Worker:**

    import pygear

    def reverse(job):
       raise Exception("I can't spell backwards!")

    w = pygear.Worker()
    w.add_server('localhost', 4730)
    w.add_function("reverse", 0, reverse)
    w.work()


**Client:**

    import pygear

    def onexn_callback(task):
        print task.result()

    c = pygear.Client()
    c.add_server('localhost', 4730)
    c.set_exception_fn(onexn_callback)
    c.add_task('reverse', 'Hello python!')
    c.run_tasks()
