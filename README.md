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
    w.work()


**Blocking Client:**

    import pygear
    c = pygear.Client()
    c.add_server('localhost', 4730)
    print c.do('reverse', 'Hello python!')

**Asynchronous Client:**

    import pygear

    def oncomplete_callback(task):
        print task.result()

    c = pygear.Client()
    c.add_server('localhost', 4730)
    c.set_complete_fn(oncomplete_callback)
    c.add_task('reverse', 'Hello python!')
    c.run_tasks()

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
