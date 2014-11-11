import pygear
print pygear.__file__

from util import parse_options_and_args


def main():

#    [opts, args] = parse_options_and_args('client')

    a = pygear.Admin('localhost', 49180)
#    print 'workers', a.workers()
#    print 'maxqueue', a.maxqueue()


    # OK
    # print 'version', a.version()
    # print 'getpid', a.getpid()
    # print 'verbose', a.verbose()
    # print 'status', a.status()


if __name__ == '__main__':
    main()
