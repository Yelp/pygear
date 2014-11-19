# -*- coding: utf-8 -*-
"""
This module provides a context manager for writing acceptance tests
again any code which uses gearmand.

Example usage:

.. code-block:: python

    with gearmand_sandbox() as port:
        client = pygear.Client()
        client.add_server('localhost', port)
        client.do(...)
"""
import contextlib
import shutil
import socket
import subprocess
from subprocess import PIPE
import tempfile
import time


def pick_unused_port(host):
    """ Return a port that is currently unused.

    There is a race condition here in that the returned port can be grabbed
    by any other process. This should only be used for testing.
    """
    with contextlib.closing(
        socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ) as s:
        s.bind((host, 0))
        _, port = s.getsockname()
        return port


@contextlib.contextmanager
def gearmand_sandbox(*args, **kwargs):
    """Run a gearmand instance in a subprocess and yield its information

    :param wait_on_start: seconds to wait for gearmand to start (default 0.5)
    """
    host = 'localhost'
    port = pick_unused_port(host)
    cmd = ['gearmand', '-p', str(port)]
    proc = subprocess.Popen(cmd + list(args), stderr=PIPE, stdout=PIPE)
    time.sleep(kwargs.pop('wait_on_start', 0.5))
    try:
        yield {'host': host, 'port': port, 'pid': proc.pid}
    finally:
        proc.kill()
        print '\n'.join(proc.communicate())


@contextlib.contextmanager
def temp_dir():
    path = tempfile.mkdtemp()
    try:
        yield path
    finally:
        shutil.rmtree(path)
