import gc

import mock
import pytest
import pygear

from . import noop_serializer


@pytest.fixture
def t():
    return pygear.Task(None, None)


def test_task_data_size(t):
    assert t.data_size() == 0


def test_task_denominator(t):
    assert t.denominator() == 0


def test_task_error(t):
    assert t.error() is None


def test_task_function_name(t):
    assert t.function_name() is None


def test_task_is_known(t):
    assert not t.is_known()


def test_task_is_running(t):
    assert not t.is_running()


def test_task_job_handle(t):
    assert t.job_handle() is None


def test_task_numerator(t):
    assert t.numerator() == 0


def test_task_result(t):
    assert t.result() is None


def test_task_returncode(t):
    assert pygear.describe_returncode(t.returncode()) == 'INVALID_ARGUMENT'


def test_task_set_serializer(t):
    t.set_serializer(noop_serializer())  # valid
    with pytest.raises(AttributeError):  # invalid
        t.set_serializer("a string doesn't implement loads.")


def test_task_strstate(t):
    assert t.strstate() is None


def test_task_unique(t):
    assert t.unique() is None


def test_gc_traversal(t):
    sentinel = mock.Mock()
    t.set_serializer(sentinel)
    assert sentinel in gc.get_referents(t)
