#include <Python.h>
#include <libgearman-1.0/gearman.h>
#include <stdio.h>
#include "structmember.h"

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

#ifndef TASK_H
#define TASK_H

#define _TASKMETHOD(name,flags) {#name,(PyCFunction) pygear_task_##name,flags,pygear_task_##name##_doc},

/*
 * pygear_task_st is a union that can be cast to and used as a gearman_task_st
 * within the actual gearman libraries, but also has optional pygear state.
 * Using pygear_task_st is necessary to store Python callback information, since
 * the libgearman callback functions require the function contract
 *      gearman_return_t (func)(gearman_task_st *task);
 * In order to be able to pass callbacks on to Python, we store function objects
 * and have wrapper callback functions that re-wrap the gearman_task_st in a
 * Python Task object and then call the appropriate function with the Task as
 * an argument.
 */

typedef union {
    gearman_task_st task;
    struct {
        gearman_task_st task;
        PyObject* cb_workload;
        PyObject* cb_created;
        PyObject* cb_data;
        PyObject* cb_warning;
        PyObject* cb_status;
        PyObject* cb_complete;
        PyObject* cb_exception;
        PyObject* cb_fail;
    } extended_task;
} pygear_task_st;

typedef struct {
    PyObject_HEAD
    struct gearman_task_st* g_Task;
} pygear_TaskObject;

PyDoc_STRVAR(task_module_docstring, "Represents a Gearman task");

/* Class init methods */
PyObject* Task_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int Task_init(pygear_TaskObject *self, PyObject *args, PyObject *kwds);
void Task_dealloc(pygear_TaskObject* self);

/* Method definitions */
static PyObject* pygear_task_function_name(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_function_name_doc,
"Get function name associated with a task.");

static PyObject* pygear_task_unique(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_unique_doc,
"Get unique identifier for a task.");

static PyObject* pygear_task_job_handle(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_job_handle_doc,
"Get job handle for a task.");

static PyObject* pygear_task_is_known(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_is_known_doc,
"Get status on whether a task is known or not.");

static PyObject* pygear_task_is_running(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_is_running_doc,
"Get status on whether a task is running or not.");

static PyObject* pygear_task_numerator(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_numerator_doc,
"Get the numerator of percentage complete for a task.");

static PyObject* pygear_task_denominator(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_denominator_doc,
"Get the denominator of percentage complete for a task.");

static PyObject* pygear_task_returncode(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_returncode_doc,
"Get the return code for a task.");

static PyObject* pygear_task_strstate(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_strstate_doc,
"Get a string representation of the state of a task.");

static PyObject* pygear_task_result(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_result_doc,
"Get the data returned by a completed task as a string");

static PyObject* pygear_task_data_size(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_data_size_doc,
"Get the size of the data for a completed task in bytes");

/* Module method specification */
static PyMethodDef task_module_methods[] = {
    _TASKMETHOD(function_name, METH_NOARGS)
    _TASKMETHOD(unique, METH_NOARGS)
    _TASKMETHOD(job_handle, METH_NOARGS)
    _TASKMETHOD(is_known, METH_NOARGS)
    _TASKMETHOD(is_running, METH_NOARGS)
    _TASKMETHOD(numerator, METH_NOARGS)
    _TASKMETHOD(denominator, METH_NOARGS)
    _TASKMETHOD(returncode, METH_NOARGS)
    _TASKMETHOD(strstate, METH_NOARGS)
    _TASKMETHOD(result, METH_NOARGS)
    _TASKMETHOD(data_size, METH_NOARGS)
    {NULL, NULL, 0, NULL}
};

PyTypeObject pygear_TaskType = {
    PyObject_HEAD_INIT(NULL)
    0,                                          /*ob_size*/
    "pygear.Task",                              /*tp_name*/
    sizeof(pygear_TaskObject),                  /*tp_basicsize*/
    0,                                          /*tp_itemsize*/
    (destructor)Task_dealloc,                   /*tp_dealloc*/
    0,                                          /*tp_print*/
    0,                                          /*tp_getattr*/
    0,                                          /*tp_setattr*/
    0,                                          /*tp_compare*/
    0,                                          /*tp_repr*/
    0,                                          /*tp_as_number*/
    0,                                          /*tp_as_sequence*/
    0,                                          /*tp_as_mapping*/
    0,                                          /*tp_hash */
    0,                                          /*tp_call*/
    0,                                          /*tp_str*/
    0,                                          /*tp_getattro*/
    0,                                          /*tp_setattro*/
    0,                                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /*tp_flags*/
    task_module_docstring,                      /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    task_module_methods,                        /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)Task_init,                        /* tp_init */
    0,                                          /* tp_alloc */
    Task_new,                                   /* tp_new */
};

#endif
