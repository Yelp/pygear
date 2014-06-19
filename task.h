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
static PyObject* pygear_task_function_name(pygear_TaskObject* self, PyObject* args);
PyDoc_STRVAR(pygear_task_function_name_doc,
"Get function name associated with a task.");

static PyObject* pygear_task_unique(pygear_TaskObject* self, PyObject* args);
PyDoc_STRVAR(pygear_task_unique_doc,
"");
static PyObject* pygear_task_job_handle(pygear_TaskObject* self, PyObject* args);
PyDoc_STRVAR(pygear_task_job_handle_doc,
"");
static PyObject* pygear_task_is_known(pygear_TaskObject* self, PyObject* args);
PyDoc_STRVAR(pygear_task_is_known_doc,
"");
static PyObject* pygear_task_is_running(pygear_TaskObject* self, PyObject* args);
PyDoc_STRVAR(pygear_task_is_running_doc,
"");
static PyObject* pygear_task_numerator(pygear_TaskObject* self, PyObject* args);
PyDoc_STRVAR(pygear_task_numerator_doc,
"");
static PyObject* pygear_task_denominator(pygear_TaskObject* self, PyObject* args);
PyDoc_STRVAR(pygear_task_denominator_doc,
"");

/* Module method specification */
static PyMethodDef task_module_methods[] = {
    _TASKMETHOD(function_name, METH_NOARGS)
    _TASKMETHOD(unique, METH_NOARGS)
    _TASKMETHOD(job_handle, METH_NOARGS)
    _TASKMETHOD(is_known, METH_NOARGS)
    _TASKMETHOD(is_running, METH_NOARGS)
    _TASKMETHOD(numerator, METH_NOARGS)
    _TASKMETHOD(denominator, METH_NOARGS)
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
