#include <Python.h>
#include <libgearman-1.0/gearman.h>
#include <stdio.h>
#include "structmember.h"

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

#ifndef WORKER_H
#define WORKER_H

#define _WORKERMETHOD(name,flags) {#name,(PyCFunction) pygear_worker_##name,flags,pygear_worker_##name##_doc},

typedef struct {
    PyObject_HEAD
    struct gearman_worker_st* g_Worker;
} pygear_WorkerObject;

PyDoc_STRVAR(worker_module_docstring, "Represents a Gearman worker");

/* Class init methods */
PyObject* Worker_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int Worker_init(pygear_WorkerObject *self, PyObject *args, PyObject *kwds);
void Worker_dealloc(pygear_WorkerObject* self);

/* Method definitions */

/* Module method specification */
static PyMethodDef worker_module_methods[] = {
    {NULL, NULL, 0, NULL}
};

PyTypeObject pygear_WorkerType = {
    PyObject_HEAD_INIT(NULL)
    0,                                          /*ob_size*/
    "pygear.Worker",                            /*tp_name*/
    sizeof(pygear_WorkerObject),                /*tp_basicsize*/
    0,                                          /*tp_itemsize*/
    (destructor)Worker_dealloc,                 /*tp_dealloc*/
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
    worker_module_docstring,                    /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    worker_module_methods,                      /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)Worker_init,                      /* tp_init */
    0,                                          /* tp_alloc */
    Worker_new,                                 /* tp_new */
};

#endif
