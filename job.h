#include <Python.h>
#include <libgearman-1.0/gearman.h>
#include <stdio.h>
#include "structmember.h"

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

#ifndef JOB_H
#define JOB_H

#define _JOBMETHOD(name,flags) {#name,(PyCFunction) pygear_job_##name,flags,pygear_job_##name##_doc},

typedef struct {
    PyObject_HEAD
    struct gearman_job_st* g_Job;
} pygear_JobObject;

PyDoc_STRVAR(job_module_docstring, "Represents a Gearman job");

/* Class init methods */
PyObject* Job_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int Job_init(pygear_JobObject *self, PyObject *args, PyObject *kwds);
void Job_dealloc(pygear_JobObject* self);

/* Method definitions */

/* Module method specification */
static PyMethodDef job_module_methods[] = {
    {NULL, NULL, 0, NULL}
};

PyTypeObject pygear_JobType = {
    PyObject_HEAD_INIT(NULL)
    0,                                          /*ob_size*/
    "pygear.Job",                               /*tp_name*/
    sizeof(pygear_JobObject),                   /*tp_basicsize*/
    0,                                          /*tp_itemsize*/
    (destructor)Job_dealloc,                    /*tp_dealloc*/
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
    job_module_docstring,                       /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    job_module_methods,                         /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)Job_init,                         /* tp_init */
    0,                                          /* tp_alloc */
    Job_new,                                    /* tp_new */
};

#endif
