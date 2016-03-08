/*
 *
 * Copyright (c) 2014, Yelp Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Yelp Inc. nor the
 *     names of its contributors may be used to endorse or promote products
 *     derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL YELP INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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
    PyObject* serializer;
} pygear_TaskObject;

PyDoc_STRVAR(task_module_docstring, "Represents a Gearman task");

/* Class init methods */
int Task_init(pygear_TaskObject *self, PyObject *args, PyObject *kwds);
int Task_traverse(pygear_TaskObject *self,  visitproc visit, void *arg);
int Task_clear(pygear_TaskObject* self);
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

static PyObject* pygear_task_error(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_error_doc,
"Get the last error message encountered for a task.");

static PyObject* pygear_task_returncode(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_returncode_doc,
"Get the last return code stored for a task.");

static PyObject* pygear_task_strstate(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_strstate_doc,
"Get a string representation of the state of a task.");

static PyObject* pygear_task_result(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_result_doc,
"Get the data returned by a completed task as a string");

static PyObject* pygear_task_data_size(pygear_TaskObject* self);
PyDoc_STRVAR(pygear_task_data_size_doc,
"Get the size of the data for a completed task in bytes");

static PyObject* pygear_task_set_serializer(pygear_TaskObject* self, PyObject* args);
PyDoc_STRVAR(pygear_task_set_serializer_doc,
"Specify the object to be used to serialize data passed through gearman.\n"
"By default, pygear will use 'simplejson' to convert data to a string\n"
"representation during transit and reconstitute it on the other end.\n"
"You can replace the serializer with your own as long as it implements\n"
"the 'dumps' and 'loads' methods. 'dumps' must return a string, and loads\n"
"must take a string.\n"
"@param[in] serializer Object implementing dumps and loads");

/* Module method specification */
static PyMethodDef task_module_methods[] = {
    _TASKMETHOD(function_name, METH_NOARGS)
    _TASKMETHOD(unique, METH_NOARGS)
    _TASKMETHOD(job_handle, METH_NOARGS)
    _TASKMETHOD(is_known, METH_NOARGS)
    _TASKMETHOD(is_running, METH_NOARGS)
    _TASKMETHOD(numerator, METH_NOARGS)
    _TASKMETHOD(denominator, METH_NOARGS)
    _TASKMETHOD(error, METH_NOARGS)
    _TASKMETHOD(returncode, METH_NOARGS)
    _TASKMETHOD(strstate, METH_NOARGS)
    _TASKMETHOD(result, METH_NOARGS)
    _TASKMETHOD(data_size, METH_NOARGS)
    _TASKMETHOD(set_serializer, METH_VARARGS)
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
    Py_TPFLAGS_DEFAULT |
    Py_TPFLAGS_BASETYPE |
    Py_TPFLAGS_HAVE_GC,                         /*tp_flags*/
    task_module_docstring,                      /* tp_doc */
    (traverseproc)Task_traverse,                /* tp_traverse */
    (inquiry)Task_clear,                        /* tp_clear */
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
};

#endif
