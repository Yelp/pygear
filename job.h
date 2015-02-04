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
#include "worker.h"

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

#ifndef JOB_H
#define JOB_H

#define _JOBMETHOD(name,flags) {#name,(PyCFunction) pygear_job_##name,flags,pygear_job_##name##_doc},

typedef struct {
    PyObject_HEAD
    struct gearman_job_st* g_Job;
    PyObject* serializer;
} pygear_JobObject;

PyDoc_STRVAR(job_module_docstring, "Represents a Gearman job");

/* Class init methods */
int Job_init(pygear_JobObject *self, PyObject *args, PyObject *kwds);
int Job_traverse(pygear_JobObject *self,  visitproc visit, void *arg);
int Job_clear(pygear_JobObject* self);
void Job_dealloc(pygear_JobObject* self);


/* Method definitions */
static PyObject* pygear_job_send_data(pygear_JobObject* self, PyObject* args);
PyDoc_STRVAR(pygear_job_send_data_doc,
"Send data for a running job.\n"
"@param[in] data Data to send.");

static PyObject* pygear_job_send_warning(pygear_JobObject* self, PyObject* args);
PyDoc_STRVAR(pygear_job_send_warning_doc,
" Send warning for a running job."
"@param[in] warning Warning to send");

static PyObject* pygear_job_send_status(pygear_JobObject* self, PyObject* args);
PyDoc_STRVAR(pygear_job_send_status_doc,
"Send status information for a running job."
"@param[in] information Information to send");

static PyObject* pygear_job_send_complete(pygear_JobObject* self, PyObject* args);
PyDoc_STRVAR(pygear_job_send_complete_doc,
"Send result and complete status for a job."
"@param[in] result Result to send");

static PyObject* pygear_job_send_exception(pygear_JobObject* self, PyObject* args);
PyDoc_STRVAR(pygear_job_send_exception_doc,
"Send exception for a running job."
"@param[in] exception Exception to send");

static PyObject* pygear_job_send_fail(pygear_JobObject* self);
PyDoc_STRVAR(pygear_job_send_fail_doc,
"Send fail status for a job.");

static PyObject* pygear_job_handle(pygear_JobObject* self);
PyDoc_STRVAR(pygear_job_handle_doc,
"Get job handle.");

static PyObject* pygear_job_function_name(pygear_JobObject* self);
PyDoc_STRVAR(pygear_job_function_name_doc,
"Get the function name associated with a job.");

static PyObject* pygear_job_unique(pygear_JobObject* self);
PyDoc_STRVAR(pygear_job_unique_doc,
"Get a pointer to the workload for a job.");

static PyObject* pygear_job_workload(pygear_JobObject* self);
PyDoc_STRVAR(pygear_job_workload_doc,
"Get the workload for a job.");

static PyObject* pygear_job_workload_size(pygear_JobObject* self);
PyDoc_STRVAR(pygear_job_workload_size_doc,
"Get size of the workload for a job.");

static PyObject* pygear_job_error(pygear_JobObject* self);
PyDoc_STRVAR(pygear_job_error_doc,
"Get a string representation of the last job error");

static PyObject* pygear_job_set_serializer(pygear_JobObject* self, PyObject* args);
PyDoc_STRVAR(pygear_job_set_serializer_doc,
"Specify the object to be used to serialize data passed through gearman.\n"
"By default, pygear will use pickle or cPickle to convert data to a string\n"
"representation during transit and reconstitute it on the other end.\n"
"You can replace the serializer with your own as long as it implements\n"
"the 'dumps' and 'loads' methods. 'dumps' must return a string, and loads\n"
"must take a string.\n"
"@param[in] serializer Object implementing dumps and loads");

/* Module method specification */
static PyMethodDef job_module_methods[] = {
     _JOBMETHOD(send_data,          METH_VARARGS)
     _JOBMETHOD(send_warning,       METH_VARARGS)
     _JOBMETHOD(send_status,        METH_VARARGS)
     _JOBMETHOD(send_complete,      METH_VARARGS)
     _JOBMETHOD(send_exception,     METH_VARARGS)
     _JOBMETHOD(send_fail,          METH_NOARGS)
     _JOBMETHOD(handle,             METH_NOARGS)
     _JOBMETHOD(function_name,      METH_NOARGS)
     _JOBMETHOD(unique,             METH_NOARGS)
     _JOBMETHOD(workload,           METH_NOARGS)
     _JOBMETHOD(workload_size,      METH_NOARGS)
     _JOBMETHOD(error,              METH_NOARGS)
     _JOBMETHOD(set_serializer,     METH_VARARGS)
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
    Py_TPFLAGS_DEFAULT |
    Py_TPFLAGS_BASETYPE |
    Py_TPFLAGS_HAVE_GC,                         /*tp_flags*/
    job_module_docstring,                       /* tp_doc */
    (traverseproc)Job_traverse,                 /* tp_traverse */
    (inquiry)Job_clear,                         /* tp_clear */
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
};

#endif
