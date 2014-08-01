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
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "structmember.h"
#include "exception.h"

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

#ifndef ADMIN_H
#define ADMIN_H

#define _ADMINMETHOD(name,flags) {#name,(PyCFunction) pygear_admin_##name,flags,pygear_admin_##name##_doc},

#define ADMIN_DEFAULT_TIMEOUT 10.0

typedef struct {
    PyObject_HEAD
    char* host;
    int port;
    double timeout;
    int sockfd;
} pygear_AdminObject;

PyDoc_STRVAR(admin_module_docstring, "Represents a Gearman administrative client");

/* Class init methods */
PyObject* Admin_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int Admin_init(pygear_AdminObject *self, PyObject *args, PyObject *kwds);
void Admin_dealloc(pygear_AdminObject* self);

/* Method definitions */
static PyObject* pygear_admin_clone(pygear_AdminObject* self);
PyDoc_STRVAR(pygear_admin_clone_doc,
"Clone a client structure.");

static PyObject* pygear_admin_set_server(pygear_AdminObject* self, PyObject* args);
PyDoc_STRVAR(pygear_admin_set_server_doc,
"Add a job server to a client. This goes into a list of servers that can be\n"
"used to run tasks. No socket I/O happens here, it is just added to a list.\n"
"@param[in] host Hostname or IP address (IPv4 or IPv6) of the server to add.\n"
"@param[in] port Port of the server to add.\n"
"@raises Pygear exception on failure.");

static PyObject* pygear_admin_status(pygear_AdminObject* self);
PyDoc_STRVAR(pygear_admin_status_doc,
"Get the status of the functions on the gearman server\n"
"Returns a list of dictionaries, each of which has four keys:\n"
"function: string: name of the queue\n"
"total: int: number of tasks currently in queue\n"
"running: int: number of tasks currently being run by workers\n"
"available_workers: number of workers available to process tasks in that queue");

static PyObject* pygear_admin_workers(pygear_AdminObject* self);
PyDoc_STRVAR(pygear_admin_workers_doc,
"Get the status of all attached workers");

static PyObject* pygear_admin_version(pygear_AdminObject* self);
PyDoc_STRVAR(pygear_admin_version_doc,
"Get the version number of the server");

static PyObject* pygear_admin_maxqueue(pygear_AdminObject* self, PyObject* args);
PyDoc_STRVAR(pygear_admin_maxqueue_doc,
"Set the max queue length on the server\n"
"@param[in] queuelen New max queue length for the gearman server");

static PyObject* pygear_admin_shutdown(pygear_AdminObject* self, PyObject* args);
PyDoc_STRVAR(pygear_admin_shutdown_doc,
"Shut down the gearman server"
"@param[in] graceful Optional boolean whether to shutdown gracefully");

static PyObject* pygear_admin_verbose(pygear_AdminObject* self);
PyDoc_STRVAR(pygear_admin_verbose_doc,
"Get the verbose level of the server");

static PyObject* pygear_admin_getpid(pygear_AdminObject* self);
PyDoc_STRVAR(pygear_admin_getpid_doc,
"Get the PID of the gearman server");

static PyObject* pygear_admin_drop_function(pygear_AdminObject* self, PyObject* args);
PyDoc_STRVAR(pygear_admin_drop_function_doc,
"Instruct the server to drop a function by name\n"
"@param[in] func_name Name of the function (queue) to drop");

static PyObject* pygear_admin_create_function(pygear_AdminObject* self, PyObject* args);
PyDoc_STRVAR(pygear_admin_create_function_doc,
"Instruct the server to create a function \n"
"@param[in] func_name Name of the function (queue) to create");

static PyObject* pygear_admin_set_timeout(pygear_AdminObject* self, PyObject* args);
PyDoc_STRVAR(pygear_admin_set_timeout_doc,
"Set the timeout (in seconds) for socket operations.\n"
"By default, the timeout is one minute.\n"
"@param[in] timeout Float number of seconds to wait");

static PyObject* pygear_admin_show_jobs(pygear_AdminObject* self);
PyDoc_STRVAR(pygear_admin_show_jobs_doc,
"Show jobs currently on the server.\n"
"Returns a list of dicts containing the following:\n"
"handle: The job handle for the job\n"
"retries: \n"
"ignore_job: \n"
"job_queued: ");

static PyObject* pygear_admin_show_unique_jobs(pygear_AdminObject* self);
PyDoc_STRVAR(pygear_admin_show_unique_jobs_doc,
"Get a list of Job unique IDs");

static PyObject* pygear_admin_cancel_job(pygear_AdminObject* self, PyObject* args);
PyDoc_STRVAR(pygear_admin_cancel_job_doc,
"Cancel a job by job handle.\n"
"@param[in] handle String handle of the job to cancel");

/* Module method specification */
static PyMethodDef admin_module_methods[] = {
    _ADMINMETHOD(clone,                    METH_NOARGS)
    _ADMINMETHOD(set_server,               METH_VARARGS)
    _ADMINMETHOD(status,                   METH_NOARGS)
    _ADMINMETHOD(workers,                  METH_NOARGS)
    _ADMINMETHOD(version,                  METH_NOARGS)
    _ADMINMETHOD(maxqueue,                 METH_VARARGS)
    _ADMINMETHOD(shutdown,                 METH_VARARGS)
    _ADMINMETHOD(verbose,                  METH_NOARGS)
    _ADMINMETHOD(getpid,                   METH_NOARGS)
    _ADMINMETHOD(drop_function,            METH_VARARGS)
    _ADMINMETHOD(create_function,          METH_VARARGS)
    _ADMINMETHOD(set_timeout,              METH_VARARGS)
    _ADMINMETHOD(show_jobs,                METH_NOARGS)
    _ADMINMETHOD(show_unique_jobs,         METH_NOARGS)
    _ADMINMETHOD(cancel_job,               METH_VARARGS)

    {NULL, NULL, 0, NULL}
};

PyTypeObject pygear_AdminType = {
    PyObject_HEAD_INIT(NULL)
    0,                                          /*ob_size*/
    "pygear.Admin",                            /*tp_name*/
    sizeof(pygear_AdminObject),                /*tp_basicsize*/
    0,                                          /*tp_itemsize*/
    (destructor)Admin_dealloc,                 /*tp_dealloc*/
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
    admin_module_docstring,                    /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    admin_module_methods,                      /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)Admin_init,                      /* tp_init */
    0,                                          /* tp_alloc */
    Admin_new,                                 /* tp_new */
};

#endif
