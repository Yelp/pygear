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

#ifndef WORKER_H
#define WORKER_H

#define _WORKERMETHOD(name,flags) {#name,(PyCFunction) pygear_worker_##name,flags,pygear_worker_##name##_doc},

typedef struct {
    PyObject_HEAD
    struct gearman_worker_st* g_Worker;
    PyObject* g_FunctionMap;
    PyObject* serializer;
} pygear_WorkerObject;

PyDoc_STRVAR(worker_module_docstring, 
"Represents a Gearman worker.\n"
"Pygear wraps libgearman C/C++ client library with minimal modifications.\n"
"See http://gearman.info/libgearman/ for details.");

/* Class init methods */
PyObject* Worker_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int Worker_init(pygear_WorkerObject *self, PyObject *args, PyObject *kwds);
void Worker_dealloc(pygear_WorkerObject* self);

/* Private methods */
void* _pygear_worker_function_mapper(gearman_job_st* gear_job, void* context,
    size_t* result_size, gearman_return_t* ret_ptr);

/* Method definitions */
static PyObject* pygear_worker_add_function(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_add_function_doc,
"Register and add callback function for worker. To remove functions that have\n"
"been added, call 'unregister' or 'unregister_all'.\n\n"
"@param[in] function_name - Function name to register.\n"
"@param[in] timeout - Timeout (in seconds) that specifies the maximum time a\n"
"\tjob should execute. A value of 0 means infinite time.\n"
"@param[in] function - Function (that takes a Job instance) to run.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.\n\n"
"Example:\n"
"def reverse(job):\n"
"    return job.workload()[::-1]\n\n"
"w.add_function('reverse', 1, reverse)  # 1 second timeout");


static PyObject* pygear_worker_add_server(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_add_server_doc,
"Add a job server to a worker. This goes into a list of servers that can be\n"
"used to run tasks. No socket I/O happens here, it is just added to a list.\n\n"
"@param[in] host - Hostname or IP address (IPv4 or IPv6) of the server to add.\n"
"@param[in] port - Port of the server to add.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.");


static PyObject* pygear_worker_add_servers(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_add_servers_doc,
"Add a list of job servers to a worker.\n\n"
"@param[in] servers_list - A list of servers each in the format of 'HOST[:PORT]'.\n"
"\tHOST can be a hostname or an IP address.\n"
"\tPORT is default as 4730 if not set.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.\n\n"
"Example:\n"
"servers_list = ['localhost234', 'jobserver2.domain.com:7003', '10.0.0.3']\n"
"w.add_servers(servers_list)");


static PyObject* pygear_worker_clone(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_clone_doc,
"Clone a pygear worker.\n\n"
"@return new Worker instance.");


static PyObject* pygear_worker_echo(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_echo_doc,
"Send a message to all servers to see if they echo it back. This is for\n"
"testing the connection to the servers.\n\n"
"@param[in] workload - Workload to ask the server to echo back.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.");


static PyObject* pygear_worker_errno(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_errno_doc,
"Report on the last error code that the worker reported/stored.\n"
"Use 'set_log_fn' if you are interested in recording all errors.\n\n"
"@return integer.");


static PyObject* pygear_worker_error(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_error_doc,
"Report on the last errors that the worker reported/stored.\n"
"Use 'set_log_fn' if you are interested in recording all errors.\n\n"
"@return string.");


static PyObject* pygear_worker_function_exists(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_function_exists_doc,
"See if a function exists in the server. It will return False\n"
"if the function is currently being de-allocated.\n\n"
"@param[in] function_name - Function name for search.\n\n"
"@return bool.");


static PyObject* pygear_worker_get_options(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_get_options_doc,
"Get options for a worker.\n\n"
"@return dictionary of options currently set on the worker.");


static PyObject* pygear_worker_grab_job(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_grab_job_doc,
"Takes a job from one of the job servers. The caller is responsible for\n"
"freeing the job once it is done. This does not used the callback interface,\n"
"which means result must be sent back to the job server manually. This\n"
"interface is used in testing, and is rarely the one to program against.\n\n"
"@return new Job object on success.\n"
"@return NULL on failure.");


static PyObject* pygear_worker_id(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_id_doc,
"Get the ID of the worker.\n\n"
"@return integer.");


static PyObject* pygear_worker_job_free_all(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_job_free_all_doc,
"Free all jobs for this worker.");


static PyObject* pygear_worker_namespace(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_namespace_doc,
"Get the current namespace of the worker. Only clients and workers sharing\n"
"a same namespace can see one another's functions and workloads. Use\n"
"'set_namespace' to set a namespace.\n\n"
"@return string.\n"
"@return None if no namespace has been set.");


static PyObject* pygear_worker_register(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_register_doc,
"Register function with job servers with an optional timeout. The timeout\n"
"specifies how many seconds the server will wait before marking a job as\n"
"failed.\n\n"
"@param[in] function_name - Function name to register.\n"
"@param[in] timeout - Optional timeout (in seconds) that specifies the maximum\n"
"\ttime a job should execute. A value of 0 or not given means no timeout.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.");


static PyObject* pygear_worker_remove_servers(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_remove_servers_doc,
"Remove all servers currently associated with the worker.");


static PyObject* pygear_worker_set_identifier(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_set_identifier_doc,
"Set the identifier that the server uses to identify the worker.\n\n"
"@param[in] identifier - New identifier string for the worker.\n\n"
"@return None on success.\n"
"@return NULL on failure."
);


static PyObject* pygear_worker_set_log_fn(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_set_log_fn_doc,
"Set logging function for this worker.\n\n"
"@param[in] function - Function to call when there is a logging message.\n"
"@param[in] context - Arguments to pass into the callback function.\n"
"@param[in] verbose - Verbosity level threshold. Only call function\n"
"\twhen the logging message is equal to or less verbose than this.\n"
"\tMust be one of the pygear.PYGEAR_VERBOSE_* constants.\n\n"
"@return None on success.\n"
"@return NULL on failure.");


static PyObject* pygear_worker_set_namespace(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_set_namespace_doc,
"Set a namespace for this worker. Only clients and workers sharing a same\n"
"namespace can see one another's workloads and functions.\n\n"
"@param[in] namespace - A namespace string to assign the worker to.\n\n"
"@return None on success.\n"
"@return NULL on failure.");


static PyObject* pygear_worker_set_options(pygear_WorkerObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_worker_set_options_doc,
"Set options for a worker.\n\n"
"@param[in] options - Dictionary of options to set on the worker.");


static PyObject* pygear_worker_set_serializer(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_set_serializer_doc,
"Specify the object to be used to serialize data passed through gearman.\n"
"By default, pygear will use 'json' to convert data to a string\n"
"representation during transit and reconstitute it on the other end.\n"
"You can replace the serializer with your own as long as it implements\n"
"the 'dumps' and 'loads' methods. 'dumps' must return a string, and loads\n"
"must take a string.\n\n"
"@param[in] serializer - Object implementing dumps and loads.");


static PyObject* pygear_worker_set_timeout(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_set_timeout_doc,
"Set the current timeout value, in milliseconds, for the worker.\n\n"
"@param[in] timeout - Timeout in milliseconds.");


static PyObject* pygear_worker_timeout(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_timeout_doc,
"Get the current timeout value, in milliseconds, for the worker.\n\n"
"@return integer.");


static PyObject* pygear_worker_wait(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_wait_doc,
"When in non-blocking I/O mode, wait for activity from one of the servers.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.");


static PyObject* pygear_worker_work(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_work_doc,
"Wait for a job and call the appropriate function when it gets one.\n"
"Note that this may run for an indefinite time and blocks KeyboardInterrupt\n"
"from the python interpreter. Call 'set_timeout' beforehand to avoid this.\n\n"
"@raises pygear exception on failure.\n");


static PyObject* pygear_worker_unregister(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_unregister_doc,
"Unregister a function with job servers.\n\n"
"@param[in] function_name - Function name to unregister.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.");


static PyObject* pygear_worker_unregister_all(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_unregister_all_doc,
"Unregister all functions with job servers.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.");


/* Module method specification */
static PyMethodDef worker_module_methods[] = {
    _WORKERMETHOD(clone,            METH_NOARGS)
    _WORKERMETHOD(error,            METH_NOARGS)
    _WORKERMETHOD(errno,            METH_NOARGS)
    _WORKERMETHOD(set_options,      METH_KEYWORDS)
    _WORKERMETHOD(get_options,      METH_NOARGS)
    _WORKERMETHOD(timeout,          METH_NOARGS)
    _WORKERMETHOD(set_timeout,      METH_VARARGS)
    _WORKERMETHOD(add_server,       METH_VARARGS)
    _WORKERMETHOD(add_servers,      METH_VARARGS)
    _WORKERMETHOD(remove_servers,   METH_NOARGS)
    _WORKERMETHOD(wait,             METH_NOARGS)
    _WORKERMETHOD(register,         METH_VARARGS)
    _WORKERMETHOD(unregister,       METH_VARARGS)
    _WORKERMETHOD(unregister_all,   METH_NOARGS)
    _WORKERMETHOD(grab_job,         METH_NOARGS)
    _WORKERMETHOD(job_free_all,     METH_NOARGS)
    _WORKERMETHOD(function_exists,  METH_VARARGS)
    _WORKERMETHOD(add_function,     METH_VARARGS)
    _WORKERMETHOD(work,             METH_NOARGS)
    _WORKERMETHOD(echo,             METH_VARARGS)
    _WORKERMETHOD(id,               METH_NOARGS)
    _WORKERMETHOD(set_identifier,   METH_VARARGS)
    _WORKERMETHOD(set_namespace,    METH_VARARGS)
    _WORKERMETHOD(namespace,        METH_NOARGS)
    _WORKERMETHOD(set_log_fn,       METH_VARARGS)
    _WORKERMETHOD(set_serializer,   METH_VARARGS)
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
