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
#include "task.h"
#include "exception.h"

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

#ifndef CLIENT_H
#define CLIENT_H

#define _CLIENTMETHOD(name,flags) {#name,(PyCFunction) pygear_client_##name,flags,pygear_client_##name##_doc},

typedef struct {
    PyObject_HEAD
    struct gearman_client_st* g_Client;
    PyObject* cb_workload;
    PyObject* cb_created;
    PyObject* cb_data;
    PyObject* cb_warning;
    PyObject* cb_status;
    PyObject* cb_complete;
    PyObject* cb_exception;
    PyObject* cb_fail;
    PyObject* cb_log;
    PyObject* serializer;
} pygear_ClientObject;

PyDoc_STRVAR(client_module_docstring, "Represents a Gearman client.");

/* Class init methods */
int Client_init(pygear_ClientObject *self, PyObject *args, PyObject *kwds);
void Client_dealloc(pygear_ClientObject* self);


/* Method definitions */
static PyObject* pygear_client_add_server(pygear_ClientObject *self, PyObject *args);
PyDoc_STRVAR(pygear_client_add_server_doc,
"Add a job server to a client. This goes into a list of servers that can be\n"
"used to run tasks. No socket I/O happens here, it is just added to a list.\n\n"
"@param[in] host - Hostname or IP address (IPv4 or IPv6) of the server to add.\n"
"@param[in] port - Port of the server to add.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.\n\n"
"Example:\n"
"c.add_servers('localhost', 4730)");

static PyObject* pygear_client_add_servers(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_add_servers_doc,
"Add a list of job servers to a client.\n\n"
"@param[in] servers_list - A list of servers each in the format of 'HOST[:PORT]'.\n"
"\tHOST can be a hostname or an IP address.\n"
"\tPORT is default as 4730 if not set.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.\n\n"
"Example:\n"
"servers_list = ['localhost234', 'jobserver2.domain.com:7003', '10.0.0.3']\n"
"c.add_servers(servers_list)");

static PyObject* pygear_client_add_task(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_add_task_doc,
"Add a foreground task to be run in parallel. This task is locally queued and will only be\n"
"sent to job server when 'run_tasks' is called. The client will wait for the result from\n"
"the server during 'run_tasks'.\n"
"@param[in] function_name - The name of the function to run.\n"
"@param[in] workload - The workload to pass to the function when it is run.\n"
"@param[in] unique - Optional unique job identifier, or None for a new UUID.\n\n"
"@return new Task instance on success.\n"
"@return NULL and raises pygear exception on failure.");

static PyObject* pygear_client_add_task_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_add_task_background_doc,
"Add a background task to be run in parallel. This task is locally queued and will only be\n"
"sent to job server when 'run_tasks' is called. The client will return immediately without\n"
"waiting for the result of the task during 'run_tasks'.\n\n"
"@param[in] function_name - The name of the function to run.\n"
"@param[in] workload - The workload to pass to the function when it is run.\n"
"@param[in] unique - Optional unique job identifier, or None for a new UUID.\n\n"
"@return new Task instance on success.\n"
"@return NULL and raises pygear exception on failure.");

static PyObject* pygear_client_add_task_high(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_add_task_high_doc,
"Add a high priority foreground task to be run in parallel.\n"
"See 'add_task' for details.");

static PyObject* pygear_client_add_task_high_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_add_task_high_background_doc,
"Add a high priority background task to be run in parallel.\n"
"See 'add_task_background' for details.");

static PyObject* pygear_client_add_task_low(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_add_task_low_doc,
"Add a low priority foreground task to be run in parallel.\n"
"See 'add_task' for details.");

static PyObject* pygear_client_add_task_low_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_add_task_low_background_doc,
"Add a low priority background task to be run in parallel.\n"
"See 'add_task_background' for details.");

static PyObject* pygear_client_add_task_status(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_add_task_status_doc,
"Add a task to get the status for a backgound task in parallel.\n\n"
"@param[in] job_handle - The job handle of the background task.\n\n"
"@return new Task instance on success.\n"
"@return NULL and raises pygear exception on failure.\n");

static PyObject* pygear_client_clear_fn(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_clear_fn_doc,
"Clear all task callback functions.");

static PyObject* pygear_client_clone(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_clone_doc,
"Clone a pygear client.\n\n"
"@return new Client instance.");

static PyObject* pygear_client_do(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_do_doc,
"Send a foreground task to server immediately and wait for its result (blocking).\n\n"
"@param[in] function_name - The name of the function to run.\n"
"@param[in] unique - Optional unique job identifier, or None for a new UUID.\n"
"@param[in] workload - The workload to pass to the function when it is run.\n\n"
"@return the result of the task (None if empty result) on success.\n"
"@return NULL and raises pygear exception on failure.\n\n"
"Note: If the exception is one of GEARMAN_WORK_DATA, GEARMAN_WORK_WARNING,\n"
"or GEARMAN_WORK_STATUS, the caller should take actions to handle the event\n"
"and call this function again. This may happen multiple times until a\n"
"GEARMAN_WORK_ERROR, GEARMAN_WORK_FAIL, or GEARMAN_SUCCESS (work complete)\n"
"is returned. For GEARMAN_WORK_DATA or GEARMAN_WORK_WARNING, the result size\n"
"will be set to the intermediate data chunk being returned and an allocated\n"
"data buffer will be returned. For GEARMAN_WORK_STATUS, the caller can use\n"
"'do_status' to get the current task status.");

static PyObject* pygear_client_do_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_do_background_doc,
"Send a background task to server and return immediately without waiting for\n"
"the result (non-blocking).\n\n"
"@param[in] function_name - The name of the function to run.\n"
"@param[in] unique - Optional unique job identifier, or None for a new UUID.\n"
"@param[in] workload - The workload to pass to the function when it is run.\n\n"
"@return job_handle (string) of the task on success.\n"
"@return NULL and raises pygear exception on failure.\n"
"See 'do' for handling different exceptions.");

static PyObject* pygear_client_do_high(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_do_high_doc,
"Run a high priority foreground task and return the result.\n"
"See 'do' for parameters and return information.");

static PyObject* pygear_client_do_high_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_do_high_background_doc,
"Run a high priority background task and return the job handle.\n"
"See 'do_background' for parameters and return information.");

static PyObject* pygear_client_do_low(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_do_low_doc,
"Run a low priority foreground task and return the result.\n"
"See 'do' for parameters and return information.");

static PyObject* pygear_client_do_low_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_do_low_background_doc,
"Run a low priority background task and return the job handle.\n"
"See 'do_background' for parameters and return information.");

static PyObject* pygear_client_do_job_handle(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_do_job_handle_doc,
"Get the job handle for the running task. This should be used between\n"
"repeated gearman_client_do() (and related) calls to get information.");

static PyObject* pygear_client_do_status(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_do_status_doc,
"Get the completion progress of a task.\n"
"Returns a tuple (numerator, denominator)");

static PyObject* pygear_client_echo(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_echo_doc,
"Send a message to all servers to see if they echo it back. This is for\n"
"testing the connection to the servers.\n\n"
"@param[in] workload - Workload to ask the server to echo back.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.");

static PyObject* pygear_client_errno(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_errno_doc,
"Report on the last error code that the client reported/stored.\n"
"Use 'set_log_fn' if you are interested in recording all errors.\n\n"
"@return integer.\n"
"See gearman_errno() for details.");

static PyObject* pygear_client_error(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_error_doc,
"Report on the last errors that the client reported/stored.\n"
"Use 'set_log_fn' if you are interested in recording all errors.\n\n"
"@return string.\n"
"See gearman_error() for details.");

static PyObject* pygear_client_error_code(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_error_code_doc,
"See gearman_error_code() for details.");

static PyObject* pygear_client_execute(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_execute_doc,
"Run a task immediately and wait for the return.\n\n"
"@param[in] function_name - The name of the function to run.\n"
"@param[in] workload - The workload to pass to the function when it is run.\n"
"@param[in] unique - Optional unique job identifier, or None for a new UUID.\n"
"@param[in] name - Optional name for the gearman_argument_t.\n"
"@return the result on success.\n"
"@return NULL on failure.\n\n"
"Note: This is an example of executing work using a combination of libgearman calls.\n"
"See http://gearman.info/libgearman/gearman_execute.html");

static PyObject* pygear_client_get_options(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_get_options_doc,
"Get options for a client.\n\n"
"@return dictionary of options currently set on the client.");

static PyObject* pygear_client_job_status(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_job_status_doc,
"Get the status for a backgound task by its job handle.\n\n"
"@return dictionary with the following keys:\n"
"is_known - Whether or not the task is known.\n"
"is_running - Whether or not the task is running.\n"
"numerator - Progress numerator.\n"
"denominator - Progress denominator.\n");

static PyObject* pygear_client_remove_servers(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_remove_servers_doc,
"Remove all servers currently associated with the client.");

static PyObject* pygear_client_run_tasks(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_run_tasks_doc,
"Run tasks that have been added by 'add_task' and/or 'add_task_background'.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.");

static PyObject* pygear_client_set_complete_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_complete_fn_doc,
"Set the callback function when a task is complete.\n\n"
"@param[in] function - Function to call.\n"
"\tThis function must take one argument of type pygear.Task.\n\n"
"Example:\n"
"def oncomplete_callback(task):\n"
"    print task.results()\n\n"
"c.set_complete_fn(oncomplete_callback)");

static PyObject* pygear_client_set_created_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_created_fn_doc,
"Set the callback function when a job has been created for a task.\n\n"
"@param[in] function - Function to call.\n"
"\tThis function must take one argument of type pygear.Task.\n\n");

static PyObject* pygear_client_set_data_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_data_fn_doc,
"Set the callback function when there is a data packet for a task.\n\n"
"@param[in] function - Function to call.\n"
"\tThis function must take one argument of type pygear.Task.\n\n");

static PyObject* pygear_client_set_exception_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_exception_fn_doc,
"Set the callback function when there is an exception packet for a task.\n\n"
"@param[in] function - Function to call.\n"
"\tThis function must take one argument of type pygear.Task.\n\n");

static PyObject* pygear_client_set_fail_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_fail_fn_doc,
"Set the callback function when a task has failed.\n\n"
"@param[in] function - Function to call.\n"
"\tThis function must take one argument of type pygear.Task.\n\n");

static PyObject* pygear_client_set_log_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_log_fn_doc,
"Set logging function for this client. The logging function will be executed\n"
"whenever an error occurs. An error message string will be passed in as the parameter.\n\n"
"@param[in] function - Function to call when there is a logging message.\n"
"@param[in] verbose - Verbosity level threshold. Only call function\n"
"\twhen the logging message is equal to or less verbose than this.\n"
"\tMust be one of the pygear.PYGEAR_VERBOSE_* constants.\n\n"
"@return None on success.\n"
"@return NULL on failure.\n\n"
"Example:\n"
"def log_func(line):\n"
"    print line\n\n"
"c.set_log_fn(log_func, pygear.PYGEAR_VERBOSE_INFO)");

static PyObject* pygear_client_set_options(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_set_options_doc,
"Add a number of options for a client.\n"
"Options are specified as keyword arguments. If an argument is set to be True,\n"
"the option is enabled. If an argument is false or omitted, the option is\n"
"disabled. Available options are:\n"
"non_blocking, unbuffered_result, free_tasks, and generate_unique.\n\n"
"@return None on success, NULL on failure.");


static PyObject* pygear_client_set_serializer(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_serializer_doc,
"Specify the object to be used to serialize data passed through gearman.\n"
"By default, pygear will use 'json' to convert data to a string\n"
"representation during transit and reconstitute it on the other end.\n"
"You can replace the serializer with your own as long as it implements\n"
"the 'dumps' and 'loads' methods. 'dumps' must return a string, and loads\n"
"must take a string.\n\n"
"@param[in] serializer - Object implementing dumps and loads");

static PyObject* pygear_client_set_status_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_status_fn_doc,
"Set the callback function when there is a status packet for a task.\n\n"
"@param[in] function - Function to call.\n"
"\tThis function must take one argument of type pygear.Task.\n\n");

static PyObject* pygear_client_set_timeout(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_timeout_doc,
"Set the timeout, in milliseconds, that the client will wait.\n"
"A value of zero means the client never time out.\n\n"
"@param[in] timeout - Duration to wait in milliseconds.");

static PyObject* pygear_client_set_warning_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_warning_fn_doc,
"Set the callback function when there is a warning packet for a task.\n\n"
"@param[in] function - Function to call.\n"
"\tThis function must take one argument of type pygear.Task.\n\n");

static PyObject* pygear_client_set_workload_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_workload_fn_doc,
"Set the callback function when workload data needs to be sent for a task.\n\n"
"@param[in] function - Function to call.\n"
"\tThis function must take one argument of type pygear.Task.\n\n");

static PyObject* pygear_client_timeout(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_timeout_doc,
"Get the current timeout value, in milliseconds, for the client.\n"
"@return integer.");

static PyObject* pygear_client_unique_status(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_unique_status_doc,
"Get the status for a backgound task by its unique identifier.\n\n"
"@return dictionary with the following keys:\n"
"is_known - Whether or not the task is known.\n"
"is_running - Whether or not the task is running.\n"
"numerator - Progress numerator.\n"
"denominator - Progress denominator.");

static PyObject* pygear_client_wait(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_wait_doc,
"When in non-blocking I/O mode, wait for activity from one of the servers.\n\n"
"@return None on success.\n"
"@return NULL and raises pygear exception on failure.");


/* Module method specification */
static PyMethodDef client_module_methods[] = {
    _CLIENTMETHOD(clone,                    METH_NOARGS)

    // Server management
    _CLIENTMETHOD(add_server,               METH_VARARGS)
    _CLIENTMETHOD(add_servers,              METH_VARARGS)
    _CLIENTMETHOD(remove_servers,           METH_NOARGS)
    _CLIENTMETHOD(echo,                     METH_VARARGS)

    // Task management
    _CLIENTMETHOD(add_task,                 METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(add_task_background,      METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(add_task_high,            METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(add_task_high_background, METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(add_task_low,             METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(add_task_low_background,  METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(add_task_status,          METH_VARARGS)
    _CLIENTMETHOD(execute,                  METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(run_tasks,                METH_NOARGS)
    _CLIENTMETHOD(wait,                     METH_NOARGS)
    _CLIENTMETHOD(do,                       METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(do_background,            METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(do_high,                  METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(do_high_background,       METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(do_low,                   METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(do_low_background,        METH_VARARGS | METH_KEYWORDS)

    // Errors
    _CLIENTMETHOD(error,                    METH_NOARGS)
    _CLIENTMETHOD(error_code,               METH_NOARGS)
    _CLIENTMETHOD(errno,                    METH_NOARGS)

    // Job management
    _CLIENTMETHOD(do_job_handle,            METH_VARARGS)
    _CLIENTMETHOD(do_status,                METH_NOARGS)
    _CLIENTMETHOD(job_status,               METH_VARARGS)
    _CLIENTMETHOD(unique_status,            METH_VARARGS)

    // Callbacks
    _CLIENTMETHOD(set_workload_fn,          METH_VARARGS)
    _CLIENTMETHOD(set_created_fn,           METH_VARARGS)
    _CLIENTMETHOD(set_data_fn,              METH_VARARGS)
    _CLIENTMETHOD(set_warning_fn,           METH_VARARGS)
    _CLIENTMETHOD(set_status_fn,            METH_VARARGS)
    _CLIENTMETHOD(set_complete_fn,          METH_VARARGS)
    _CLIENTMETHOD(set_exception_fn,         METH_VARARGS)
    _CLIENTMETHOD(set_fail_fn,              METH_VARARGS)
    _CLIENTMETHOD(clear_fn,                 METH_NOARGS)
    _CLIENTMETHOD(set_log_fn,               METH_VARARGS)

    // Client Options
    _CLIENTMETHOD(set_options,              METH_KEYWORDS)
    _CLIENTMETHOD(get_options,              METH_NOARGS)
    _CLIENTMETHOD(timeout,                  METH_NOARGS)
    _CLIENTMETHOD(set_timeout,              METH_VARARGS)
    _CLIENTMETHOD(set_serializer,           METH_VARARGS)

    {NULL, NULL, 0, NULL}
};

PyTypeObject pygear_ClientType = {
    PyObject_HEAD_INIT(NULL)
    0,                                          /*ob_size*/
    "pygear.Client",                            /*tp_name*/
    sizeof(pygear_ClientObject),                /*tp_basicsize*/
    0,                                          /*tp_itemsize*/
    (destructor)Client_dealloc,                 /*tp_dealloc*/
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
    client_module_docstring,                    /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    client_module_methods,                      /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)Client_init,                      /* tp_init */
};

#endif
