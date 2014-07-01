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
    PyObject* pickle;
} pygear_WorkerObject;

PyDoc_STRVAR(worker_module_docstring, "Represents a Gearman worker");

/* Class init methods */
PyObject* Worker_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int Worker_init(pygear_WorkerObject *self, PyObject *args, PyObject *kwds);
void Worker_dealloc(pygear_WorkerObject* self);

/* Method definitions */
static PyObject* pygear_worker_clone(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_clone_doc,
"Clone a worker structure.\n"
"@return On success, a new Worker instance");

static PyObject* pygear_worker_error(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_error_doc,
"See gearman_error() for details.");

static PyObject* pygear_worker_errno(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_errno_doc,
"See gearman_errno() for details.");

static PyObject* pygear_worker_set_options(pygear_WorkerObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_worker_set_options_doc,
"Set options for a worker.\n"
"@param[in] options Dictionary of options to set on the worker");

static PyObject* pygear_worker_get_options(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_get_options_doc,
"Get options for a worker.\n"
"@return Dictionary of options currently set on the worker");

static PyObject* pygear_worker_timeout(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_timeout_doc,
"See gearman_universal_timeout() for details.");

static PyObject* pygear_worker_set_timeout(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_set_timeout_doc,
"See gearman_universal_set_timeout() for details.");

static PyObject* pygear_worker_add_server(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_add_server_doc,
"Add a job server to a worker. This goes into a list of servers that can be\n"
"used to run tasks. No socket I/O happens here, it is just added to a list.\n"
"@param[in] host Hostname or IP address (IPv4 or IPv6) of the server to add.\n"
"@param[in] port Port of the server to add.\n"
"@raises Pygear exception on failure.");

static PyObject* pygear_worker_add_servers(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_add_servers_doc,
"Add a list of job servers to a worker. The format for the server list is:\n"
"[ 'SERVER[:PORT]', [,SERVER[:PORT]]... ]\n"
"Some examples are:\n"
"['10.0.0.1, '10.0.0.2', '10.0.0.3']\n"
"['localhost234', 'jobserver2.domain.com:7003', '10.0.0.3']\n"
"@param[in] servers Server list described above.\n"
"@raises Pygear exception on failure.");

static PyObject* pygear_worker_remove_servers(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_remove_servers_doc,
"Remove all servers currently associated with the worker.");

static PyObject* pygear_worker_wait(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_wait_doc,
"When in non-blocking I/O mode, wait for activity from one of the servers.");

static PyObject* pygear_worker_register(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_register_doc,
"Register function with job servers with an optional timeout. The timeout\n"
"specifies how many seconds the server will wait before marking a job as\n"
"failed. If timeout is zero, there is no timeout.\n"
"@param[in] function_name Function name to register.\n"
"@param[in] timeout Optional timeout (in seconds) that specifies the maximum\n"
" time a job should. This is enforced on the job server. A value of 0 means\n"
" an infinite time.\n"
"@raises Pygear exception on failure.");

static PyObject* pygear_worker_unregister(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_unregister_doc,
"Unregister function with job servers.\n"
"@param[in] function_name Function name to unregister.\n"
"@raises Pygear exception on failure.");

static PyObject* pygear_worker_unregister_all(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_unregister_all_doc,
"Unregister all functions with job servers.\n"
"@raises Pygear exception on failure.");

static PyObject* pygear_worker_grab_job(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_grab_job_doc,
"Get a job from one of the job servers. This does not used the callback\n"
"interface below, which means results must be sent back to the job server\n"
"manually. It is also the responsibility of the caller to free the job once\n"
"it has been completed.\n"
"@return On success, a new Job object");

static PyObject* pygear_worker_free_all(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_free_all_doc,
"Free all jobs for a gearman structure.");

static PyObject* pygear_worker_function_exists(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_function_exists_doc,
"See if a function exists in the server. It will not return\n"
"true if the function is currently being de-allocated.\n"
"@param[in] function_name Function name for search.\n"
"@return bool");

static PyObject* pygear_worker_add_function(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_add_function_doc,
"Register and add callback function for worker.\n"
"@param[in] function_name Name of funtion to register\n"
"@param[in] timeout Timeout in seconds for the job execution\n"
"@param[in] function A function that takes a Job instance\n"
"@raises Pygear exception on failure\n");

static PyObject* pygear_worker_work(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_work_doc,
"Wait for a job and call the appropriate callback function when it gets one.\n"
"@raises Pygear exception on failure\n");

static PyObject* pygear_worker_echo(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_echo_doc,
"Send data to all job servers to see if they echo it back. This is a test\n"
"function to see if job servers are responding properly.\n"
"@param[in] workload The workload to ask the server to echo back.\n"
"@raises Pygear exception on failure.");

static PyObject* pygear_worker_id(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_id_doc,
"Get the ID of the worker\n"
"@return Integer id of the worker");

static PyObject* pygear_worker_set_identifier(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_set_identifier_doc,
"Set the identifier that the server uses to identify the worker.\n"
"@param[in] identifier String new identifier for the worker");

static PyObject* pygear_worker_set_namespace(pygear_WorkerObject* self, PyObject* args);
PyDoc_STRVAR(pygear_worker_set_namespace_doc,
"Set the namespace of the worker\n"
"@param[in] namespace String namespace to assign the worker to.\n"
"Only clients sharing a namespace can see each other's data.");

static PyObject* pygear_worker_namespace(pygear_WorkerObject* self);
PyDoc_STRVAR(pygear_worker_namespace_doc,
"Get the current namespace of the worker\n"
"@return The current namespace, or None if no namespace has been set");

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
    _WORKERMETHOD(free_all,         METH_NOARGS)
    _WORKERMETHOD(function_exists,  METH_VARARGS)
    _WORKERMETHOD(add_function,     METH_VARARGS)
    _WORKERMETHOD(work,             METH_NOARGS)
    _WORKERMETHOD(echo,             METH_VARARGS)
    _WORKERMETHOD(id,               METH_NOARGS)
    _WORKERMETHOD(set_identifier,   METH_VARARGS)
    _WORKERMETHOD(set_namespace,    METH_VARARGS)
    _WORKERMETHOD(namespace,        METH_NOARGS)
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
