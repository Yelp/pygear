#include <Python.h>
#include <libgearman-1.0/gearman.h>
#include <stdio.h>
#include "structmember.h"

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
} pygear_ClientObject;

PyDoc_STRVAR(client_module_docstring, "Represents a Gearman client");

/* Class init methods */
PyObject* Client_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int Client_init(pygear_ClientObject *self, PyObject *args, PyObject *kwds);
void Client_dealloc(pygear_ClientObject* self);

/* Method definitions */
static PyObject* pygear_client_clone(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_clone_doc,
"Clone a client structure.");

static PyObject* pygear_client_add_server(pygear_ClientObject *self, PyObject *args);
PyDoc_STRVAR(pygear_client_add_server_doc,
"Add a list of job servers to a client. The format for the server list is:\n"
"SERVER[:PORT][,SERVER[:PORT]]...\n"
"Some examples are:\n"
"10.0.0.1,10.0.0.2,10.0.0.3\n"
"localhost234,jobserver2.domain.com:7003,10.0.0.3\n"
"@param[in] client Structure previously initialized with "
"gearman_client_create() or gearman_client_clone().\n"
"@param[in] servers Server list described above.\n"
"@return Standard gearman return value.");

static PyObject* pygear_client_add_servers(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_add_servers_doc,
"Add a job server to a client. This goes into a list of servers that can be\n"
"used to run tasks. No socket I/O happens here, it is just added to a list.\n"
"@param[in] client Structure previously initialized with "
"gearman_client_create() or gearman_client_clone().\n"
"@param[in] host Hostname or IP address (IPv4 or IPv6) of the server to add.\n"
"@param[in] port Port of the server to add.\n"
"@return Standard gearman return value.");

static PyObject* pygear_client_add_task(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_add_task_doc,
"Add a task to be run in parallel.\n\n"
"@param[in] function_name The name of the function to run.\n"
"@param[in] workload The workload to pass to the function when it is run.\n"
"@param[in] unique Optional unique job identifier, or None for a new UUID.\n"
"@return A tuple of return code, Task object. On failure the Task will be None.");

static PyObject* pygear_client_add_task_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_add_task_background_doc,
"Add a background task to be run in parallel.\n"
"See gearman_client_add_task() for details.");

static PyObject* pygear_client_add_task_high(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_add_task_high_doc,
"Add a high priority task to be run in parallel.\n"
"See gearman_client_add_task() for details.");

static PyObject* pygear_client_add_task_high_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_add_task_high_background_doc,
"Add a high priority background task to be run in parallel.\n"
"See gearman_client_add_task() for details.");

static PyObject* pygear_client_add_task_low(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_add_task_low_doc,
"Add a low priority task to be run in parallel.\n"
"See gearman_client_add_task() for details.");

static PyObject* pygear_client_add_task_low_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_add_task_low_background_doc,
"Add a low priority background task to be run in parallel.\n"
"See gearman_client_add_task() for details.");

static PyObject* pygear_client_do(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_do_doc,
"Run a single task synchronously and return a result.\n"
"@param[in] function_name The name of the function to run.\n"
"@param[in] unique Optional unique job identifier, or None for a new UUID.\n"
"@param[in] workload The workload to pass to the function when it is run.\n"
"@return Tuple of standard gearman return value and result. In the case of\n"
" GEARMAN_WORK_DATA, GEARMAN_WORK_WARNING, or GEARMAN_WORK_STATUS, the caller\n"
" should take any actions to handle the event and then call this function\n"
" again. This may happen multiple times until a GEARMAN_WORK_ERROR,\n"
" GEARMAN_WORK_FAIL, or GEARMAN_SUCCESS (work complete) is returned. For\n"
" GEARMAN_WORK_DATA or GEARMAN_WORK_WARNING, the result_size will be set to\n"
" the intermediate data chunk being returned and an allocated data buffer\n"
" will be returned. For GEARMAN_WORK_STATUS, the caller can use\n"
" gearman_client_do_status() to get the current tasks status.");

static PyObject* pygear_client_do_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_do_background_doc,
"Run a background task and return an allocated result.\n"
"See Client.do() for parameter and return information.");

static PyObject* pygear_client_do_high(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_do_high_doc,
"Run a high priority task and return an allocated result.\n"
"See Client.do() for parameter and return information.");

static PyObject* pygear_client_do_high_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_do_high_background_doc,
"Run a high priority background task and return an allocated result.\n"
"See Client.do() for parameter and return information.");

static PyObject* pygear_client_do_low(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_do_low_doc,
"Run a low priority task and return an allocated result.\n"
"See Client.do() for parameter and return information.");

static PyObject* pygear_client_do_low_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_do_low_background_doc,
"Run a low priority background task and return an allocated result.\n"
"See Client.do() for parameter and return information.");

static PyObject* pygear_client_execute(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_execute_doc,
"Run a task immediately and wait for the return.\n\n"
"@param[in] function_name The name of the function to run.\n"
"@param[in] workload The workload to pass to the function when it is run.\n"
"@param[in] unique Optional unique job identifier, or None for a new UUID.\n"
"@param[in] name Optional name for the gearman_argument_t.\n"
"@return A tuple of return code, Task object. On failure the Task will be None.");

static PyObject* pygear_client_set_options(pygear_ClientObject* self, PyObject* args, PyObject* kwargs);
PyDoc_STRVAR(pygear_client_set_options_doc,
"Add a number of options to a Gearman client.\n"
"Options are specified as keyword arguments. If an argument is set to True, "
"the option is enabled. If an argument is false or omitted, the option is "
"disabled.\nAvailable options are:\n"
"non_blocking, unbuffered_result, free_tasks and generate_unique");

static PyObject* pygear_client_get_options(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_get_options_doc,
"Returns a dictionary of the current options set on the client");

static PyObject* pygear_client_run_tasks(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_run_tasks_doc,
"Run tasks that have been added in parallel");

static PyObject* pygear_client_wait(pygear_ClientObject* self);
PyDoc_STRVAR(pygear_client_wait_doc,
"When in non-blocking I/O mode, wait for activity from one of the servers.");

static PyObject* pygear_client_set_workload_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_workload_fn_doc,
"Callback function when workload data needs to be sent for a task.\n"
"@param[in] function Function to call. Must take one argument, which will be \n"
"of type pygear.Task");

static PyObject* pygear_client_set_created_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_created_fn_doc,
"Callback function when a job has been created for a task.\n"
"@param[in] function Function to call. Must take one argument, which will be \n"
"of type pygear.Task");

static PyObject* pygear_client_set_data_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_data_fn_doc,
"Callback function when there is a data packet for a task..\n"
"@param[in] function Function to call. Must take one argument, which will be \n"
"of type pygear.Task");

static PyObject* pygear_client_set_warning_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_warning_fn_doc,
"Callback function when there is a warning packet for a task.\n"
"@param[in] function Function to call. Must take one argument, which will be \n"
"of type pygear.Task");

static PyObject* pygear_client_set_status_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_status_fn_doc,
"Callback function when there is a status packet for a task.\n"
"@param[in] function Function to call. Must take one argument, which will be \n"
"of type pygear.Task");

static PyObject* pygear_client_set_complete_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_complete_fn_doc,
"Callback function when a task is complete.\n"
"@param[in] function Function to call. Must take one argument, which will be \n"
"of type pygear.Task");

static PyObject* pygear_client_set_exception_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_exception_fn_doc,
"Callback function when there is an exception packet for a task.\n"
"@param[in] function Function to call. Must take one argument, which will be \n"
"of type pygear.Task");

static PyObject* pygear_client_set_fail_fn(pygear_ClientObject* self, PyObject* args);
PyDoc_STRVAR(pygear_client_set_fail_fn_doc,
"Callback function when a task has failed.\n"
"@param[in] function Function to call. Must take one argument, which will be \n"
"of type pygear.Task");

/* Module method specification */
static PyMethodDef client_module_methods[] = {
    _CLIENTMETHOD(clone,                    METH_NOARGS)

    // Server management

    _CLIENTMETHOD(add_server,               METH_VARARGS)
    _CLIENTMETHOD(add_servers,              METH_VARARGS)

    // Task management
    _CLIENTMETHOD(add_task,                 METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(add_task_background,      METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(add_task_high,            METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(add_task_high_background, METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(add_task_low,             METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(add_task_low_background,  METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(execute,                  METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(run_tasks,                METH_NOARGS)
    _CLIENTMETHOD(wait,                     METH_NOARGS)
    _CLIENTMETHOD(do,                       METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(do_background,            METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(do_high,                  METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(do_high_background,       METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(do_low,                   METH_VARARGS | METH_KEYWORDS)
    _CLIENTMETHOD(do_low_background,        METH_VARARGS | METH_KEYWORDS)

    // Callbacks
    _CLIENTMETHOD(set_workload_fn,          METH_VARARGS)
    _CLIENTMETHOD(set_created_fn,           METH_VARARGS)
    _CLIENTMETHOD(set_data_fn,              METH_VARARGS)
    _CLIENTMETHOD(set_warning_fn,           METH_VARARGS)
    _CLIENTMETHOD(set_status_fn,            METH_VARARGS)
    _CLIENTMETHOD(set_complete_fn,          METH_VARARGS)
    _CLIENTMETHOD(set_exception_fn,         METH_VARARGS)
    _CLIENTMETHOD(set_fail_fn,              METH_VARARGS)

    // Client Options
    _CLIENTMETHOD(set_options,              METH_KEYWORDS)
    _CLIENTMETHOD(get_options,              METH_NOARGS)
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
    0,                                          /* tp_alloc */
    Client_new,                                 /* tp_new */
};

#endif
