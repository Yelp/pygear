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

#include "worker.h"

/*
 * Class constructor / destructor methods
 */

PyObject* Worker_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    pygear_WorkerObject* self = NULL;

    self = (pygear_WorkerObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->g_Worker = NULL;
        self->g_FunctionMap = NULL;
    }

    return (PyObject *)self;
}

/* Return -1 if fail, 0 if success */
int Worker_init(pygear_WorkerObject* self, PyObject* args, PyObject* kwds) {
    self->g_Worker = gearman_worker_create(NULL);
    gearman_worker_options_t worker_options = gearman_worker_options(self->g_Worker);
    worker_options = worker_options & (~GEARMAN_WORKER_GRAB_ALL);
    gearman_worker_set_options(self->g_Worker, worker_options);

    self->g_FunctionMap = PyDict_New();
    self->serializer = PyImport_ImportModule(PYTHON_SERIALIZER);

    if (self->serializer == NULL) {
        PyObject* err_string = PyString_FromFormat("Failed to import '%s'", PYTHON_SERIALIZER);
        PyErr_SetObject(PyExc_ImportError, err_string);
        Py_XDECREF(err_string);
        return -1;
    }
    if (self->g_Worker == NULL) {
        PyErr_SetString(PyGearExn_ERROR, "Failed to create internal gearman worker structure");
        return -1;
    }
    if (self->g_FunctionMap == NULL) {
        PyErr_SetString(PyGearExn_ERROR, "Failed to create internal dictionary");
        return -1;
    }
    return 0;
}

void Worker_dealloc(pygear_WorkerObject* self) {
    if (self->g_Worker) {
        gearman_worker_free(self->g_Worker);
        self->g_Worker = NULL;
    }

    Py_XDECREF(self->g_FunctionMap);
    Py_XDECREF(self->serializer);

    self->ob_type->tp_free((PyObject*)self);
}

/*
 * Instance Methods
 */

/* Return NULL if fail, Py_RETURN_NONE if success */
static PyObject* pygear_worker_set_serializer(pygear_WorkerObject* self, PyObject* args) {
    PyObject* serializer = NULL;

    if (!PyArg_ParseTuple(args, "O", &serializer)) {
        return NULL;
    }

    if (!PyObject_HasAttrString(serializer, "loads")) {
        PyErr_SetString(PyExc_AttributeError, "Serializer does not implement 'loads'");
        return NULL;
    }

    if (!PyObject_HasAttrString(serializer, "dumps")) {
        PyErr_SetString(PyExc_AttributeError, "Serializer does not implement 'dumps'");
        return NULL;
    }

    Py_INCREF(serializer);
    Py_XDECREF(self->serializer); // dealloc old one
    self->serializer = serializer;

    Py_RETURN_NONE;
}

/* Return value: New reference */
static PyObject* pygear_worker_clone(pygear_WorkerObject* self) {
    PyObject *argList = NULL;
    pygear_WorkerObject* python_worker = NULL;
    PyObject* ret = NULL;

    argList = Py_BuildValue("(O, O)", Py_None, Py_None);
    python_worker = (pygear_WorkerObject*) PyObject_CallObject((PyObject *) &pygear_WorkerType, argList);
    python_worker->g_Worker = gearman_worker_clone(NULL, self->g_Worker);
    ret = Py_BuildValue("O", python_worker);

    Py_XDECREF(argList);
    Py_XDECREF(python_worker);
    return ret;
}

/* Return value: New reference */
static PyObject* pygear_worker_error(pygear_WorkerObject* self) {
    return Py_BuildValue("s", gearman_worker_error(self->g_Worker));
}

/* Return value: New reference */
static PyObject* pygear_worker_errno(pygear_WorkerObject* self) {
    return Py_BuildValue("i", gearman_worker_errno(self->g_Worker));
}

#define PYGEAR_WORKER_ALLOCATED         "allocated"
#define PYGEAR_WORKER_NON_BLOCKING      "non_blocking"
#define PYGEAR_WORKER_PACKET_INIT       "packet_init"
#define PYGEAR_WORKER_GRAB_JOB_IN_USE   "grab_job_in_use"
#define PYGEAR_WORKER_PRE_SLEEP_IN_USE  "pre_sleep_in_use"
#define PYGEAR_WORKER_WORK_JOB_IN_USE   "job_in_use"
#define PYGEAR_WORKER_CHANGE            "change"
#define PYGEAR_WORKER_GRAB_UNIQ         "grab_uniq"
#define PYGEAR_WORKER_TIMEOUT_RETURN    "timeout_return"
#define PYGEAR_WORKER_GRAB_ALL          "grab_all"
#define PYGEAR_WORKER_MAX               "max"

/* Return NULL if fail, Py_RETURN_NONE if success */
static PyObject* pygear_worker_set_options(pygear_WorkerObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = {
        PYGEAR_WORKER_ALLOCATED,
        PYGEAR_WORKER_NON_BLOCKING,
        PYGEAR_WORKER_PACKET_INIT,
        PYGEAR_WORKER_GRAB_JOB_IN_USE,
        PYGEAR_WORKER_PRE_SLEEP_IN_USE,
        PYGEAR_WORKER_WORK_JOB_IN_USE,
        PYGEAR_WORKER_CHANGE,
        PYGEAR_WORKER_GRAB_UNIQ,
        PYGEAR_WORKER_TIMEOUT_RETURN,
        PYGEAR_WORKER_GRAB_ALL,
        PYGEAR_WORKER_MAX,
        NULL
    };

    static int options_t_value[] = {
          GEARMAN_WORKER_ALLOCATED,
          GEARMAN_WORKER_NON_BLOCKING,
          GEARMAN_WORKER_PACKET_INIT,
          GEARMAN_WORKER_GRAB_JOB_IN_USE,
          GEARMAN_WORKER_PRE_SLEEP_IN_USE,
          GEARMAN_WORKER_WORK_JOB_IN_USE,
          GEARMAN_WORKER_CHANGE,
          GEARMAN_WORKER_GRAB_UNIQ,
          GEARMAN_WORKER_TIMEOUT_RETURN,
          GEARMAN_WORKER_GRAB_ALL,
          GEARMAN_WORKER_MAX
    };

    int num_options = 11;
    int worker_options[num_options];
    if (! PyArg_ParseTupleAndKeywords(args, kwargs, "|iiiiiiiiiii", kwlist,
                                      &worker_options[0],
                                      &worker_options[1],
                                      &worker_options[2],
                                      &worker_options[3],
                                      &worker_options[4],
                                      &worker_options[5],
                                      &worker_options[6],
                                      &worker_options[7],
                                      &worker_options[8],
                                      &worker_options[9],
                                      &worker_options[10]
                                      )) {
        return NULL;
    }

    int i;
    for (i = 0; i < num_options; ++i) {
        if (worker_options[i]) {
            gearman_worker_add_options(self->g_Worker, options_t_value[i]);
        } else {
            gearman_worker_remove_options(self->g_Worker, options_t_value[i]);
        }
    }

    Py_RETURN_NONE;
}

/* Return value: New reference */
static PyObject* pygear_worker_get_options(pygear_WorkerObject* self) {
    static int options_t_value[] = {
          GEARMAN_WORKER_ALLOCATED,
          GEARMAN_WORKER_NON_BLOCKING,
          GEARMAN_WORKER_PACKET_INIT,
          GEARMAN_WORKER_GRAB_JOB_IN_USE,
          GEARMAN_WORKER_PRE_SLEEP_IN_USE,
          GEARMAN_WORKER_WORK_JOB_IN_USE,
          GEARMAN_WORKER_CHANGE,
          GEARMAN_WORKER_GRAB_UNIQ,
          GEARMAN_WORKER_TIMEOUT_RETURN,
          GEARMAN_WORKER_GRAB_ALL,
          GEARMAN_WORKER_MAX
    };

    gearman_worker_options_t options = gearman_worker_options(self->g_Worker);

    int num_options = 11;
    PyObject* worker_options[num_options];
    int i;
    for (i = 0; i < num_options; ++i) {
        worker_options[i] = (options & options_t_value[i] ? Py_True : Py_False);
    }
    PyObject* option_dictionary = Py_BuildValue(
        "{s:O, s:O, s:O, s:O, s:O, s:O, s:O, s:O, s:O, s:O, s:O}",
        PYGEAR_WORKER_ALLOCATED, worker_options[0],
        PYGEAR_WORKER_NON_BLOCKING, worker_options[1],
        PYGEAR_WORKER_PACKET_INIT, worker_options[2],
        PYGEAR_WORKER_GRAB_JOB_IN_USE, worker_options[3],
        PYGEAR_WORKER_PRE_SLEEP_IN_USE, worker_options[4],
        PYGEAR_WORKER_CHANGE, worker_options[5],
        PYGEAR_WORKER_GRAB_UNIQ, worker_options[6],
        PYGEAR_WORKER_TIMEOUT_RETURN, worker_options[7],
        PYGEAR_WORKER_GRAB_ALL, worker_options[8],
        PYGEAR_WORKER_MAX, worker_options[9],
        PYGEAR_WORKER_MAX, worker_options[10]
    );

    Py_XDECREF(worker_options);
    return option_dictionary;
}

/* Return value: New reference */
static PyObject* pygear_worker_timeout(pygear_WorkerObject* self) {
    return Py_BuildValue("i", gearman_worker_timeout(self->g_Worker));
}

/* Return NULL if fail, Py_RETURN_NONE if success */
static PyObject* pygear_worker_set_timeout(pygear_WorkerObject* self, PyObject* args) {
    int timeout;
    if (!PyArg_ParseTuple(args, "i", &timeout)) {
        return NULL;
    }
    gearman_worker_set_timeout(self->g_Worker, timeout);
    Py_RETURN_NONE;
}

static void _pygear_worker_log_fn_wrapper(const char* line, gearman_verbose_t verbose, void* context) {
    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject* python_cb_method = (PyObject*) context; // borrowed ref
    PyObject* python_line = PyString_FromString(line);
    PyObject* callback_return = PyObject_CallFunction(python_cb_method, "O", python_line);
    
    if (!callback_return) {
        if (PyErr_Occurred()) {
            PyErr_Print();
        }
    }

    Py_XDECREF(python_line);
    Py_XDECREF(callback_return);

    PyGILState_Release(gstate);
}

/* Return NULL if fail, Py_RETURN_NONE if success */
static PyObject* pygear_worker_set_log_fn(pygear_WorkerObject* self, PyObject* args) {
    PyObject* logging_cb;
    int log_level;
    if (!PyArg_ParseTuple(args, "Oi", &logging_cb, &log_level)) {
        return NULL;
    }
    Py_INCREF(logging_cb);
    gearman_worker_set_log_fn(self->g_Worker, _pygear_worker_log_fn_wrapper, logging_cb, log_level);
    Py_DECREF(logging_cb);
    Py_RETURN_NONE;
}

/* Return NULL if fail, Py_RETURN_NONE if success */
static PyObject* pygear_worker_add_server(pygear_WorkerObject* self, PyObject* args){
    char* host;
    int port;
    if (!PyArg_ParseTuple(args, "zi", &host, &port)) {
        return NULL;
    }
    gearman_return_t result = gearman_worker_add_server(self->g_Worker, host, port);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_worker_add_servers(pygear_WorkerObject* self, PyObject* args) {
    PyTypeObject* arg_type = NULL;

    PyObject* server_list;
    if (!PyArg_ParseTuple(args, "O", &server_list)) {
        return NULL;
    }

    bool success = false;

    if (!PyList_Check(server_list)) {
        arg_type = (PyTypeObject*) PyObject_Type(server_list);
        char* err_base = "Worker.add_servers expected list, got ";
        char* err_string = malloc(sizeof(char) * (strlen(err_base) + strlen(arg_type->tp_name) + 1));
        sprintf(err_string, "%s%s", err_base, arg_type->tp_name);
        PyErr_SetString(PyExc_TypeError, err_string);
        if (err_string != NULL) {
            free(err_string);
        }
        goto catch;
    }

    Py_ssize_t num_servers = PyList_Size(server_list);
    Py_ssize_t i;
    for (i = 0; i < num_servers; ++i) {
        char* srv_string = PyString_AsString(PyList_GetItem(server_list, i));
        gearman_return_t result = gearman_worker_add_servers(self->g_Worker, srv_string);
        if (_pygear_check_and_raise_exn(result)) {
            goto catch;
        }
    }

    success = true;

catch:
    Py_XDECREF(arg_type);
    if (success) {
        Py_RETURN_NONE;
    }
    return NULL;
}

static PyObject* pygear_worker_remove_servers(pygear_WorkerObject* self) {
    gearman_worker_remove_servers(self->g_Worker);
    Py_RETURN_NONE;
}

static PyObject* pygear_worker_wait(pygear_WorkerObject* self) {
    gearman_return_t result = gearman_worker_wait(self->g_Worker);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_worker_register(pygear_WorkerObject* self, PyObject* args) {
    char* function_name;
    unsigned timeout;
    if (!PyArg_ParseTuple(args, "sI", &function_name, &timeout)) {
        return NULL;
    }
    gearman_return_t result = gearman_worker_register(self->g_Worker, function_name, timeout);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_worker_unregister(pygear_WorkerObject* self, PyObject* args) {
    char* function_name;
    if (!PyArg_ParseTuple(args, "s", &function_name)) {
        return NULL;
    }
    gearman_return_t result = gearman_worker_unregister(self->g_Worker, function_name);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_worker_unregister_all(pygear_WorkerObject* self) {
    gearman_return_t result = gearman_worker_unregister_all(self->g_Worker);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

/* Return value: New reference */
static PyObject* pygear_worker_grab_job(pygear_WorkerObject* self) {
    gearman_return_t result;
    gearman_job_st* new_job = gearman_worker_grab_job(self->g_Worker, NULL, &result);

    PyObject* argList = NULL;
    pygear_JobObject* python_job = NULL;
    PyObject* callmethod_result = NULL;
    PyObject* ret = NULL;

    if (_pygear_check_and_raise_exn(result)) {
        goto catch;
    }
    argList = Py_BuildValue("(O, O)", Py_None, Py_None);
    if (!argList) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to instantiate argument tuple for new Job object");
        goto catch;
    }
    python_job = (pygear_JobObject*) PyObject_CallObject((PyObject *) &pygear_JobType, argList);
    if (!python_job) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to instantiate new Job object");
        goto catch;
    }
    callmethod_result = PyObject_CallMethod((PyObject*) python_job, "set_serializer", "O", self->serializer);
    if (!callmethod_result) {
        goto catch;
    }

    python_job->g_Job = new_job;
    ret = Py_BuildValue("O", python_job);

catch:
    Py_XDECREF(argList);
    Py_XDECREF(python_job);
    Py_XDECREF(callmethod_result);

    return ret;
}

static PyObject* pygear_worker_free_all(pygear_WorkerObject* self) {
    gearman_job_free_all(self->g_Worker);
    Py_RETURN_NONE;
}

static PyObject* pygear_worker_function_exists(pygear_WorkerObject* self, PyObject* args) {
    char* function_name;
    int function_length;
    if (!PyArg_ParseTuple(args, "s#", &function_name, &function_length)) {
        return NULL;
    }
    int func_exist = gearman_worker_function_exist(self->g_Worker, function_name, function_length);

    return (func_exist ? Py_True : Py_False);
}


void* _pygear_worker_function_mapper(gearman_job_st* gear_job, void* context,
                                   size_t* result_size, gearman_return_t* ret_ptr) {
    PyGILState_STATE gstate = PyGILState_Ensure();

    // borrowed refs
    pygear_WorkerObject* worker = ((pygear_WorkerObject*) context);
    const char* job_func_name = gearman_job_function_name(gear_job);
    PyObject* python_cb_method = PyDict_GetItemString(worker->g_FunctionMap, job_func_name);

    // new refs
    PyObject* argList = NULL;
    pygear_JobObject* python_job = NULL;
    PyObject* callmethod_result = NULL;
    PyObject* callback_return = NULL;
    PyObject* pickled_result = NULL;
    PyObject* ptype_repr = NULL;
    PyObject* pvalue_args = NULL;
    PyObject* traceback = NULL;
    PyObject* string_traceback = NULL;
    PyObject* error_tuple = NULL;
    PyObject* serialized_data = NULL;

    enum {FAIL, SUCCESS, UNDEFINED};
    int retptr = FAIL;

    if (!python_cb_method) {
        PyErr_SetString(PyExc_SystemError, "Worker does not support method %s\n");
        goto catch;
    }

    // Bind the job into a python representation, and call through the python callback method
    argList = Py_BuildValue("(O, O)", Py_None, Py_None);
    python_job = (pygear_JobObject*) PyObject_CallObject((PyObject *) &pygear_JobType, argList);
    callmethod_result = PyObject_CallMethod((PyObject*) python_job, "set_serializer", "O", worker->serializer);
    if (!callmethod_result) {
        goto catch;
    }

    python_job->g_Job = gear_job;

    callback_return = PyObject_CallFunction(python_cb_method, "O", python_job);

    if (!callback_return) {

        if (!PyErr_Occurred()) {
            // If the callback returned NULL but did not set an exception, set a generic one to be sent back.
            PyObject* err_string = PyString_FromFormat("Callback method for %s failed, but threw no exception", job_func_name);
            PyErr_SetObject(PyGearExn_ERROR, err_string);
            Py_XDECREF(err_string);
        }

        PyObject *ptype, *pvalue, *ptraceback; // FIXME: missing Py_XDECREF
        PyErr_Fetch(&ptype, &pvalue, &ptraceback);
        Py_XINCREF(ptype);
        Py_XINCREF(pvalue);
        Py_XINCREF(ptraceback);
        PyErr_Restore(ptype, pvalue, ptraceback);
        PyErr_Print();
        PyErr_Restore(ptype, pvalue, ptraceback);

        // The value and traceback object may be NULL even when the type object is not.
        // NULL values would break Py_BuildValue below, so switch them to None
        if (!pvalue) {
            pvalue = Py_None;
        }
        if (!ptraceback) {
            ptraceback = Py_None;
        }

        ptype_repr = PyObject_Repr(ptype);
        if (!ptype_repr) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_SystemError, "Failed to get repr of exception type\n");
            }
            goto catch;
        }
        pvalue_args = PyObject_GetAttrString(pvalue, "args");
        if (!pvalue_args) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_SystemError, "Failed to extract args from exception\n");
            }
            goto catch;
        }
        traceback = PyImport_ImportModule("traceback");
        if (!traceback) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_SystemError, "Failed to import traceback for error reporting\n");
            }
            goto catch;
        }
        string_traceback = PyObject_CallMethod(traceback, "format_tb", "O", ptraceback);
        if (!string_traceback) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_SystemError, "Failed to get formatted traceback\n");
            }
            goto catch;
        }
        error_tuple = Py_BuildValue("(O, O, O)", ptype_repr, pvalue_args, string_traceback);
        if (!error_tuple) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_SystemError, "Failed create tuple containing exception details\n");
            }
            goto catch;
        }
        serialized_data = PyObject_CallMethod(worker->serializer, "dumps", "(O)", error_tuple);
        if (!serialized_data) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_SystemError, "Failed to serialize exception data\n");
            }
            goto catch;
        }
        char* c_data; Py_ssize_t c_data_size;
        if (PyString_AsStringAndSize(serialized_data, &c_data, &c_data_size) == -1) {
            PyErr_SetString(PyExc_SystemError, "Failed to stringify serialized exception data\n");
            goto catch;
        }

        gearman_return_t exn_sent = gearman_job_send_exception(gear_job, c_data, c_data_size);

        if (!gearman_success(exn_sent)) {
            PyObject* err_string = PyString_FromFormat("Failed to send exception data for job: %s\n", gearman_strerror(exn_sent));
            PyErr_SetObject(PyExc_SystemError, err_string);
            Py_XDECREF(err_string);
            goto catch;
        }

        retptr = UNDEFINED;

    } else {
        // Try to pickle the return from the function
        pickled_result = PyObject_CallMethodObjArgs (
            worker->serializer,
            PyString_FromString("dumps"),
            callback_return,
            NULL
        );
        if (!pickled_result) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_SystemError, "Failed to serialize worker result data\n");
            }
            goto catch;
        }
        else {
            Py_ssize_t len;
            char* buffer;
            PyString_AsStringAndSize(pickled_result, &buffer, &len);
            if (_pygear_check_and_raise_exn(gearman_job_send_complete(gear_job, buffer, len))) {
                PyErr_Print();
                retptr = UNDEFINED;
            } else {
                retptr = SUCCESS;
            }
        }
    }

catch:
    Py_XDECREF(pickled_result);
    Py_XDECREF(callmethod_result);
    Py_XDECREF(argList);
    Py_XDECREF(ptype_repr);
    Py_XDECREF(pvalue_args);
    Py_XDECREF(traceback);
    Py_XDECREF(string_traceback);
    Py_XDECREF(error_tuple);
    Py_XDECREF(serialized_data);
    python_job->g_Job = NULL;
    Py_XDECREF(python_job);

    PyGILState_Release(gstate);
    if (retptr == SUCCESS) {
        *ret_ptr = GEARMAN_SUCCESS;
    } else if (retptr == FAIL) {
        *ret_ptr = GEARMAN_FAIL;
    } else { 
        // ret_ptr remain unchanged as input
    }
    return NULL;
}

/* Return NULL if fail, Py_RETURN_NONE if success */
static PyObject* pygear_worker_add_function(pygear_WorkerObject* self, PyObject* args) {
    char* function_name;
    int timeout;
    PyObject* callback_method;
    if (!PyArg_ParseTuple(args, "siO", &function_name, &timeout, &callback_method)) {
        return NULL;
    }

    Py_INCREF(callback_method);
    PyObject* func_name_str = PyString_FromString(function_name);
    PyDict_SetItem(self->g_FunctionMap, func_name_str, callback_method);
    Py_DECREF(func_name_str);
    Py_DECREF(callback_method);

    gearman_return_t result =
        gearman_worker_add_function(
            self->g_Worker,
            function_name,
            timeout,
            _pygear_worker_function_mapper,
            self
        );

    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_worker_work(pygear_WorkerObject* self) {
    gearman_return_t result  = gearman_worker_work(self->g_Worker);
    if (PyErr_Occurred()) {
        return NULL;
    }
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_worker_echo(pygear_WorkerObject* self, PyObject* args) {
    char* workload;
    int workload_size;
    if (!PyArg_ParseTuple(args, "s#", &workload, &workload_size)) {
        return NULL;
    }
    gearman_return_t result =
        gearman_worker_echo(
            self->g_Worker,
            workload,
            workload_size
        );
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

/* Return value: New reference */
static PyObject* pygear_worker_id(pygear_WorkerObject* self) {
    return Py_BuildValue("i", gearman_worker_id(self->g_Worker));
}

static PyObject* pygear_worker_set_identifier(pygear_WorkerObject* self, PyObject* args) {
    char* id;
    int id_size;
    if (!PyArg_ParseTuple(args, "s#", &id, &id_size)) {
        return NULL;
    }
    gearman_return_t result = gearman_worker_set_identifier(self->g_Worker, id, id_size);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_worker_set_namespace(pygear_WorkerObject* self, PyObject* args) {
    char* namespace_key;
    int namespace_key_size;
    if (!PyArg_ParseTuple(args, "s#", &namespace_key, &namespace_key_size)) {
        return NULL;
    }
    gearman_worker_set_namespace(self->g_Worker, namespace_key, namespace_key_size);
    Py_RETURN_NONE;
}

/* Return value: New reference */
static PyObject* pygear_worker_namespace(pygear_WorkerObject* self) {
    const char* namespace = gearman_worker_namespace(self->g_Worker);
    return PyString_FromString(namespace);
}
