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

#include "client.h"

/*
 * Class constructor / destructor methods
 */

int Client_init(pygear_ClientObject* self, PyObject* args, PyObject*kwds) {
    self->g_Client = gearman_client_create(NULL);
    self->serializer = PyImport_ImportModule(PYTHON_SERIALIZER);
    if (self->serializer == NULL) {
        PyObject* err_string = PyString_FromFormat("Failed to import '%s'", PYTHON_SERIALIZER);
        PyErr_SetObject(PyExc_ImportError, err_string);
        Py_XDECREF(err_string);
        return -1;
    }
    if (self->g_Client == NULL) {
        PyErr_SetString(PyGearExn_ERROR, "Failed to create internal gearman client structure");
        return -1;
    }
    // Callbacks
    self->cb_workload = NULL;
    self->cb_created = NULL;
    self->cb_data = NULL;
    self->cb_warning = NULL;
    self->cb_status = NULL;
    self->cb_complete = NULL;
    self->cb_exception = NULL;
    self->cb_fail = NULL;
    self->cb_log = NULL;
    return 0;
}

int Client_traverse(pygear_ClientObject* self, visitproc visit, void* arg) {
    Py_VISIT(self->cb_workload);
    Py_VISIT(self->cb_created);
    Py_VISIT(self->cb_data);
    Py_VISIT(self->cb_warning);
    Py_VISIT(self->cb_status);
    Py_VISIT(self->cb_complete);
    Py_VISIT(self->cb_exception);
    Py_VISIT(self->cb_fail);
    Py_VISIT(self->cb_log);
    Py_VISIT(self->serializer);
    return 0;
}

int Client_clear(pygear_ClientObject* self) {
    Py_CLEAR(self->cb_workload);
    Py_CLEAR(self->cb_created);
    Py_CLEAR(self->cb_data);
    Py_CLEAR(self->cb_warning);
    Py_CLEAR(self->cb_status);
    Py_CLEAR(self->cb_complete);
    Py_CLEAR(self->cb_exception);
    Py_CLEAR(self->cb_fail);
    Py_CLEAR(self->cb_log);
    Py_CLEAR(self->serializer);
    return 0;
}

void Client_dealloc(pygear_ClientObject* self) {
    if (self->g_Client) {
        gearman_client_free(self->g_Client);
        self->g_Client = NULL;
    }
    Client_clear(self);
    self->ob_type->tp_free((PyObject*)self);
}


/********************
 * Instance methods *
 ********************
 */

static PyObject* pygear_client_add_server(pygear_ClientObject* self, PyObject* args) {
    char* host;
    int port;
    if (!PyArg_ParseTuple(args, "zi", &host, &port)) {
        return NULL;
    }
    gearman_return_t result = gearman_client_add_server(self->g_Client, host, port);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    // gearman_client_set_exception_fn() will only be called if exceptions are enabled on the server
    const char *EXCEPTIONS = "exceptions";
    gearman_client_set_server_option(self->g_Client, EXCEPTIONS, strlen(EXCEPTIONS));
    Py_RETURN_NONE;
}


static PyObject* pygear_client_add_servers(pygear_ClientObject* self, PyObject* args) {
    PyObject* server_list;
    if (!PyArg_ParseTuple(args, "O", &server_list)) {
        return NULL;
    }
    if (!PyList_Check(server_list)) {
        PyTypeObject* arg_type = (PyTypeObject*) PyObject_Type(server_list);
        char* err_base = "Client.add_servers expected list, got ";
        char* err_string = malloc(sizeof(char) * (strlen(err_base) + strlen(arg_type->tp_name) + 1));
        sprintf(err_string, "%s%s", err_base, arg_type->tp_name);
        PyErr_SetString(PyExc_TypeError, err_string);
        if (err_string != NULL) {
            free(err_string);
        }
        Py_XDECREF(arg_type);
        return NULL;
    }
    Py_ssize_t num_servers = PyList_Size(server_list);
    Py_ssize_t i;
    for (i = 0; i < num_servers; ++i) {
        char* server_string = PyString_AsString(PyList_GetItem(server_list, i));
        gearman_return_t result = gearman_client_add_servers(self->g_Client, server_string);
        if (_pygear_check_and_raise_exn(result)) {
            return NULL;
        }
    }
    const char *EXCEPTIONS="exceptions";
    gearman_client_set_server_option(self->g_Client, EXCEPTIONS, strlen(EXCEPTIONS));
    Py_RETURN_NONE;
}


#define CLIENT_ADD_TASK(TASKTYPE) \
static PyObject* pygear_client_add_task##TASKTYPE(pygear_ClientObject* self, PyObject* args, PyObject* kwargs) { \
    /* Parsing input arguments */ \
    char* function_name; \
    PyObject* workload; \
    char* unique = NULL; /* optional */ \
    static char* kwlist[] = {"function", "workload", "unique", NULL}; \
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO|s", kwlist, \
        &function_name, &workload, &unique)) { \
        return NULL; \
    } \
    /* Convert python input to string */ \
    PyObject* dumpstr = PyString_FromString("dumps"); \
    PyObject* pickled_input = PyObject_CallMethodObjArgs( \
        self->serializer, \
        dumpstr, \
        workload, \
        NULL \
    ); \
    Py_XDECREF(dumpstr); \
    if (!pickled_input) { \
        return NULL; \
    } \
    char* workload_string; \
    Py_ssize_t workload_size; \
    PyString_AsStringAndSize(pickled_input, &workload_string, &workload_size); \
    /* Py_XDECREF(pickled_input); */ \
    /* dealloc pickled_input will cause error because tasks are not sented until client_run_tasks() is called */ \
    /* Call gearman_add_task function */ \
    gearman_return_t ret; \
    gearman_task_st* new_task = gearman_client_add_task##TASKTYPE( \
        self->g_Client, \
        NULL, /* task */ \
        self, /* context */ \
        function_name, \
        unique, \
        workload_string, \
        workload_size, \
        &ret \
    ); \
    if (_pygear_check_and_raise_exn(ret)) { \
        return NULL; \
    } \
    /* Creating new python task */ \
    PyObject *argList = Py_BuildValue("(O, O)", Py_None, Py_None); \
    pygear_TaskObject* python_task = (pygear_TaskObject*) PyObject_CallObject((PyObject *) &pygear_TaskType, argList); \
    PyObject* method_result = PyObject_CallMethod((PyObject*) python_task, "set_serializer", "O", self->serializer); \
    if (!method_result) { \
        Py_XDECREF(argList); \
        Py_XDECREF(python_task); \
        return NULL; \
    } \
    Py_XDECREF(method_result); \
    Py_XDECREF(argList); \
    /* Return task */ \
    python_task->g_Task = new_task; \
    PyObject* result = Py_BuildValue("O", python_task); \
    python_task->g_Task = NULL; \
    Py_XDECREF(python_task); \
    return result; \
}


CLIENT_ADD_TASK()
CLIENT_ADD_TASK(_background)
CLIENT_ADD_TASK(_high)
CLIENT_ADD_TASK(_high_background)
CLIENT_ADD_TASK(_low)
CLIENT_ADD_TASK(_low_background)


static PyObject* pygear_client_add_task_status(pygear_ClientObject* self, PyObject* args) {
    char* job_handle;
    if (!PyArg_ParseTuple(args, "s", &job_handle)) {
        return NULL;
    }
    gearman_return_t gearman_return;
    gearman_task_st* new_task = gearman_client_add_task_status(
        self->g_Client,
        NULL,
        (void*) self,
        job_handle,
        &gearman_return
    );
    if (_pygear_check_and_raise_exn(gearman_return)) {
        return NULL;
    }
    pygear_TaskObject* python_task = (pygear_TaskObject*) _PyObject_New(&pygear_TaskType);
    if (!python_task){
        return NULL;
    }
    python_task->g_Task = new_task;
    PyObject* ret = Py_BuildValue("O", python_task);
    Py_XDECREF(python_task);
    return ret;
}


static PyObject* pygear_client_clear_fn(pygear_ClientObject* self) {
    gearman_client_clear_fn(self->g_Client);
    Py_XDECREF(self->cb_workload); self->cb_workload = NULL;
    Py_XDECREF(self->cb_created); self->cb_created = NULL;
    Py_XDECREF(self->cb_data); self->cb_data = NULL;
    Py_XDECREF(self->cb_warning); self->cb_warning = NULL;
    Py_XDECREF(self->cb_status); self->cb_status = NULL;
    Py_XDECREF(self->cb_complete); self->cb_complete = NULL;
    Py_XDECREF(self->cb_exception); self->cb_exception = NULL;
    Py_XDECREF(self->cb_fail); self->cb_fail = NULL;
    Py_RETURN_NONE;
}


static PyObject* pygear_client_clone(pygear_ClientObject* self) {
    PyObject* argList = NULL;
    pygear_ClientObject* python_client = NULL;
    PyObject* ret = NULL;
    argList = Py_BuildValue("(O, O)", Py_None, Py_None);
    python_client = (pygear_ClientObject*) PyObject_CallObject((PyObject *) &pygear_ClientType, argList);
    python_client->g_Client = gearman_client_clone(NULL, self->g_Client);
    ret = Py_BuildValue("O", python_client);
    Py_XDECREF(argList);
    Py_XDECREF(python_client);
    return ret;
}


#define CLIENT_DO(DOTYPE) \
static PyObject* pygear_client_do##DOTYPE(pygear_ClientObject* self, PyObject* args, PyObject* kwargs) { \
    /* Parsing input arguments */ \
    char* function_name; \
    PyObject* workload; \
    Py_ssize_t workload_size; \
    char* unique = NULL;  /* optional */ \
    static char* kwlist[] = {"function", "workload", "unique", NULL}; \
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO|s", kwlist, \
        &function_name, &workload, &unique)) { \
        return NULL; \
    } \
    /* Convert python input to string */ \
    PyObject* dumpstr = PyString_FromString("dumps"); \
    PyObject* pickled_input = PyObject_CallMethodObjArgs( \
        self->serializer, \
        dumpstr, \
        workload, \
        NULL \
    ); \
    Py_XDECREF(dumpstr); \
    char* workload_string; \
    PyString_AsStringAndSize(pickled_input, &workload_string, &workload_size); \
    /* Call gearman_do function */ \
    size_t result_size; \
    gearman_return_t ret; \
    void* work_result = gearman_client_do##DOTYPE( \
        self->g_Client, \
        function_name, \
        unique, \
        workload_string, \
        workload_size, \
        &result_size, \
        &ret); /* work_result must be freed later to avoid memory leak */ \
    Py_XDECREF(pickled_input); /* safely dealloc workload */ \
    if (_pygear_check_and_raise_exn(ret)) { \
        free(work_result); \
        return NULL; \
    } \
    /* Convert result to python format */ \
    PyObject* py_result = Py_BuildValue("s#", work_result, result_size); \
    free(work_result); \
    if (py_result == Py_None) { \
        Py_XDECREF(py_result); \
        Py_RETURN_NONE; \
    } \
    PyObject* ret_dict = PyObject_CallMethod(self->serializer, "loads", "O", py_result); \
    Py_XDECREF(py_result); \
    return ret_dict; \
}

CLIENT_DO()
CLIENT_DO(_high)
CLIENT_DO(_low)

#define CLIENT_DO_BACKGROUND(DOTYPE) \
static PyObject* pygear_client_do##DOTYPE##_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs) { \
    /* Parsing input arguments */ \
    char* function_name; \
    PyObject* workload; \
    Py_ssize_t workload_size; \
    char* unique = NULL; /* optional */ \
    static char* kwlist[] = {"function", "workload", "unique", NULL}; \
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO|s", kwlist, \
        &function_name, &workload, &unique)) { \
        return NULL; \
    } \
    /* Convert python input to string */ \
    PyObject* dumpstr = PyString_FromString("dumps"); \
    PyObject* pickled_input = PyObject_CallMethodObjArgs( \
        self->serializer, \
        dumpstr, \
        workload, \
        NULL \
    ); \
    Py_XDECREF(dumpstr); \
    char* workload_string; \
    PyString_AsStringAndSize(pickled_input, &workload_string, &workload_size); \
    /* Call libgearman function */ \
    char* job_handle = malloc(sizeof(char) * GEARMAN_JOB_HANDLE_SIZE); \
    gearman_return_t work_result = gearman_client_do##DOTYPE##_background( \
        self->g_Client, \
        function_name, \
        unique, \
        workload_string, \
        workload_size, \
        job_handle \
    ); \
    Py_XDECREF(pickled_input); /* safely dealloc workload */ \
    if (_pygear_check_and_raise_exn(work_result)) { \
        free(job_handle); \
        return NULL; \
    } \
    PyObject* ret = NULL; \
    if (job_handle != NULL) { \
        ret = Py_BuildValue("s", job_handle); \
        free(job_handle); \
    } \
    return ret; \
}

CLIENT_DO_BACKGROUND()
CLIENT_DO_BACKGROUND(_high)
CLIENT_DO_BACKGROUND(_low)

static PyObject* pygear_client_do_job_handle(pygear_ClientObject* self) {
    return Py_BuildValue("s", gearman_client_do_job_handle(self->g_Client));
}


// Deprecated
static PyObject* pygear_client_do_status(pygear_ClientObject* self) {
    unsigned numerator, denominator;
    gearman_client_do_status(self->g_Client, &numerator, &denominator);
    return Py_BuildValue("(I,I)", numerator, denominator);
}


static PyObject* pygear_client_echo(pygear_ClientObject* self, PyObject* args) {
    char* workload;
    unsigned workload_len;
    if (!PyArg_ParseTuple(args, "s#", &workload, &workload_len)) {
        return NULL;
    }
    gearman_return_t result = gearman_client_echo(self->g_Client, workload, workload_len);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject* pygear_client_errno(pygear_ClientObject* self) {
    return Py_BuildValue("i", gearman_client_errno(self->g_Client));
}


static PyObject* pygear_client_error(pygear_ClientObject* self) {
    return Py_BuildValue("s", gearman_client_error(self->g_Client));
}


static PyObject* pygear_client_error_code(pygear_ClientObject* self) {
    return Py_BuildValue("i", gearman_client_error_code(self->g_Client));
}


static PyObject* pygear_client_execute(pygear_ClientObject* self, PyObject* args, PyObject* kwargs) {
    /*
        This is an example of executing work using a combination of libgearman calls.
        http://gearman.info/libgearman/gearman_execute.html
    */
    PyObject* ret = NULL;
    // Mandatory arguments
    char* function_name;
    char* workload;
    int workload_size;
    // Optional arguments
    char* unique = NULL;
    char* name = NULL;
    static char* kwlist[] = {"function", "workload", "unique", "name", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ss#|ss", kwlist,
        &function_name, &workload, &workload_size, &unique, &name)) {
        return NULL;
    }
    // Generate the arguments for the function
    gearman_argument_t arguments = gearman_argument_make(
        name, (name ? strlen(name) : 0),
        workload, workload_size
    );
    // Execute the function
    // TODO: do we need to free new_task later?
    gearman_task_st* new_task = gearman_execute(
        self->g_Client,
        function_name, strlen(function_name),
        unique, (unique? strlen(unique) : 0),
        NULL, // workload
        &arguments,
        NULL // context
    );
    if (new_task == NULL) {
        if (_pygear_check_and_raise_exn(gearman_client_errno(self->g_Client))) {
            return NULL;
        }
    }
    // Convert task to python format
    PyObject* argList = Py_BuildValue("(O, O)", Py_None, Py_None);
    pygear_TaskObject* python_task = (pygear_TaskObject*) PyObject_CallObject((PyObject *) &pygear_TaskType, argList);
    python_task->g_Task = new_task;
    PyObject* method_result = PyObject_CallMethod((PyObject*) python_task, "set_serializer", "O", self->serializer);
    if (!method_result) {
        goto catch;
    }
    // Make sure the task was run successfully
    if (_pygear_check_and_raise_exn(gearman_task_return(new_task))) {
        goto catch;
    }
    // Make use of the result
    gearman_result_st* result = gearman_task_result(new_task);
    int result_size = gearman_result_size(result);
    const char* result_data = gearman_result_value(result);
    ret = Py_BuildValue("s#", result_data, result_size);
catch:
    Py_XDECREF(argList);
    Py_XDECREF(python_task);
    Py_XDECREF(method_result);
    return ret;
}


#define PYGEAR_CLIENT_NUM_OPTIONS              4
#define PYGEAR_CLIENT_OPT_NON_BLOCKING         "non_blocking"
#define PYGEAR_CLIENT_OPT_UNBUFFERED_RESULT    "unbuffered_result"
#define PYGEAR_CLIENT_OPT_FREE_TASKS           "free_tasks"
#define PYGEAR_CLIENT_OPT_GENERATE_UNIQUE      "generate_unique"

static PyObject* pygear_client_get_options(pygear_ClientObject* self) {
    static int options_t_value[] = {
        GEARMAN_CLIENT_NON_BLOCKING,
        GEARMAN_CLIENT_UNBUFFERED_RESULT,
        GEARMAN_CLIENT_FREE_TASKS,
        GEARMAN_CLIENT_GENERATE_UNIQUE
    };
    PyObject* client_options[PYGEAR_CLIENT_NUM_OPTIONS];
    int i;
    for (i=0; i < 4; i++){
        client_options[i] = (gearman_client_has_option(self->g_Client, options_t_value[i]) ? Py_True : Py_False);
    }
    PyObject* option_dict = Py_BuildValue(
        "{s:O, s:O, s:O, s:O}",
        PYGEAR_CLIENT_OPT_NON_BLOCKING, client_options[0],
        PYGEAR_CLIENT_OPT_UNBUFFERED_RESULT, client_options[1],
        PYGEAR_CLIENT_OPT_FREE_TASKS, client_options[2],
        PYGEAR_CLIENT_OPT_GENERATE_UNIQUE, client_options[3]
    );
    return option_dict;
}


static PyObject* pygear_client_job_status(pygear_ClientObject* self, PyObject* args) {
    gearman_job_handle_t job_handle;
    bool is_known, is_running;
    unsigned numerator, denominator;
    if (!PyArg_ParseTuple(args, "s", &job_handle)) {
        return NULL;
    }
    gearman_return_t result = gearman_client_job_status(
        self->g_Client,
        job_handle,
        &is_known, &is_running,
        &numerator, &denominator
    );
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    PyObject* status_dict = Py_BuildValue(
        "{s:O, s:O, s:I, s:I}",
        "is_known", (is_known ? Py_True : Py_False),
        "is_running", (is_running ? Py_True : Py_False),
        "numerator", numerator,
        "denominator", denominator
    );
    return status_dict;
}


static PyObject* pygear_client_remove_servers(pygear_ClientObject* self) {
    gearman_client_remove_servers(self->g_Client);
    Py_RETURN_NONE;
}


static PyObject* pygear_client_run_tasks(pygear_ClientObject* self) {
    gearman_return_t result = gearman_client_run_tasks(self->g_Client);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}


#define CALLBACK_WRAPPER(CB) gearman_return_t pygear_client_wrap_callback_##CB(gearman_task_st* gear_task) { \
    pygear_ClientObject* client = (pygear_ClientObject*) gearman_task_context(gear_task); \
    if (!client->cb_##CB) { \
        return GEARMAN_SUCCESS; \
    } \
    /* Need to lock the GIL to avoid undefined behaviour */ \
    PyGILState_STATE gstate = PyGILState_Ensure(); \
    PyObject *argList = Py_BuildValue("(O, O)", Py_None, Py_None); \
    pygear_TaskObject* python_task = (pygear_TaskObject*) PyObject_CallObject((PyObject *) &pygear_TaskType, argList); \
    PyObject* method_result = PyObject_CallMethod((PyObject*) python_task, "set_serializer", "O", client->serializer); \
    if (!method_result) { \
        PyErr_Print(); \
        Py_XDECREF(argList); \
        Py_XDECREF(python_task); \
        Py_XDECREF(method_result); \
        PyGILState_Release(gstate); \
        return GEARMAN_ERROR; \
    } \
    python_task->g_Task = gear_task; \
    PyObject* callback_return = PyObject_CallFunction(client->cb_##CB, "O", python_task); \
    if (!callback_return) { \
        if (PyErr_Occurred()) { \
            PyErr_Print(); \
        } \
    } \
    /* Release the thread */ \
    Py_XDECREF(argList); \
    python_task->g_Task = NULL; \
    Py_XDECREF(python_task); \
    Py_XDECREF(method_result); \
    Py_XDECREF(callback_return); \
    PyGILState_Release(gstate); \
    return GEARMAN_SUCCESS; \
}

#define CALLBACK_SETTER(CB) static PyObject* pygear_client_set_##CB##_fn(pygear_ClientObject* self, PyObject* args) { \
    PyObject* callback_fn; \
    PyObject* tmp; \
    if (!PyArg_ParseTuple(args, "O", &callback_fn)) { \
        return NULL; \
    } \
    tmp = self->cb_##CB; \
    Py_INCREF(callback_fn); \
    self->cb_##CB = callback_fn; \
    gearman_client_set_##CB##_fn(self->g_Client, pygear_client_wrap_callback_##CB); \
    Py_XDECREF(tmp); \
    Py_RETURN_NONE;  \
}

#define CALLBACK_HANDLE(CB) CALLBACK_WRAPPER(CB) CALLBACK_SETTER(CB)

CALLBACK_HANDLE(created)
CALLBACK_HANDLE(complete)
CALLBACK_HANDLE(data)
CALLBACK_HANDLE(exception)
CALLBACK_HANDLE(fail)
CALLBACK_HANDLE(status)
CALLBACK_HANDLE(warning)
CALLBACK_HANDLE(workload)


/* private method */
static void _pygear_client_log_fn_wrapper(const char* line, gearman_verbose_t verbose, void* context) {
    pygear_ClientObject* client = (pygear_ClientObject*) context;
    PyGILState_STATE gstate = PyGILState_Ensure();
    PyObject* result = PyObject_CallFunction(client->cb_log, "s", line);
    Py_XDECREF(result);
    PyGILState_Release(gstate);
}

static PyObject* pygear_client_set_log_fn(pygear_ClientObject* self, PyObject* args) {
    PyObject* function;
    PyObject* tmp;
    gearman_verbose_t verbose;
    if (!PyArg_ParseTuple(args, "Oi", &function, &verbose)) {
        return NULL;
    }
    tmp = self->cb_log;
    Py_INCREF(function);
    self->cb_log = function;
    gearman_client_set_log_fn(self->g_Client, _pygear_client_log_fn_wrapper, self, verbose);
    Py_XDECREF(tmp);
    Py_RETURN_NONE;
}


static PyObject* pygear_client_set_options(pygear_ClientObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = {
        PYGEAR_CLIENT_OPT_NON_BLOCKING,
        PYGEAR_CLIENT_OPT_FREE_TASKS,
        PYGEAR_CLIENT_OPT_UNBUFFERED_RESULT,
        PYGEAR_CLIENT_OPT_GENERATE_UNIQUE,
        NULL
    };
    static int options_t_value[] = {
        GEARMAN_CLIENT_NON_BLOCKING,
        GEARMAN_CLIENT_FREE_TASKS,
        GEARMAN_CLIENT_UNBUFFERED_RESULT,
        GEARMAN_CLIENT_GENERATE_UNIQUE
    };
    int client_options[PYGEAR_CLIENT_NUM_OPTIONS];
    if (! PyArg_ParseTupleAndKeywords(
        args, kwargs, "|iiii", kwlist,
        &client_options[0],
        &client_options[1],
        &client_options[2],
        &client_options[3]
    )) {
        return NULL;
    }
    int i;
    for (i = 0; i < PYGEAR_CLIENT_NUM_OPTIONS; ++i) {
        if (client_options[i]) {
            gearman_client_add_options(self->g_Client, options_t_value[i]);
        } else {
            gearman_client_remove_options(self->g_Client, options_t_value[i]);
        }
    }
    Py_RETURN_NONE;
}


static PyObject* pygear_client_set_serializer(pygear_ClientObject* self, PyObject* args) {
    PyObject* serializer;
    PyObject* tmp;
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
    tmp = self->serializer;
    Py_INCREF(serializer);
    self->serializer = serializer;
    Py_XDECREF(tmp);
    Py_RETURN_NONE;
}


static PyObject* pygear_client_set_timeout(pygear_ClientObject* self, PyObject* args) {
    int timeout;
    if (!PyArg_ParseTuple(args, "i", &timeout)) {
        return NULL;
    }
    gearman_client_set_timeout(self->g_Client, timeout);
    Py_RETURN_NONE;
}


static PyObject* pygear_client_timeout(pygear_ClientObject* self) {
    return Py_BuildValue("i", gearman_client_timeout(self->g_Client));
}


static PyObject* pygear_client_unique_status(pygear_ClientObject* self, PyObject* args) {
    char* unique;
    unsigned unique_len;
    if (!PyArg_ParseTuple(args, "s#", &unique, &unique_len)) {
        return NULL;
    }
    gearman_status_t status = gearman_client_unique_status(self->g_Client, unique, unique_len);
    if (_pygear_check_and_raise_exn(status.status_.mesg_.result_rc)) {
        return NULL;
    }
    PyObject* status_dict = Py_BuildValue(
        "{s:O, s:O, s:I, s:I}",
        "is_known", (status.status_.mesg_.is_known ? Py_True : Py_False),
        "is_running", (status.status_.mesg_.is_running ? Py_True : Py_False),
        "numerator", status.status_.mesg_.numerator,
        "denominator", status.status_.mesg_.denominator
    );
    return status_dict;
}


static PyObject* pygear_client_wait(pygear_ClientObject* self) {
    gearman_return_t result = gearman_client_wait(self->g_Client);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}
