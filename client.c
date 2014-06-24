#include "client.h"

/*
 * Class constructor / destructor methods
 */

PyObject* Client_new(PyTypeObject *type, PyObject *args, PyObject *kwds){
    pygear_ClientObject* self;

    self = (pygear_ClientObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->g_Client = NULL;
    }

    return (PyObject *)self;
}

int Client_init(pygear_ClientObject *self, PyObject *args, PyObject *kwds){
    self->g_Client = gearman_client_create(NULL);
    if (self->g_Client == NULL) {
        return 1;
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

    return 0;
}

void Client_dealloc(pygear_ClientObject* self){
    if (self->g_Client){
        gearman_client_free(self->g_Client);
        self->g_Client = NULL;
    }

    // Decrement the reference count for callback functions, if they are set
    Py_XDECREF(self->cb_workload);
    Py_XDECREF(self->cb_created);
    Py_XDECREF(self->cb_data);
    Py_XDECREF(self->cb_warning);
    Py_XDECREF(self->cb_status);
    Py_XDECREF(self->cb_complete);
    Py_XDECREF(self->cb_exception);
    Py_XDECREF(self->cb_fail);

    self->ob_type->tp_free((PyObject*)self);
}

/********************
 * Instance methods *
 ********************
 */

/**
 * Clone a worker structure.
 *
 * @param[in] worker Caller allocated structure, or NULL to allocate one.
 * @param[in] from Structure to use as a source to clone from.
 * @return Same return as gearman_worker_create().
 */
static PyObject* pygear_client_clone(pygear_ClientObject* self){
    pygear_ClientObject* python_client = (pygear_ClientObject*) _PyObject_New(&pygear_ClientType);
    python_client->g_Client = gearman_client_clone(NULL, self->g_Client);
    return Py_BuildValue("O", python_client);
}

/*
 * Server management
 */
static PyObject* pygear_client_add_server(pygear_ClientObject* self, PyObject* args){
    char* host;
    int port;
    if (!PyArg_ParseTuple(args, "zi", &host, &port)){
        return NULL;
    }
    gearman_return_t result = gearman_client_add_server(self->g_Client, host, port);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_client_add_servers(pygear_ClientObject* self, PyObject* args){
    char* servers;
    if (!PyArg_ParseTuple(args, "z", &servers)){
        return NULL;
    }
    gearman_return_t result = gearman_client_add_servers(self->g_Client, servers);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/*
 * Task Management
 */

/**
 * Add a task to be run in parallel.
 *
 * @param[in] function_name The name of the function to run.
 * @param[in] workload The workload to pass to the function when it is run.
 * @param[in] unique Optional unique job identifier
 * @return A tuple of return code, Task structure. In the case of an error, Task
 *  will be None
 */
#define CLIENT_ADD_TASK(TASKTYPE) \
static PyObject* pygear_client_add_task##TASKTYPE(pygear_ClientObject* self, PyObject* args, PyObject* kwargs){ \
    /* Mandatory arguments*/ \
    char* function_name; \
    char* workload; \
    int workload_size; \
    gearman_return_t ret; \
\
    /* Optional arguments */ \
    char* unique = NULL; \
    static char* kwlist[] = {"function", "workload", "unique", NULL}; \
\
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ss#|s", kwlist, \
        &function_name, &workload, &workload_size, &unique)){ \
        return NULL; \
    } \
    gearman_task_st* new_task = gearman_client_add_task##TASKTYPE(self->g_Client, \
                                                        NULL, \
                                                        self, \
                                                        function_name, \
                                                        unique, \
                                                        workload, \
                                                        workload_size, \
                                                        &ret); \
\
    pygear_TaskObject* python_task = (pygear_TaskObject*) _PyObject_New(&pygear_TaskType); \
    python_task->g_Task = new_task; \
    if (_pygear_check_and_raise_exn(ret)){ \
        return NULL; \
    } \
    return Py_BuildValue("O", python_task); \
}

CLIENT_ADD_TASK()
CLIENT_ADD_TASK(_background)
CLIENT_ADD_TASK(_high)
CLIENT_ADD_TASK(_high_background)
CLIENT_ADD_TASK(_low)
CLIENT_ADD_TASK(_low_background)


/**
 * Run a single task and return an allocated result.
 *
 * @param[in] function_name The name of the function to run.
 * @param[in] unique Optional unique job identifier, or NULL for a new UUID.
 * @param[in] workload_size Size of the workload.
 * @param[in] workload The workload to pass to the function when it is run.
 * @param[out] result_size The size of the data being returned.
 * @param[out] ret_ptr Standard gearman return value. In the case of
 *  GEARMAN_WORK_DATA, GEARMAN_WORK_WARNING, or GEARMAN_WORK_STATUS, the caller
 *  should take any actions to handle the event and then call this function
 *  again. This may happen multiple times until a GEARMAN_WORK_ERROR,
 *  GEARMAN_WORK_FAIL, or GEARMAN_SUCCESS (work complete) is returned. For
 *  GEARMAN_WORK_DATA or GEARMAN_WORK_WARNING, the result_size will be set to
 *  the intermediate data chunk being returned and an allocated data buffer
 *  will be returned. For GEARMAN_WORK_STATUS, the caller can use
 *  gearman_client_do_status() to get the current tasks status.
 * @return The result allocated by the library, this needs to be freed when the
 *  caller is done using it.
 */

#define CLIENT_DO(DOTYPE) \
static PyObject* pygear_client_do##DOTYPE(pygear_ClientObject* self, PyObject* args, PyObject* kwargs){ \
    /* Mandatory arguments*/ \
    char* function_name; \
    char* workload; \
    int workload_size; \
\
    /* Optional arguments */ \
    char* unique = NULL; \
    static char* kwlist[] = {"function", "workload", "unique", NULL}; \
\
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ss#|s", kwlist, \
        &function_name, &workload, &workload_size, &unique)){ \
        return NULL; \
    } \
\
    size_t result_size; \
    gearman_return_t ret_ptr; \
\
    void* work_result = gearman_client_do##DOTYPE(self->g_Client, \
                                          function_name, \
                                          unique, \
                                          workload, workload_size, \
                                          &result_size, \
                                          &ret_ptr); \
    if (_pygear_check_and_raise_exn(ret_ptr)){ \
        return NULL; \
    } \
    return Py_BuildValue("s#", work_result, result_size); \
}

#define CLIENT_DO_BACKGROUND(DOTYPE) \
static PyObject* pygear_client_do##DOTYPE##_background(pygear_ClientObject* self, PyObject* args, PyObject* kwargs){ \
    /* Mandatory arguments*/ \
    char* function_name; \
    PyObject* workload; \
    char* workload_string; \
    Py_ssize_t workload_size; \
\
    /* Optional arguments */ \
    char* unique = NULL; \
    static char* kwlist[] = {"function", "workload", "unique", NULL}; \
\
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO|s", kwlist, \
        &function_name, &workload, &unique)){ \
        return NULL; \
    } \
\
    char* job_handle = malloc(sizeof(char) * GEARMAN_JOB_HANDLE_SIZE); \
    PyObject* pickled_input = PyObject_CallMethod(self->pickle, "dumps", "O", workload); \
    PyString_AsStringAndSize(pickled_input, &workload_string, &workload_size); \
\
    gearman_return_t work_result = gearman_client_do##DOTYPE##_background( \
        self->g_Client, \
        function_name, \
        unique, \
        workload_string, workload_size, \
        job_handle); \
    if (_pygear_check_and_raise_exn(work_result)){ \
        return NULL; \
    } \
    return Py_BuildValue("i", work_result); \
}

CLIENT_DO()
CLIENT_DO(_high)
CLIENT_DO(_low)

CLIENT_DO_BACKGROUND()
CLIENT_DO_BACKGROUND(_high)
CLIENT_DO_BACKGROUND(_low)


static PyObject* pygear_client_execute(pygear_ClientObject* self, PyObject* args, PyObject* kwargs){
    // Mandatory arguments
    char* function_name;
    char* workload;
    int workload_size;

    // Optional arguments
    char* unique = NULL;
    char* name = NULL;

    static char* kwlist[] = {"function", "workload", "unique", "name", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ss#|ss", kwlist,
        &function_name, &workload, &workload_size, &unique, &name)){
        return NULL;
    }
    gearman_argument_t arguments = gearman_argument_make(
        name, (name ? strlen(name) : 0),
        workload, workload_size
    );

    gearman_task_st* new_task = gearman_execute(
        self->g_Client,
        function_name, strlen(function_name),
        unique, (unique? strlen(unique) : 0),
        NULL, // Workload
        &arguments,
        NULL
    );

    if (new_task == NULL){
        _pygear_check_and_raise_exn(gearman_client_errno(self->g_Client));
        return NULL;
    }

    pygear_TaskObject* python_task = (pygear_TaskObject*) _PyObject_New(&pygear_TaskType);
    python_task->g_Task = new_task;

    if (_pygear_check_and_raise_exn(gearman_task_return(new_task))){
        return NULL;
    }

    gearman_result_st *result= gearman_task_result(new_task);
    int result_size = gearman_result_size(result);
    const char* result_data = gearman_result_value(result);
    return Py_BuildValue("s#", result_data, result_size);
}

static PyObject* pygear_client_run_tasks(pygear_ClientObject* self){
    if (_pygear_check_and_raise_exn(gearman_client_run_tasks(self->g_Client))){
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_client_wait(pygear_ClientObject* self){
    if (_pygear_check_and_raise_exn(gearman_client_wait(self->g_Client))){
        return NULL;
    }
    Py_RETURN_NONE;
}

/*
 * Callback function management
 */

#define CALLBACK_WRAPPER(CB) gearman_return_t pygear_client_wrap_callback_##CB(gearman_task_st* gear_task) { \
    pygear_ClientObject* client = (pygear_ClientObject*) gearman_task_context(gear_task); \
    if (!client->cb_##CB){ \
        return GEARMAN_SUCCESS; \
    } \
    /* Need to lock the GIL to avoid undefined behaviour */ \
    PyGILState_STATE gstate = PyGILState_Ensure(); \
    pygear_TaskObject* python_task = (pygear_TaskObject*) _PyObject_New(&pygear_TaskType); \
    python_task->g_Task = gear_task; \
    PyObject* callback_return = PyObject_CallFunction(client->cb_##CB, "O", python_task); \
    if (!callback_return){ \
        fprintf(stderr, "Callback function failed!\n"); \
    } \
    /* Release the thread */ \
    PyGILState_Release(gstate); \
    return GEARMAN_SUCCESS; \
}

#define CALLBACK_SETTER(CB) static PyObject* pygear_client_set_##CB##_fn(pygear_ClientObject* self, PyObject* args){ \
    PyObject* callback_fn; \
    if (!PyArg_ParseTuple(args, "O", &callback_fn)){ \
        return NULL; \
    } \
    Py_INCREF(callback_fn); \
    if (self->cb_##CB){ \
        Py_DECREF(self->cb_##CB); \
    } \
    self->cb_##CB = callback_fn; \
    gearman_client_set_##CB##_fn(self->g_Client, pygear_client_wrap_callback_##CB);\
    Py_RETURN_NONE; \
}

#define CALLBACK_HANDLE(CB) CALLBACK_WRAPPER(CB) CALLBACK_SETTER(CB)

CALLBACK_HANDLE(workload)
CALLBACK_HANDLE(created)
CALLBACK_HANDLE(data)
CALLBACK_HANDLE(warning)
CALLBACK_HANDLE(status)
CALLBACK_HANDLE(complete)
CALLBACK_HANDLE(exception)
CALLBACK_HANDLE(fail)


/*
 *
 */

#define CLIENT_OPT_NON_BLOCKING "non_blocking"
#define CLIENT_OPT_UNBUFFERED_RESULT "unbuffered_result"
#define CLIENT_OPT_FREE_TASKS "free_tasks"
#define CLIENT_OPT_GENERATE_UNIQUE "generate_unique"

static PyObject* pygear_client_set_options(pygear_ClientObject* self, PyObject* args, PyObject* kwargs){
    static char *kwlist[] = {
        CLIENT_OPT_NON_BLOCKING,
        CLIENT_OPT_UNBUFFERED_RESULT,
        CLIENT_OPT_FREE_TASKS,
        CLIENT_OPT_GENERATE_UNIQUE,
        NULL
    };

    static int options_t_value[] = {
        GEARMAN_CLIENT_NON_BLOCKING,
        GEARMAN_CLIENT_UNBUFFERED_RESULT,
        GEARMAN_CLIENT_FREE_TASKS,
        GEARMAN_CLIENT_GENERATE_UNIQUE
    };

    int client_options[4];

    if (! PyArg_ParseTupleAndKeywords(args, kwargs, "|iiii", kwlist,
                                      &client_options[0],
                                      &client_options[1],
                                      &client_options[2],
                                      &client_options[3]
                                      )){
        return NULL;
    }

    int i;
    for (i=0; i < 4; i++){
        if (client_options[i]){
            gearman_client_add_options(self->g_Client, options_t_value[i]);
        } else {
            gearman_client_remove_options(self->g_Client, options_t_value[i]);
        }
    }

    Py_RETURN_NONE;
}

static PyObject* pygear_client_get_options(pygear_ClientObject* self){
    static int options_t_value[] = {
        GEARMAN_CLIENT_NON_BLOCKING,
        GEARMAN_CLIENT_UNBUFFERED_RESULT,
        GEARMAN_CLIENT_FREE_TASKS,
        GEARMAN_CLIENT_GENERATE_UNIQUE
    };

    PyObject* client_options[4];
    int i;
    for (i=0; i < 4; i++){
        client_options[i] = (gearman_client_has_option(self->g_Client, options_t_value[i]) ? Py_True : Py_False);
    }
    PyObject* option_dictionary = Py_BuildValue(
        "{s:O, s:O, s:O, s:O}",
        CLIENT_OPT_NON_BLOCKING, client_options[0],
        CLIENT_OPT_UNBUFFERED_RESULT, client_options[1],
        CLIENT_OPT_FREE_TASKS, client_options[2],
        CLIENT_OPT_GENERATE_UNIQUE, client_options[3]
    );
    return option_dictionary;
}
