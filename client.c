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
    self->pickle = PyImport_ImportModule("pickle");
    if (self->g_Client == NULL || self->pickle == NULL) {
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

    Py_XDECREF(self->pickle);

    self->ob_type->tp_free((PyObject*)self);
}

/********************
 * Instance methods *
 ********************
 */


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

    PyObject *argList = Py_BuildValue("(O, O)", Py_None, Py_None);
    pygear_TaskObject* python_task = (pygear_TaskObject*) PyObject_CallObject((PyObject *) &pygear_TaskType, argList);
    python_task->g_Task = new_task;

    if (_pygear_check_and_raise_exn(gearman_task_return(new_task))){
        return NULL;
    }

    gearman_result_st *result= gearman_task_result(new_task);
    int result_size = gearman_result_size(result);
    const char* result_data = gearman_result_value(result);
    return Py_BuildValue("s#", result_data, result_size);
}

/**
 * Clone a client structure.
 *
 * @param[in] client Caller allocated structure, or NULL to allocate one.
 * @param[in] from Structure to use as a source to clone from.
 * @return Same return as gearman_client_create().
 */
static PyObject* pygear_client_clone(pygear_ClientObject* self){
    PyObject *argList = Py_BuildValue("(O, O)", Py_None, Py_None);
    pygear_ClientObject* python_client = (pygear_ClientObject*) PyObject_CallObject((PyObject *) &pygear_ClientType, argList);
    python_client->g_Client = gearman_client_clone(NULL, self->g_Client);
    return Py_BuildValue("O", python_client);
}

/**
 * See gearman_error() for details.
 */
static PyObject* pygear_client_error(pygear_ClientObject* self){
    return Py_BuildValue("s", gearman_client_error(self->g_Client));
}

static PyObject* pygear_client_error_code(pygear_ClientObject* self){
    return Py_BuildValue("i", gearman_client_error_code(self->g_Client));
}

/**
 * See gearman_errno() for details.
 */
static PyObject* pygear_client_errno(pygear_ClientObject* self){
    return Py_BuildValue("i", gearman_client_errno(self->g_Client));
}

/**
 * Set options for a client structure.
 *
 * @param[in] client Structure previously initialized with
 *  gearman_client_create() or gearman_client_clone().
 * @param[in] options Available options for client structures.
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

/**
 * Get options for a client structure.
 *
 * @return A dictionary of options and their values
 */
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

/**
 * See gearman_universal_timeout() for details.
 */
static PyObject* pygear_client_timeout(pygear_ClientObject* self){
    return Py_BuildValue("i", gearman_client_timeout(self->g_Client));
}

/**
 * See gearman_universal_set_timeout() for details.
 */
static PyObject* pygear_client_set_timeout(pygear_ClientObject* self, PyObject* args){
    int timeout;
    if (!PyArg_ParseTuple(args, "i", &timeout)){
        return NULL;
    }
    gearman_client_set_timeout(self->g_Client, timeout);
    Py_RETURN_NONE;
}

/**
 * See gearman_set_log_fn() for details.
 */

static void _pygear_log_fn_wrapper(const char* line, gearman_verbose_t verbose, void* context){
    pygear_ClientObject* client = (pygear_ClientObject*) context;
    PyGILState_STATE gstate = PyGILState_Ensure();
    PyObject* callback_return = PyObject_CallFunction(client->cb_log, "s", line);
    if (callback_return == NULL){
        fprintf(stderr, "Callback function failed!\n");
    }
    PyGILState_Release(gstate);
}

static PyObject* pygear_client_set_log_fn(pygear_ClientObject* self, PyObject* args){
    PyObject* callback_fn;
    gearman_verbose_t verbose;
    if (!PyArg_ParseTuple(args, "Oi", &callback_fn, &verbose)){
        return NULL;
    }

    Py_XDECREF(self->cb_log);
    self->cb_log = callback_fn;
    Py_INCREF(self->cb_log);

    gearman_client_set_log_fn(self->g_Client, _pygear_log_fn_wrapper, self, verbose);

    Py_RETURN_NONE;
}

/**
 * Add a job server to a client. This goes into a list of servers that can be
 * used to run tasks. No socket I/O happens here, it is just added to a list.
 *
 * @param[in] client Structure previously initialized with
 *  gearman_client_create() or gearman_client_clone().
 * @param[in] host Hostname or IP address (IPv4 or IPv6) of the server to add.
 * @param[in] port Port of the server to add.
 * @return Standard gearman return value.
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

/**
 * Add a list of job servers to a client. The format for the server list is:
 * SERVER[:PORT][,SERVER[:PORT]]...
 * Some examples are:
 * 10.0.0.1,10.0.0.2,10.0.0.3
 * localhost234,jobserver2.domain.com:7003,10.0.0.3
 *
 * @param[in] client Structure previously initialized with
 *  gearman_client_create() or gearman_client_clone().
 * @param[in] servers Server list described above.
 * @return Standard gearman return value.
 */
static PyObject* pygear_client_add_servers(pygear_ClientObject* self, PyObject* args){
    PyObject* server_list;
    if (!PyArg_ParseTuple(args, "O", &server_list)){
        return NULL;
    }
    if (!PyList_Check(server_list)){
        PyTypeObject* arg_type = (PyTypeObject*) PyObject_Type(server_list);
        char* err_base = "Client.add_servers expected list, got ";
        char* err_string = malloc(sizeof(char) * (strlen(err_base) + strlen(arg_type->tp_name) + 1));
        sprintf(err_string, "%s%s", err_base, arg_type->tp_name);
        PyErr_SetString(PyExc_TypeError, err_string);
        return NULL;
    }

    Py_ssize_t num_servers = PyList_Size(server_list);
    Py_ssize_t i;
    for (i=0; i < num_servers; i++){
        char* srv_string = PyString_AsString(PyList_GetItem(server_list, i));
        gearman_return_t result = gearman_client_add_servers(self->g_Client, srv_string);

        if (_pygear_check_and_raise_exn(result)){
            return NULL;
        }
    }
    Py_RETURN_NONE;
}

/**
 * Remove all servers currently associated with the client.
 *
 * @param[in] client Structure previously initialized with
 *  gearman_client_create() or gearman_client_clone().
 */
static PyObject* pygear_client_remove_servers(pygear_ClientObject* self){
    gearman_client_remove_servers(self->g_Client);
    Py_RETURN_NONE;
}

/**
 * When in non-blocking I/O mode, wait for activity from one of the servers.
 *
 * @param[in] client Structure previously initialized with
 *  gearman_client_create() or gearman_client_clone().
 * @return Standard gearman return value.
 */
static PyObject* pygear_client_wait(pygear_ClientObject* self){
    if (_pygear_check_and_raise_exn(gearman_client_wait(self->g_Client))){
        return NULL;
    }
    Py_RETURN_NONE;
}


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
    size_t result_size; \
    gearman_return_t ret_ptr; \
    PyObject* pickled_input = PyObject_CallMethod(self->pickle, "dumps", "O", workload); \
    PyString_AsStringAndSize(pickled_input, &workload_string, &workload_size); \
\
    void* work_result = gearman_client_do##DOTYPE(self->g_Client, \
                                          function_name, \
                                          unique, \
                                          workload_string, workload_size, \
                                          &result_size, \
                                          &ret_ptr); \
    if (_pygear_check_and_raise_exn(ret_ptr)){ \
        return NULL; \
    } \
    PyObject* py_result = Py_BuildValue("s#", work_result, result_size); \
    if (py_result == Py_None){ \
        Py_RETURN_NONE; \
    } \
    return PyObject_CallMethod(self->pickle, "loads", "O", py_result); \
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


/**
 * Get the job handle for the running task. This should be used between
 * repeated gearman_client_do() (and related) calls to get information.
 *
 * @param[in] client Structure previously initialized with
 *  gearman_client_create() or gearman_client_clone().
 * @return Pointer to static buffer in the client structure that holds the job
 *  handle.
 */
static PyObject* pygear_client_do_job_handle(pygear_ClientObject* self){
    return Py_BuildValue("s", gearman_client_do_job_handle(self->g_Client));
}

// Deprecatd
static PyObject* pygear_client_do_status(pygear_ClientObject* self){
    unsigned numerator, denominator;
    gearman_client_do_status(self->g_Client, &numerator, &denominator);
    return Py_BuildValue("(I,I)", numerator, denominator);
}

/**
 * Get the status for a backgound job.
 *
 * @param[in] client Structure previously initialized with
 *  gearman_client_create() or gearman_client_clone().
 * @param[in] job_handle The job handle to get status for.
 * @param[out] is_known Optional parameter to store the known status in.
 * @param[out] is_running Optional parameter to store the running status in.
 * @param[out] numerator Optional parameter to store the numerator in.
 * @param[out] denominator Optional parameter to store the denominator in.
 * @return Standard gearman return value.
 */
static PyObject* pygear_client_job_status(pygear_ClientObject* self, PyObject* args){
    gearman_job_handle_t job_handle;
    bool is_known, is_running;
    unsigned numerator, denominator;

    if (!PyArg_ParseTuple(args, "s", &job_handle)){
        return NULL;
    }

    gearman_return_t result =
        gearman_client_job_status(
            self->g_Client,
            job_handle,
            &is_known, &is_running,
            &numerator, &denominator
        );

    if (_pygear_check_and_raise_exn(result)){
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


static PyObject* pygear_client_unique_status(pygear_ClientObject* self, PyObject* args){
    char* unique;
    unsigned unique_len;

    if (!PyArg_ParseTuple(args, "s#", &unique, &unique_len)){
        return NULL;
    }

    gearman_status_t status = gearman_client_unique_status(self->g_Client, unique, unique_len);

    if (_pygear_check_and_raise_exn(status.status_.mesg_.result_rc)){
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

/**
 * Send data to all job servers to see if they echo it back. This is a test
 * function to see if the job servers are responding properly.
 *
 * @param[in] client Structure previously initialized with
 *  gearman_client_create() or gearman_client_clone().
 * @param[in] workload The workload to ask the server to echo back.
 * @param[in] workload_size Size of the workload.
 * @return Standard gearman return value.
 */
static PyObject* pygear_client_echo(pygear_ClientObject* self, PyObject* args){
    char* workload;
    unsigned workload_len;

    if (!PyArg_ParseTuple(args, "s#", &workload, &workload_len)){
        return NULL;
    }
    gearman_return_t result = gearman_client_echo(self->g_Client, workload, workload_len);

    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }

    Py_RETURN_NONE;
}

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
    PyObject* workload; \
    gearman_return_t ret; \
\
    /* Optional arguments */ \
    char* unique = NULL; \
    static char* kwlist[] = {"function", "workload", "unique", NULL}; \
\
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO|s", kwlist, \
        &function_name, &workload, &unique)){ \
        return NULL; \
    } \
    PyObject* pickled_input = PyObject_CallMethod(self->pickle, "dumps", "O", workload); \
    char* workload_string; Py_ssize_t workload_size; \
    PyString_AsStringAndSize(pickled_input, &workload_string, &workload_size); \
    gearman_task_st* new_task = gearman_client_add_task##TASKTYPE(self->g_Client, \
                                                        NULL, \
                                                        self, \
                                                        function_name, \
                                                        unique, \
                                                        workload_string, \
                                                        workload_size, \
                                                        &ret); \
\
    PyObject *argList = Py_BuildValue("(O, O)", Py_None, Py_None); \
    pygear_TaskObject* python_task = (pygear_TaskObject*) PyObject_CallObject((PyObject *) &pygear_TaskType, argList); \
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
 * Add task to get the status for a backgound task in parallel.
 *
 * @param[in] client Structure previously initialized with
 *  gearman_client_create() or gearman_client_clone().
 * @param[in] task Caller allocated structure, or NULL to allocate one.
 * @param[in] context Application context to associate with the task.
 * @param[in] job_handle The job handle to get status for.
 * @param[out] ret_ptr Standard gearman return value.
 * @return On success, a pointer to the (possibly allocated) structure. On
 *  failure this will be NULL.
 */

static PyObject* pygear_client_add_task_status(pygear_ClientObject* self, PyObject* args){
    char* job_handle;

    if (!PyArg_ParseTuple(args, "s", &job_handle)){
        return NULL;
    }

    gearman_return_t gearman_return;

    gearman_task_st* new_task = gearman_client_add_task_status(
        self->g_Client,
        NULL,
        (void*) self,
        job_handle,
        &gearman_return);

    if (_pygear_check_and_raise_exn(gearman_return)){
        return NULL;
    }

    pygear_TaskObject* python_task = (pygear_TaskObject*) _PyObject_New(&pygear_TaskType);
    if (!python_task){
        return NULL;
    }
    python_task->g_Task = new_task;
    return Py_BuildValue("O", python_task);
}

/**
 * Callback function when a job has been created for a task.
 *
 * @param[in] client Structure previously initialized with
 *  gearman_client_create() or gearman_client_clone().
 * @param[in] function Function to call.
 */
#define CALLBACK_WRAPPER(CB) gearman_return_t pygear_client_wrap_callback_##CB(gearman_task_st* gear_task) { \
    pygear_ClientObject* client = (pygear_ClientObject*) gearman_task_context(gear_task); \
    if (!client->cb_##CB){ \
        return GEARMAN_SUCCESS; \
    } \
    /* Need to lock the GIL to avoid undefined behaviour */ \
    PyGILState_STATE gstate = PyGILState_Ensure(); \
    PyObject *argList = Py_BuildValue("(O, O)", Py_None, Py_None); \
    pygear_TaskObject* python_task = (pygear_TaskObject*) PyObject_CallObject((PyObject *) &pygear_TaskType, argList); \
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

/**
 * Clear all task callback functions.
 *
 * @param[in] client Structure previously initialized with
 *  gearman_client_create() or gearman_client_clone().
 */
static PyObject* pygear_client_clear_fn(pygear_ClientObject* self){
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

/**
 * Run tasks that have been added in parallel.
 *
 * @param[in] client Structure previously initialized with
 *  gearman_client_create() or gearman_client_clone().
 * @return Standard gearman return value.
 */
static PyObject* pygear_client_run_tasks(pygear_ClientObject* self){
    if (_pygear_check_and_raise_exn(gearman_client_run_tasks(self->g_Client))){
        return NULL;
    }
    Py_RETURN_NONE;
}
