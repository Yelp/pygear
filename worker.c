#include "worker.h"

/*
 * Class constructor / destructor methods
 */

PyObject* Worker_new(PyTypeObject *type, PyObject *args, PyObject *kwds){
    pygear_WorkerObject* self;

    self = (pygear_WorkerObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->g_Worker = NULL;
        self->g_FunctionMap = NULL;
    }

    return (PyObject *)self;
}

int Worker_init(pygear_WorkerObject *self, PyObject *args, PyObject *kwds){
    self->g_Worker = gearman_worker_create(NULL);
    self->g_FunctionMap = PyDict_New();
    self->pickle = PyImport_ImportModule("pickle");
    if (self->g_Worker == NULL || self->g_FunctionMap == NULL || self->pickle == NULL){
        return 1;
    }
    return 0;
}

void Worker_dealloc(pygear_WorkerObject* self){
    if (self->g_Worker){
        gearman_worker_free(self->g_Worker);
        self->g_Worker = NULL;
    }

    Py_XDECREF(self->g_FunctionMap);
    Py_XDECREF(self->pickle);

    self->ob_type->tp_free((PyObject*)self);
}

/*
 * Instance Methods
 */

/**
 * Clone a worker structure.
 *
 * @param[in] worker Caller allocated structure, or NULL to allocate one.
 * @param[in] from Structure to use as a source to clone from.
 * @return Same return as gearman_worker_create().
 */
static PyObject* pygear_worker_clone(pygear_WorkerObject* self){
    PyObject *argList = Py_BuildValue("(O, O)", Py_None, Py_None);
    pygear_WorkerObject* python_worker = (pygear_WorkerObject*) PyObject_CallObject((PyObject *) &pygear_WorkerType, argList);

    python_worker->g_Worker = gearman_worker_clone(NULL, self->g_Worker);
    return Py_BuildValue("O", python_worker);
}

/**
 * See gearman_error() for details.
 */
static PyObject* pygear_worker_error(pygear_WorkerObject* self){
    return Py_BuildValue("s", gearman_worker_error(self->g_Worker));
}

/**
 * See gearman_errno() for details.
 */
static PyObject* pygear_worker_errno(pygear_WorkerObject* self){
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

/**
 * Set options for a worker structure.
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 * @param options Available options for worker structures.
 */

static PyObject* pygear_worker_set_options(pygear_WorkerObject* self, PyObject* args, PyObject* kwargs){
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

    int worker_options[11];

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
                                      )){
        return NULL;
    }

    int i;
    for (i=0; i < 11; i++){
        if (worker_options[i]){
            gearman_worker_add_options(self->g_Worker, options_t_value[i]);
        } else {
            gearman_worker_remove_options(self->g_Worker, options_t_value[i]);
        }
    }
    Py_RETURN_NONE;
}

/**
 * Get options for a worker structure.
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 * @return Options set for the worker structure.
 */

static PyObject* pygear_worker_get_options(pygear_WorkerObject* self){
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

    PyObject* worker_options[11];
    int i;
    for (i=0; i < 11; i++){
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
    return option_dictionary;
}

/**
 * See gearman_universal_timeout() for details.
 */
static PyObject* pygear_worker_timeout(pygear_WorkerObject* self){
    return Py_BuildValue("i", gearman_worker_timeout(self->g_Worker));
}

/**
 * See gearman_universal_set_timeout() for details.
 */
static PyObject* pygear_worker_set_timeout(pygear_WorkerObject* self, PyObject* args){
    int timeout;
    if (!PyArg_ParseTuple(args, "i", &timeout)){
        return NULL;
    }
    gearman_worker_set_timeout(self->g_Worker, timeout);

    Py_RETURN_NONE;
}

/**
 * See gearman_set_log_fn() for details.
 */

static void _pygear_worker_log_fn_wrapper(const char* line, gearman_verbose_t verbose, void* context){
    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject* python_cb_method = (PyObject*) context;

    PyObject* python_line = PyString_FromString(line);
    PyObject* callback_return = PyObject_CallFunction(python_cb_method, "O", python_line);
    if (!callback_return){
        if (PyErr_Occurred()){
            PyErr_Print();
        }
    }

    PyGILState_Release(gstate);
}

static PyObject* pygear_worker_set_log_fn(pygear_WorkerObject* self, PyObject* args){
    PyObject* logging_cb;
    int log_level;
    if (!PyArg_ParseTuple(args, "Oi", &logging_cb, &log_level)){
        return NULL;
    }
    Py_INCREF(logging_cb);
    gearman_worker_set_log_fn(self->g_Worker, _pygear_worker_log_fn_wrapper, logging_cb, log_level);
    Py_RETURN_NONE;
}

/**
 * Add a job server to a worker. This goes into a list of servers that can be
 * used to run tasks. No socket I/O happens here, it is just added to a list.
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 * @param[in] host Hostname or IP address (IPv4 or IPv6) of the server to add.
 * @param[in] port Port of the server to add.
 * @return Standard gearman return value.
 */
static PyObject* pygear_worker_add_server(pygear_WorkerObject* self, PyObject* args){
    char* host;
    int port;
    if (!PyArg_ParseTuple(args, "zi", &host, &port)){
        return NULL;
    }
    gearman_return_t result = gearman_worker_add_server(self->g_Worker, host, port);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}


/**
 * Add a list of job servers to a worker. The format for the server list is:
 * SERVER[:PORT][,SERVER[:PORT]]...
 * Some examples are:
 * [ "10.0.0.1", "10.0.0.2", "10.0.0.3" ]
 * [ "localhost234", "jobserver2.domain.com:7003", "10.0.0.3" ]
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 * @param[in] servers Server list described above.
 * @return Standard gearman return value.
 */
static PyObject* pygear_worker_add_servers(pygear_WorkerObject* self, PyObject* args){
    PyObject* server_list;
    if (!PyArg_ParseTuple(args, "O", &server_list)){
        return NULL;
    }
    if (!PyList_Check(server_list)){
        PyTypeObject* arg_type = (PyTypeObject*) PyObject_Type(server_list);
        char* err_base = "Worker.add_servers expected list, got ";
        char* err_string = malloc(sizeof(char) * (strlen(err_base) + strlen(arg_type->tp_name) + 1));
        sprintf(err_string, "%s%s", err_base, arg_type->tp_name);
        PyErr_SetString(PyExc_TypeError, err_string);
        return NULL;
    }

    Py_ssize_t num_servers = PyList_Size(server_list);
    Py_ssize_t i;
    for (i=0; i < num_servers; i++){
        char* srv_string = PyString_AsString(PyList_GetItem(server_list, i));
        gearman_return_t result = gearman_worker_add_servers(self->g_Worker, srv_string);

        if (_pygear_check_and_raise_exn(result)){
            return NULL;
        }
    }
    Py_RETURN_NONE;
}

/**
 * Remove all servers currently associated with the worker.
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 */
static PyObject* pygear_worker_remove_servers(pygear_WorkerObject* self){
    gearman_worker_remove_servers(self->g_Worker);
    Py_RETURN_NONE;
}

/**
 * When in non-blocking I/O mode, wait for activity from one of the servers.
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 * @return Standard gearman return value.
 */
static PyObject* pygear_worker_wait(pygear_WorkerObject* self){
    gearman_return_t result = gearman_worker_wait(self->g_Worker);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Register function with job servers with an optional timeout. The timeout
 * specifies how many seconds the server will wait before marking a job as
 * failed. If timeout is zero, there is no timeout.
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 * @param[in] function_name Function name to register.
 * @param[in] timeout Optional timeout (in seconds) that specifies the maximum
 *  time a job should. This is enforced on the job server. A value of 0 means
 *  an infinite time.
 * @return Standard gearman return value.
 */
static PyObject* pygear_worker_register(pygear_WorkerObject* self, PyObject* args){
    char* function_name;
    unsigned timeout;
    if (!PyArg_ParseTuple(args, "sI", &function_name, &timeout)){
        return NULL;
    }
    gearman_return_t result = gearman_worker_register(self->g_Worker, function_name, timeout);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Unregister function with job servers.
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 * @param[in] function_name Function name to unregister.
 * @return Standard gearman return value.
 */
static PyObject* pygear_worker_unregister(pygear_WorkerObject* self, PyObject* args){
    char* function_name;
    if (!PyArg_ParseTuple(args, "s", &function_name)){
        return NULL;
    }
    gearman_return_t result = gearman_worker_unregister(self->g_Worker, function_name);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Unregister all functions with job servers.
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 * @return Standard gearman return value.
 */
static PyObject* pygear_worker_unregister_all(pygear_WorkerObject* self){
    gearman_return_t result = gearman_worker_unregister_all(self->g_Worker);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Get a job from one of the job servers. This does not used the callback
 * interface below, which means results must be sent back to the job server
 * manually. It is also the responsibility of the caller to free the job once
 * it has been completed.
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 * @param[in] job Caller allocated structure, or NULL to allocate one.
 * @param[out] ret_ptr Standard gearman return value.
 * @return On success, a pointer to the (possibly allocated) structure. On
 *  failure this will be NULL.
 */
static PyObject* pygear_worker_grab_job(pygear_WorkerObject* self){
    gearman_return_t result;
    gearman_job_st* new_job = gearman_worker_grab_job(self->g_Worker, NULL, &result);

    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }

    PyObject *argList = Py_BuildValue("(O, O)", Py_None, Py_None);
    pygear_JobObject* python_job = (pygear_JobObject*) PyObject_CallObject((PyObject *) &pygear_JobType, argList);
    python_job->g_Job = new_job;

    return Py_BuildValue("O", python_job);
}

/**
 * Free all jobs for a gearman structure.
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 */
static PyObject* pygear_worker_free_all(pygear_WorkerObject* self){
    gearman_job_free_all(self->g_Worker);
    Py_RETURN_NONE;
}

/**
 * See if a function exists in the server. It will not return
 * true if the function is currently being de-allocated.
 * @param[in] worker gearman_worker_st that will be used.
 * @param[in] function_name Function name for search.
 * @param[in] function_length Length of function name.
 * @return bool
 */
static PyObject* pygear_worker_function_exists(pygear_WorkerObject* self, PyObject* args){
    char* function_name;
    int function_length;
    if (!PyArg_ParseTuple(args, "s#", &function_name, &function_length)){
        return NULL;
    }

    int func_exist = gearman_worker_function_exist(self->g_Worker, function_name, function_length);

    return (func_exist ? Py_True : Py_False);
}


void* _pygear_worker_function_mapper(gearman_job_st* gear_job, void* context,
                                   size_t* result_size, gearman_return_t* ret_ptr){
    PyGILState_STATE gstate = PyGILState_Ensure();

    pygear_WorkerObject* worker = ((pygear_WorkerObject*) context);
    const char* job_func_name = gearman_job_function_name(gear_job);
    PyObject* python_cb_method = PyDict_GetItemString(worker->g_FunctionMap, job_func_name);

    if (python_cb_method == NULL){
        PyErr_SetString(PyExc_SystemError, "Worker does not support method %s\n");
        PyGILState_Release(gstate);
        *ret_ptr = GEARMAN_FAIL;
        return NULL;
    }

    // Bind the job into a python representation, and call through the python
    // callback method
    PyObject *argList = Py_BuildValue("(O, O)", Py_None, Py_None);
    pygear_JobObject* python_job = (pygear_JobObject*) PyObject_CallObject((PyObject *) &pygear_JobType, argList);
    python_job->g_Job = gear_job;

    PyObject* callback_return = PyObject_CallFunction(python_cb_method, "O", python_job);
    if (!callback_return){
        if (!PyErr_Occurred()){
            // If the callback returned NULL but did not set an exception,
            // set a generic one to be sent back.
            PyErr_SetObject(
                PyGearExn_ERROR,
                PyString_FromFormat(
                    "Callback method for %s failed, but threw no exception",
                    job_func_name
                )
            );
        }
        PyObject *ptype, *pvalue, *ptraceback;
        PyErr_Fetch(&ptype, &pvalue, &ptraceback);
        Py_XINCREF(ptype);
        Py_XINCREF(pvalue);
        Py_XINCREF(ptraceback);
        PyErr_Restore(ptype, pvalue, ptraceback);

        // The value and traceback object may be NULL even when the type object is not.
        // NULL values would break Py_BuildValue below, so switch them to None
        if (!pvalue){ pvalue = Py_None; }
        if (!ptraceback){ ptraceback = Py_None; }

        PyObject* traceback = PyImport_ImportModule("traceback");
        PyObject* string_traceback = PyObject_CallMethod(traceback, "format_tb", "O", ptraceback);

        PyObject* error_tuple = Py_BuildValue("(O, O, O)", ptype, pvalue, string_traceback);
        PyObject* pickled_data = PyObject_CallMethod(worker->pickle, "dumps", "(O)", error_tuple);
        if (!pickled_data){
            PyErr_SetString(PyExc_SystemError, "Failed to pickle exception data\n");
            PyGILState_Release(gstate);
            *ret_ptr = GEARMAN_FAIL;
            return NULL;
        }

        char* c_data; Py_ssize_t c_data_size;
        if (PyString_AsStringAndSize(pickled_data, &c_data, &c_data_size) == -1){
            PyErr_SetString(PyExc_SystemError, "Failed to stringify pickled exception data\n");
            PyGILState_Release(gstate);
            *ret_ptr = GEARMAN_FAIL;
            return NULL;
        }

        if (_pygear_check_and_raise_exn(gearman_job_send_exception(gear_job, c_data, c_data_size))){
            PyErr_Print();
        }

    } else {
        // Try to pickle the return from the function
        PyObject* pickled_result = PyObject_CallMethod(worker->pickle, "dumps", "O", callback_return);
        if (!pickled_result){
            PyErr_SetString(PyExc_SystemError, "Failed to pickle worker result data\n");
            PyGILState_Release(gstate);
            *ret_ptr = GEARMAN_FAIL;
            return NULL;
        } else {
            Py_ssize_t len;
            char* buffer;
            PyString_AsStringAndSize(pickled_result, &buffer, &len);
            if (_pygear_check_and_raise_exn(gearman_job_send_complete(gear_job, buffer, len))){
                PyErr_Print();
            } else {
                *ret_ptr = GEARMAN_SUCCESS;
            }
        }
    }

    PyGILState_Release(gstate);
    return NULL;
}

/**
 * Register and add callback function for worker. To remove functions that have
 * been added, call gearman_worker_unregister() or
 * gearman_worker_unregister_all().
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 * @param[in] function_name Function name to register.
 * @param[in] timeout Optional timeout (in seconds) that specifies the maximum
 *  time a job should. This is enforced on the job server. A value of 0 means
 *  an infinite time.
 * @param[in] function Function to run when there is a job ready.
 * @param[in] context Argument to pass into the callback function.
 * @return Standard gearman return value.
 */
static PyObject* pygear_worker_add_function(pygear_WorkerObject* self, PyObject* args){
    char* function_name;
    int timeout;
    PyObject* callback_method;

    if (!PyArg_ParseTuple(args, "siO", &function_name, &timeout, &callback_method)){
        return NULL;
    }

    Py_INCREF(callback_method);
    PyDict_SetItemString(self->g_FunctionMap, function_name, callback_method);

    gearman_return_t result =
        gearman_worker_add_function(
            self->g_Worker,
            function_name,
            timeout,
            _pygear_worker_function_mapper,
            self
        );

    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Wait for a job and call the appropriate callback function when it gets one.
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 * @return Standard gearman return value.
 */
static PyObject* pygear_worker_work(pygear_WorkerObject* self){
    gearman_return_t result  = gearman_worker_work(self->g_Worker);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Send data to all job servers to see if they echo it back. This is a test
 * function to see if job servers are responding properly.
 *
 * @param[in] worker Structure previously initialized with
 *  gearman_worker_create() or gearman_worker_clone().
 * @param[in] workload The workload to ask the server to echo back.
 * @param[in] workload_size Size of the workload.
 * @return Standard gearman return value.
 */
static PyObject* pygear_worker_echo(pygear_WorkerObject* self, PyObject* args){
    char* workload;
    int workload_size;
    if (!PyArg_ParseTuple(args, "s#", &workload, &workload_size)){
        return NULL;
    }
    gearman_return_t result =
        gearman_worker_echo(
            self->g_Worker,
            workload,
            workload_size
        );
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_worker_id(pygear_WorkerObject* self){
    return Py_BuildValue("i", gearman_worker_id(self->g_Worker));
}

static PyObject* pygear_worker_set_identifier(pygear_WorkerObject* self, PyObject* args){
    char* id;
    int id_size;
    if (!PyArg_ParseTuple(args, "s#", &id, &id_size)){
        return NULL;
    }
    gearman_return_t result = gearman_worker_set_identifier(self->g_Worker, id, id_size);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_worker_set_namespace(pygear_WorkerObject* self, PyObject* args){
    char* namespace_key;
    int namespace_key_size;
    if (!PyArg_ParseTuple(args, "s#", &namespace_key, &namespace_key_size)){
        return NULL;
    }
    gearman_worker_set_namespace(self->g_Worker, namespace_key, namespace_key_size);

    Py_RETURN_NONE;
}

static PyObject* pygear_worker_namespace(pygear_WorkerObject* self){
    const char* namespace = gearman_worker_namespace(self->g_Worker);
    return PyString_FromString(namespace);
}
