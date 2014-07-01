#include "job.h"

/*
 * Class constructor / destructor methods
 */

PyObject* Job_new(PyTypeObject *type, PyObject *args, PyObject *kwds){
    pygear_JobObject* self;

    self = (pygear_JobObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->g_Job = NULL;
    }
    self->pickle = NULL;
    return (PyObject *)self;
}

int Job_init(pygear_JobObject *self, PyObject *args, PyObject *kwds){
    self->g_Job = NULL;
    self->pickle = PyImport_ImportModule("pickle");
    return 0;
}

void Job_dealloc(pygear_JobObject* self){
    if (self->g_Job){
        gearman_job_free(self->g_Job);
        self->g_Job = NULL;
    }
    Py_XDECREF(self->pickle);
    self->ob_type->tp_free((PyObject*)self);
}

/*
 * Instance Methods
 */


/**
 * Send data for a running job.
 */
static PyObject* pygear_job_send_data(pygear_JobObject* self, PyObject* args){
    PyObject* data;
    if (!PyArg_ParseTuple(args, "O", &data)){
        return NULL;
    }
    PyObject* pickled_data = PyObject_CallMethod(self->pickle, "dumps", "O", data);
    if (!pickled_data){
        PyErr_SetString(PyExc_SystemError, "Could not pickle job_data data for transport\n");
        return NULL;
    }
    char* c_data; Py_ssize_t c_data_size;
    if (PyString_AsStringAndSize(pickled_data, &c_data, &c_data_size) == -1){
        PyErr_SetString(PyExc_SystemError, "Failed to convert pickled data to C string");
        return NULL;
    }
    gearman_return_t result = gearman_job_send_data(self->g_Job, c_data, c_data_size);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Send warning for a running job.
 */
static PyObject* pygear_job_send_warning(pygear_JobObject* self, PyObject* args){
    PyObject* data;
    if (!PyArg_ParseTuple(args, "O", &data)){
        return NULL;
    }
    PyObject* pickled_data = PyObject_CallMethod(self->pickle, "dumps", "O", data);
    if (!pickled_data){
        PyErr_SetString(PyExc_SystemError, "Could not pickle job_warning data for transport\n");
        return NULL;
    }
    char* c_data; Py_ssize_t c_data_size;
    if (PyString_AsStringAndSize(pickled_data, &c_data, &c_data_size) == -1){
        PyErr_SetString(PyExc_SystemError, "Failed to convert pickled warning data to C string");
        return NULL;
    }
    gearman_return_t result = gearman_job_send_warning(self->g_Job, c_data, c_data_size);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Send status information for a running job.
 */
static PyObject* pygear_job_send_status(pygear_JobObject* self, PyObject* args){
    unsigned numerator, denominator;
    if (!PyArg_ParseTuple(args, "II", &numerator, &denominator)){
        return NULL;
    }
    gearman_return_t result = gearman_job_send_status(self->g_Job, numerator, denominator);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Send result and complete status for a job.
 */
static PyObject* pygear_job_send_complete(pygear_JobObject* self, PyObject* args){
    PyObject* result;
    if (!PyArg_ParseTuple(args, "O", &result)){
        return NULL;
    }
    PyObject* pickled_result = PyObject_CallMethod(self->pickle, "dumps", "O", result);
    if (!pickled_result){
        PyErr_SetString(PyExc_SystemError, "Could not pickle job_complete data for transport\n");
        return NULL;
    }
    char* c_result; Py_ssize_t c_result_size;
    if (PyString_AsStringAndSize(pickled_result, &c_result, &c_result_size) == -1){
        PyErr_SetString(PyExc_SystemError, "Failed to convert pickled complete data to C string");
        return NULL;
    }
    gearman_return_t gearman_result = gearman_job_send_complete(self->g_Job, c_result, c_result_size);
    if (_pygear_check_and_raise_exn(gearman_result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Send exception for a running job.
 */
static PyObject* pygear_job_send_exception(pygear_JobObject* self, PyObject* args){
    PyObject* data;
    if (!PyArg_ParseTuple(args, "O", &data)){
        return NULL;
    }
    PyObject* pickled_data = PyObject_CallMethod(self->pickle, "dumps", "O", data);
    if (!pickled_data){
        PyErr_SetString(PyExc_SystemError, "Could not pickle job_exception data for transport\n");
        return NULL;
    }

    char* c_data; Py_ssize_t c_data_size;
    if (PyString_AsStringAndSize(pickled_data, &c_data, &c_data_size) == -1){
        PyErr_SetString(PyExc_SystemError, "Failed to convert pickled exception data to C string");
        return NULL;
    }
    gearman_return_t result = gearman_job_send_exception(self->g_Job, c_data, c_data_size);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Send fail status for a job.
 */
static PyObject* pygear_job_send_fail(pygear_JobObject* self){
    gearman_return_t result = gearman_job_send_fail(self->g_Job);
    if (_pygear_check_and_raise_exn(result)){
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Get job handle.
 */
static PyObject* pygear_job_handle(pygear_JobObject* self){
    return Py_BuildValue("s", gearman_job_handle(self->g_Job));
}

/**
 * Get the function name associated with a job.
 */
static PyObject* pygear_job_function_name(pygear_JobObject* self){
    return Py_BuildValue("s", gearman_job_function_name(self->g_Job));
}

/**
 * Get the unique ID associated with a job.
 */
static PyObject* pygear_job_unique(pygear_JobObject* self){
    return Py_BuildValue("s", gearman_job_unique(self->g_Job));
}

/**
 * Get a the workload for a job.
 */
static PyObject* pygear_job_workload(pygear_JobObject* self){
    const char* job_workload = gearman_job_workload(self->g_Job);
    size_t job_size = gearman_job_workload_size(self->g_Job);
    PyObject* py_result = Py_BuildValue("s#", job_workload, job_size);
    PyObject* py_workload = PyObject_CallMethod(self->pickle, "loads", "S", py_result);
    return py_workload;
}

/**
 * Get size of the workload for a job.
 */
static PyObject* pygear_job_workload_size(pygear_JobObject* self){
    return Py_BuildValue("I", gearman_job_workload_size(self->g_Job));
}

/**
 * Get error string for a job.
 */
static PyObject* pygear_job_error(pygear_JobObject* self){
    return Py_BuildValue("s", gearman_job_error(self->g_Job));
}
