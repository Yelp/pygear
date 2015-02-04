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

#include "job.h"

/*
 * Class constructor / destructor methods
 */

int Job_init(pygear_JobObject* self, PyObject* args, PyObject* kwds) {
    self->g_Job = NULL;
    self->serializer = PyImport_ImportModule(PYTHON_SERIALIZER);
    if (self->serializer == NULL) {
        PyObject* err_string = PyString_FromFormat("Failed to import '%s'", PYTHON_SERIALIZER);
        PyErr_SetObject(PyExc_ImportError, err_string);
        Py_XDECREF(err_string);
        return -1;
    }
    return 0;
}

void Job_dealloc(pygear_JobObject* self) {
    if (self->g_Job) {
        gearman_job_free(self->g_Job);
        self->g_Job = NULL;
    }
    Py_XDECREF(self->serializer);
    self->ob_type->tp_free((PyObject*)self);
}

/*
 * Instance Methods
 */

static PyObject* pygear_job_set_serializer(pygear_JobObject* self, PyObject* args) {
    PyObject* serializer;
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
    Py_XDECREF(self->serializer);
    self->serializer = serializer;
    Py_RETURN_NONE;
}


static PyObject* pygear_job_send_data(pygear_JobObject* self, PyObject* args) {
    PyObject* data;
    if (!PyArg_ParseTuple(args, "O", &data)) {
        return NULL;
    }
    PyObject* pickled_data = PyObject_CallMethod(self->serializer, "dumps", "O", data);
    if (!pickled_data) {
        PyErr_SetString(PyExc_SystemError, "Could not pickle job_data data for transport\n");
        return NULL;
    }
    char* c_data; Py_ssize_t c_data_size;
    if (PyString_AsStringAndSize(pickled_data, &c_data, &c_data_size) == -1) {
        Py_XDECREF(pickled_data);
        PyErr_SetString(PyExc_SystemError, "Failed to convert pickled data to C string");
        return NULL;
    }
    Py_XDECREF(pickled_data);
    gearman_return_t result = gearman_job_send_data(self->g_Job, c_data, c_data_size);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_job_send_warning(pygear_JobObject* self, PyObject* args) {
    PyObject* data;
    if (!PyArg_ParseTuple(args, "O", &data)) {
        return NULL;
    }
    PyObject* pickled_data = PyObject_CallMethod(self->serializer, "dumps", "O", data);
    if (!pickled_data) {
        PyErr_SetString(PyExc_SystemError, "Could not pickle job_warning data for transport\n");
        return NULL;
    }
    char* c_data; Py_ssize_t c_data_size;
    if (PyString_AsStringAndSize(pickled_data, &c_data, &c_data_size) == -1) {
        Py_XDECREF(pickled_data);
        PyErr_SetString(PyExc_SystemError, "Failed to convert pickled warning data to C string");
        return NULL;
    }
    Py_XDECREF(pickled_data);
    gearman_return_t result = gearman_job_send_warning(self->g_Job, c_data, c_data_size);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_job_send_status(pygear_JobObject* self, PyObject* args) {
    unsigned numerator, denominator;
    if (!PyArg_ParseTuple(args, "II", &numerator, &denominator)) {
        return NULL;
    }
    gearman_return_t result = gearman_job_send_status(self->g_Job, numerator, denominator);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_job_send_complete(pygear_JobObject* self, PyObject* args) {
    PyObject* result;
    if (!PyArg_ParseTuple(args, "O", &result)) {
        return NULL;
    }
    PyObject* pickled_result = PyObject_CallMethod(self->serializer, "dumps", "O", result);
    if (!pickled_result) {
        PyErr_SetString(PyExc_SystemError, "Could not pickle job_complete data for transport\n");
        return NULL;
    }
    char* c_result; Py_ssize_t c_result_size;
    if (PyString_AsStringAndSize(pickled_result, &c_result, &c_result_size) == -1) {
        Py_XDECREF(pickled_result);
        PyErr_SetString(PyExc_SystemError, "Failed to convert pickled complete data to C string");
        return NULL;
    }
    Py_XDECREF(pickled_result); 
    gearman_return_t gearman_result = gearman_job_send_complete(self->g_Job, c_result, c_result_size);
    if (_pygear_check_and_raise_exn(gearman_result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_job_send_exception(pygear_JobObject* self, PyObject* args) {
    PyObject* data;
    if (!PyArg_ParseTuple(args, "O", &data)) {
        return NULL;
    }
    PyObject* pickled_data = PyObject_CallMethod(self->serializer, "dumps", "O", data);
    if (!pickled_data) {
        PyErr_SetString(PyExc_SystemError, "Could not pickle job_exception data for transport\n");
        return NULL;
    }

    char* c_data; Py_ssize_t c_data_size;
    if (PyString_AsStringAndSize(pickled_data, &c_data, &c_data_size) == -1) {
        Py_XDECREF(pickled_data);
        PyErr_SetString(PyExc_SystemError, "Failed to convert pickled exception data to C string");
        return NULL;
    }
    Py_XDECREF(pickled_data);
    gearman_return_t result = gearman_job_send_exception(self->g_Job, c_data, c_data_size);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_job_send_fail(pygear_JobObject* self) {
    gearman_return_t result = gearman_job_send_fail(self->g_Job);
    if (_pygear_check_and_raise_exn(result)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_job_handle(pygear_JobObject* self) {
    return Py_BuildValue("s", gearman_job_handle(self->g_Job));
}

static PyObject* pygear_job_function_name(pygear_JobObject* self) {
    return Py_BuildValue("s", gearman_job_function_name(self->g_Job));
}

static PyObject* pygear_job_unique(pygear_JobObject* self) {
    return Py_BuildValue("s", gearman_job_unique(self->g_Job));
}

static PyObject* pygear_job_workload(pygear_JobObject* self) {
    const char* job_workload = gearman_job_workload(self->g_Job);
    size_t job_size = gearman_job_workload_size(self->g_Job);
    PyObject* py_result = Py_BuildValue("s#", job_workload, job_size);
    PyObject* loadstr = PyString_FromString("loads");
    PyObject* py_workload = PyObject_CallMethodObjArgs(
        self->serializer,
        loadstr,
        py_result,
        NULL
    );
    Py_XDECREF(py_result);
    Py_XDECREF(loadstr);
    return py_workload;
}

static PyObject* pygear_job_workload_size(pygear_JobObject* self) {
    return Py_BuildValue("I", gearman_job_workload_size(self->g_Job));
}

static PyObject* pygear_job_error(pygear_JobObject* self) {
    return Py_BuildValue("s", gearman_job_error(self->g_Job));
}
