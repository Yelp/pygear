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

#include "task.h"

/*
 * Class constructor / destructor methods
 */

int Task_init(pygear_TaskObject* self, PyObject* args, PyObject* kwds) {
    self->serializer = PyImport_ImportModule(PYTHON_SERIALIZER);
    if (self->serializer == NULL) {
        PyObject* err_string = PyString_FromFormat("Failed to import '%s'", PYTHON_SERIALIZER);
        PyErr_SetObject(PyExc_ImportError, err_string);
        Py_XDECREF(err_string);
        return -1;
    }
    self->g_Task = NULL;
    return 0;
}

int Task_traverse(pygear_TaskObject* self, visitproc visit, void* arg) {
    Py_VISIT(self->serializer);
    return 0;
}

int Task_clear(pygear_TaskObject* self) {
    Py_CLEAR(self->serializer);
    return 0;
}

void Task_dealloc(pygear_TaskObject* self) {
    if (self->g_Task) {
        gearman_task_free(self->g_Task);
        self->g_Task = NULL;
    }
    Task_clear(self);
    self->ob_type->tp_free((PyObject*)self);
}

/*
 * Callback handling
 */

static PyObject* pygear_task_function_name(pygear_TaskObject* self) {
    return Py_BuildValue("s", gearman_task_function_name(self->g_Task));
}

static PyObject* pygear_task_unique(pygear_TaskObject* self) {
    return Py_BuildValue("s", gearman_task_unique(self->g_Task));
}

static PyObject* pygear_task_job_handle(pygear_TaskObject* self) {
    return Py_BuildValue("s", gearman_task_job_handle(self->g_Task));
}

static PyObject* pygear_task_is_known(pygear_TaskObject* self) {
    return Py_BuildValue("O", (gearman_task_is_known(self->g_Task) ? Py_True : Py_False));
}

static PyObject* pygear_task_is_running(pygear_TaskObject* self) {
    return Py_BuildValue("O", (gearman_task_is_running(self->g_Task) ? Py_True : Py_False));
}

static PyObject* pygear_task_numerator(pygear_TaskObject* self) {
    return Py_BuildValue("i", gearman_task_numerator(self->g_Task));
}

static PyObject* pygear_task_denominator(pygear_TaskObject* self) {
    return Py_BuildValue("i", gearman_task_denominator(self->g_Task));
}

static PyObject* pygear_task_error(pygear_TaskObject* self) {
    return Py_BuildValue("s", gearman_task_error(self->g_Task));
}

static PyObject* pygear_task_returncode(pygear_TaskObject* self) {
    return Py_BuildValue("i", gearman_task_return(self->g_Task));
}

static PyObject* pygear_task_strstate(pygear_TaskObject* self) {
    return Py_BuildValue("s", gearman_task_strstate(self->g_Task));
}

static PyObject* pygear_task_data_size(pygear_TaskObject* self) {
    return Py_BuildValue("I", gearman_task_data_size(self->g_Task));
}

static PyObject* pygear_task_set_serializer(pygear_TaskObject* self, PyObject* args) {
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

static PyObject* pygear_task_result(pygear_TaskObject* self) {
    const char* task_result = gearman_task_data(self->g_Task);
    size_t result_size = gearman_task_data_size(self->g_Task);
    if (!task_result) {
        Py_RETURN_NONE;
    }
    PyObject* py_result = Py_BuildValue("s#", task_result, result_size);
    if (!py_result) {
        PyErr_SetString(PyExc_SystemError, "Failed to build value from Task result\n");
        return NULL;
    }
    PyObject* unpickled_result = PyObject_CallMethod(self->serializer, "loads", "O", py_result);
    Py_XDECREF(py_result);
    if (!unpickled_result) {
        PyErr_SetString(PyExc_SystemError," Failed to unpickle internal Task data\n");
        return NULL;
    }
    return unpickled_result;
}
