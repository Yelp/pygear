#include "task.h"

/*
 * Class constructor / destructor methods
 */

PyObject* Task_new(PyTypeObject *type, PyObject *args, PyObject *kwds){
    pygear_TaskObject* self;

    self = (pygear_TaskObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->g_Task = NULL;
    }

    return (PyObject *)self;
}

int Task_init(pygear_TaskObject *self, PyObject *args, PyObject *kwds){
    self->pickle = PyImport_ImportModule("pickle");
    self->g_Task = NULL;
    if (!self->pickle){
        PyErr_SetString(PyExc_ImportError, "Failed to import 'pickle'");
        return -1;
    }
    return 0;
}

void Task_dealloc(pygear_TaskObject* self){
    if (self->g_Task){
        gearman_task_free(self->g_Task);
        self->g_Task = NULL;
    }
    Py_XDECREF(self->pickle);
    self->ob_type->tp_free((PyObject*)self);
}

/*
 * Callback handling
 */

static PyObject* pygear_task_function_name(pygear_TaskObject* self){
    return Py_BuildValue("s", gearman_task_function_name(self->g_Task));
}

static PyObject* pygear_task_unique(pygear_TaskObject* self){
    return Py_BuildValue("s", gearman_task_unique(self->g_Task));
}

static PyObject* pygear_task_job_handle(pygear_TaskObject* self){
    return Py_BuildValue("s", gearman_task_job_handle(self->g_Task));
}

static PyObject* pygear_task_is_known(pygear_TaskObject* self){
    return Py_BuildValue("O",
        (gearman_task_is_known(self->g_Task) ? Py_True : Py_False));
}

static PyObject* pygear_task_is_running(pygear_TaskObject* self){
    return Py_BuildValue("O",
        (gearman_task_is_running(self->g_Task) ? Py_True : Py_False));
}

static PyObject* pygear_task_numerator(pygear_TaskObject* self){
    return Py_BuildValue("i", gearman_task_numerator(self->g_Task));
}

static PyObject* pygear_task_denominator(pygear_TaskObject* self){
    return Py_BuildValue("i", gearman_task_denominator(self->g_Task));
}

static PyObject* pygear_task_returncode(pygear_TaskObject* self){
    return Py_BuildValue("i", gearman_task_return(self->g_Task));
}

static PyObject* pygear_task_strstate(pygear_TaskObject* self){
    return Py_BuildValue("s", gearman_task_strstate(self->g_Task));
}

static PyObject* pygear_task_data_size(pygear_TaskObject* self){
    return Py_BuildValue("I", gearman_task_data_size(self->g_Task));
}

static PyObject* pygear_task_result(pygear_TaskObject* self){
    const char* task_result = gearman_task_data(self->g_Task);
    size_t result_size = gearman_task_data_size(self->g_Task);

    if (!task_result){
        Py_RETURN_NONE;
    }

    PyObject* py_result = Py_BuildValue("s#", task_result, result_size);
    if (!py_result){
        PyErr_SetString(PyExc_SystemError, "Failed to build value from Task result\n");
        return NULL;
    }
    PyObject* unpickled_result = PyObject_CallMethod(self->pickle, "loads", "O", py_result);
    if (!unpickled_result){
        PyErr_SetString(PyExc_SystemError," Failed to unpickle internal Task data\n");
        return NULL;
    }
    return unpickled_result;
}
