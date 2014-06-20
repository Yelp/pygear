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
    self->g_Task = NULL;
    return 0;
}

void Task_dealloc(pygear_TaskObject* self){
    if (self->g_Task){
        // Decrement the reference count for callback functions, if they are set
        pygear_task_st* ext_task = (pygear_task_st*) self->g_Task;
        Py_XDECREF(ext_task->extended_task.cb_workload);
        Py_XDECREF(ext_task->extended_task.cb_created);
        Py_XDECREF(ext_task->extended_task.cb_data);
        Py_XDECREF(ext_task->extended_task.cb_warning);
        Py_XDECREF(ext_task->extended_task.cb_status);
        Py_XDECREF(ext_task->extended_task.cb_complete);
        Py_XDECREF(ext_task->extended_task.cb_exception);
        Py_XDECREF(ext_task->extended_task.cb_fail);

        gearman_task_free(self->g_Task);
        self->g_Task = NULL;
    }

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
    return Py_BuildValue("s#",
        (char *)gearman_task_data(self->g_Task),
        gearman_task_data_size(self->g_Task));
}
