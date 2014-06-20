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

    return (PyObject *)self;
}

int Job_init(pygear_JobObject *self, PyObject *args, PyObject *kwds){
    self->g_Job = NULL;
    return 0;
}

void Job_dealloc(pygear_JobObject* self){
    if (self->g_Job){
        gearman_job_free(self->g_Job);
        self->g_Job = NULL;
    }

    self->ob_type->tp_free((PyObject*)self);
}
