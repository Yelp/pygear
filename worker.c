#include "worker.h"

/*
 * Class constructor / destructor methods
 */

PyObject* Worker_new(PyTypeObject *type, PyObject *args, PyObject *kwds){
    pygear_WorkerObject* self;

    self = (pygear_WorkerObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->g_Worker = NULL;
    }

    return (PyObject *)self;
}

int Worker_init(pygear_WorkerObject *self, PyObject *args, PyObject *kwds){
    self->g_Worker = NULL;
    return 0;
}

void Worker_dealloc(pygear_WorkerObject* self){
    if (self->g_Worker){
        gearman_worker_free(self->g_Worker);
        self->g_Worker = NULL;
    }

    self->ob_type->tp_free((PyObject*)self);
}
