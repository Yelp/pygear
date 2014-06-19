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
    return 0;
}

void Client_dealloc(pygear_ClientObject* self){
    if (self->g_Client){
        gearman_client_free(self->g_Client);
        self->g_Client = NULL;
    }
    self->ob_type->tp_free((PyObject*)self);
}

/*
 * Instance methods
 */

static PyObject* pygear_client_add_server(pygear_ClientObject* self, PyObject* args){
    char* host;
    int port;
    if (!PyArg_ParseTuple(args, "zi", &host, &port)){
        return NULL;
    }
    gearman_return_t result = gearman_client_add_server(self->g_Client, host, port);
    return Py_BuildValue("i", result);
}
