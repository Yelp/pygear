#include <Python.h>
#include <libgearman-1.0/gearman.h>
#include <stdio.h>
#include "structmember.h"

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

#ifndef CLIENT_H
#define CLIENT_H

#define _PYMETHOD(name,flags) {#name,(PyCFunction) pygear_client_##name,flags,pygear_client_##name##_doc},

typedef struct {
    PyObject_HEAD
    struct gearman_client_st* g_Client;
} pygear_ClientObject;

static char client_module_docstring[] =
    "Wrapper for libgearman Client";

/* Class init methods */
PyObject* Client_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int Client_init(pygear_ClientObject *self, PyObject *args, PyObject *kwds);
void Client_dealloc(pygear_ClientObject* self);

/* Method definitions */
static PyObject* pygear_client_add_server(pygear_ClientObject *self, PyObject *args);
PyDoc_STRVAR(pygear_client_add_server_doc,
"Add a job server to a client. This goes into a list of servers that can be\n"
"used to run tasks. No socket I/O happens here, it is just added to a list.\n"
"@param[in] client Structure previously initialized with "
"gearman_client_create() or gearman_client_clone().\n"
"@param[in] host Hostname or IP address (IPv4 or IPv6) of the server to add.\n"
"@param[in] port Port of the server to add.\n"
"@return Standard gearman return value.");

/* Module method specification */
static PyMethodDef client_module_methods[] = {
    _PYMETHOD(add_server, METH_VARARGS)
    {NULL, NULL, 0, NULL}
};

PyTypeObject pygear_ClientType = {
    PyObject_HEAD_INIT(NULL)
    0,                                          /*ob_size*/
    "pygear.Client",                            /*tp_name*/
    sizeof(pygear_ClientObject),                /*tp_basicsize*/
    0,                                          /*tp_itemsize*/
    (destructor)Client_dealloc,                 /*tp_dealloc*/
    0,                                          /*tp_print*/
    0,                                          /*tp_getattr*/
    0,                                          /*tp_setattr*/
    0,                                          /*tp_compare*/
    0,                                          /*tp_repr*/
    0,                                          /*tp_as_number*/
    0,                                          /*tp_as_sequence*/
    0,                                          /*tp_as_mapping*/
    0,                                          /*tp_hash */
    0,                                          /*tp_call*/
    0,                                          /*tp_str*/
    0,                                          /*tp_getattro*/
    0,                                          /*tp_setattro*/
    0,                                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /*tp_flags*/
    client_module_docstring,                    /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    client_module_methods,                      /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)Client_init,                      /* tp_init */
    0,                                          /* tp_alloc */
    Client_new,                                 /* tp_new */
};

#endif
