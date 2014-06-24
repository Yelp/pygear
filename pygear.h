#ifndef PYGEAR_H
#define PYGEAR_H

#include <Python.h>
#include <libgearman-1.0/gearman.h>
#include "client.c"
#include "task.c"
#include "job.c"
#include "worker.c"
#include "exception.h"

PyDoc_STRVAR(pygear_class_docstring,
"PyGear is a python wrapper for the libgearman C library");

/* Method definitions */
static PyObject* pygear_describe_returncode(void* self, PyObject* args);
PyDoc_STRVAR(pygear_describe_returncode_doc,
"Convert a gearman return code into a human-readable string representation of"
"the result.\n"
"@param[in] code Error code number to describe");


/* Module method specification */
static PyMethodDef pygear_class_methods[] = {
    {"describe_returncode", (PyCFunction) pygear_describe_returncode, METH_VARARGS, pygear_describe_returncode_doc},
    {NULL, NULL, 0, NULL}
};

#endif
