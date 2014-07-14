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

#ifndef PYGEAR_H
#define PYGEAR_H

#include <Python.h>
#include <libgearman-1.0/gearman.h>
#include "client.c"
#include "task.c"
#include "job.c"
#include "worker.c"
#include "exception.h"
#include "admin.c"

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
