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

#include "pygear.h"

#define INIT_EXN(EXN) \
PyGearExn_##EXN = PyErr_NewException("pygear." # EXN, NULL, NULL); \
Py_INCREF(PyGearExn_##EXN); \
PyModule_AddObject(m, #EXN, PyGearExn_##EXN);

PyMODINIT_FUNC initpygear(void) {
    PyObject* m;

    pygear_ClientType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&pygear_ClientType) < 0) {
        return;
    }

    pygear_TaskType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&pygear_TaskType) < 0) {
        return;
    }

    pygear_JobType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&pygear_JobType) < 0) {
        return;
    }

    pygear_WorkerType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&pygear_WorkerType) < 0) {
        return;
    }

    if (PyType_Ready(&pygear_AdminType) < 0) {
        return;
    }

    // Initialize pygear module
    m = Py_InitModule3("pygear", pygear_class_methods, pygear_class_docstring);

    // Add Client class
    Py_INCREF(&pygear_ClientType);
    PyModule_AddObject(m, "Client", (PyObject *)&pygear_ClientType);

    // Add Task class
    Py_INCREF(&pygear_TaskType);
    PyModule_AddObject(m, "Task", (PyObject *)&pygear_TaskType);

    // Add Job class
    Py_INCREF(&pygear_JobType);
    PyModule_AddObject(m, "Job", (PyObject *)&pygear_JobType);

    // Add Worker class
    Py_INCREF(&pygear_WorkerType);
    PyModule_AddObject(m, "Worker", (PyObject *)&pygear_WorkerType);

    // Add Admin class
    Py_INCREF(&pygear_AdminType);
    PyModule_AddObject(m, "Admin", (PyObject *)&pygear_AdminType);

    // Enum replacements
    PyModule_AddIntConstant(m, "PYGEAR_VERBOSE_NEVER", GEARMAN_VERBOSE_NEVER);
    PyModule_AddIntConstant(m, "PYGEAR_VERBOSE_FATAL", GEARMAN_VERBOSE_FATAL);
    PyModule_AddIntConstant(m, "PYGEAR_VERBOSE_ERROR", GEARMAN_VERBOSE_ERROR);
    PyModule_AddIntConstant(m, "PYGEAR_VERBOSE_INFO",  GEARMAN_VERBOSE_INFO);
    PyModule_AddIntConstant(m, "PYGEAR_VERBOSE_DEBUG", GEARMAN_VERBOSE_DEBUG);
    PyModule_AddIntConstant(m, "PYGEAR_VERBOSE_CRAZY", GEARMAN_VERBOSE_CRAZY);
    PyModule_AddIntConstant(m, "PYGEAR_VERBOSE_MAX",   GEARMAN_VERBOSE_MAX);

    // Exception init
    INIT_EXN(ERROR);
    INIT_EXN(SHUTDOWN);
    INIT_EXN(SHUTDOWN_GRACEFUL);
    INIT_EXN(ERRNO);
    INIT_EXN(EVENT); // DEPRECATED); SERVER ONLY
    INIT_EXN(TOO_MANY_ARGS);
    INIT_EXN(NO_ACTIVE_FDS); // No servers available
    INIT_EXN(INVALID_MAGIC);
    INIT_EXN(INVALID_COMMAND);
    INIT_EXN(INVALID_PACKET);
    INIT_EXN(UNEXPECTED_PACKET);
    INIT_EXN(GETADDRINFO);
    INIT_EXN(NO_SERVERS);
    INIT_EXN(LOST_CONNECTION);
    INIT_EXN(MEMORY_ALLOCATION_FAILURE);
    INIT_EXN(JOB_EXISTS); // see gearman_client_job_status()
    INIT_EXN(JOB_QUEUE_FULL);
    INIT_EXN(SERVER_ERROR);
    INIT_EXN(WORK_ERROR);
    INIT_EXN(WORK_DATA);
    INIT_EXN(WORK_WARNING);
    INIT_EXN(WORK_STATUS);
    INIT_EXN(WORK_EXCEPTION);
    INIT_EXN(WORK_FAIL);
    INIT_EXN(NOT_CONNECTED);
    INIT_EXN(COULD_NOT_CONNECT);
    INIT_EXN(SEND_IN_PROGRESS); // DEPRECATED); SERVER ONLY
    INIT_EXN(RECV_IN_PROGRESS); // DEPRECATED); SERVER ONLY
    INIT_EXN(NOT_FLUSHING);
    INIT_EXN(DATA_TOO_LARGE);
    INIT_EXN(INVALID_FUNCTION_NAME);
    INIT_EXN(INVALID_WORKER_FUNCTION);
    INIT_EXN(NO_REGISTERED_FUNCTION);
    INIT_EXN(NO_REGISTERED_FUNCTIONS);
    INIT_EXN(NO_JOBS);
    INIT_EXN(ECHO_DATA_CORRUPTION);
    INIT_EXN(NEED_WORKLOAD_FN);
    INIT_EXN(UNKNOWN_STATE);
    INIT_EXN(PTHREAD); // DEPRECATED); SERVER ONLY
    INIT_EXN(PIPE_EOF); // DEPRECATED); SERVER ONLY
    INIT_EXN(QUEUE_ERROR); // DEPRECATED); SERVER ONLY
    INIT_EXN(FLUSH_DATA); // Internal state); should never be seen by either client or worker.
    INIT_EXN(SEND_BUFFER_TOO_SMALL);
    INIT_EXN(IGNORE_PACKET); // Internal only
    INIT_EXN(UNKNOWN_OPTION); // DEPRECATED
    INIT_EXN(TIMEOUT);
    INIT_EXN(ARGUMENT_TOO_LARGE);
    INIT_EXN(INVALID_ARGUMENT);
    INIT_EXN(INVALID_SERVER_OPTION); // Bad server option sent to server
    INIT_EXN(MAX_RETURN); /* Always add new error code before */
}

#define EXN_CASE_RAISE(EXN, ERR) \
case GEARMAN_##EXN: { \
    PyErr_SetString(PyGearExn_##EXN, ERR); \
    return 1; \
}

int _pygear_check_and_raise_exn(gearman_return_t returncode, const char* error) {
    switch (returncode) {
        EXN_CASE_RAISE(SHUTDOWN, error);
        EXN_CASE_RAISE(SHUTDOWN_GRACEFUL, error);
        EXN_CASE_RAISE(ERRNO, error);
        EXN_CASE_RAISE(EVENT, error); // DEPRECATED; SERVER ONLY
        EXN_CASE_RAISE(TOO_MANY_ARGS, error);
        EXN_CASE_RAISE(NO_ACTIVE_FDS, error); // No servers available
        EXN_CASE_RAISE(INVALID_MAGIC, error);
        EXN_CASE_RAISE(INVALID_COMMAND, error);
        EXN_CASE_RAISE(INVALID_PACKET, error);
        EXN_CASE_RAISE(UNEXPECTED_PACKET, error);
        EXN_CASE_RAISE(GETADDRINFO, error);
        EXN_CASE_RAISE(NO_SERVERS, error);
        EXN_CASE_RAISE(LOST_CONNECTION, error);
        EXN_CASE_RAISE(MEMORY_ALLOCATION_FAILURE, error);
        EXN_CASE_RAISE(JOB_EXISTS, error); // see gearman_client_job_status()
        EXN_CASE_RAISE(JOB_QUEUE_FULL, error);
        EXN_CASE_RAISE(SERVER_ERROR, error);
        EXN_CASE_RAISE(WORK_ERROR, error);
        EXN_CASE_RAISE(WORK_DATA, error);
        EXN_CASE_RAISE(WORK_WARNING, error);
        EXN_CASE_RAISE(WORK_STATUS, error);
        EXN_CASE_RAISE(WORK_EXCEPTION, error);
        EXN_CASE_RAISE(WORK_FAIL, error);
        EXN_CASE_RAISE(NOT_CONNECTED, error);
        EXN_CASE_RAISE(COULD_NOT_CONNECT, error);
        EXN_CASE_RAISE(SEND_IN_PROGRESS, error); // DEPRECATED; SERVER ONLY
        EXN_CASE_RAISE(RECV_IN_PROGRESS, error); // DEPRECATED; SERVER ONLY
        EXN_CASE_RAISE(NOT_FLUSHING, error);
        EXN_CASE_RAISE(DATA_TOO_LARGE, error);
        EXN_CASE_RAISE(INVALID_FUNCTION_NAME, error);
        EXN_CASE_RAISE(INVALID_WORKER_FUNCTION, error);
        EXN_CASE_RAISE(NO_REGISTERED_FUNCTION, error);
        EXN_CASE_RAISE(NO_REGISTERED_FUNCTIONS, error);
        EXN_CASE_RAISE(NO_JOBS, error);
        EXN_CASE_RAISE(ECHO_DATA_CORRUPTION, error);
        EXN_CASE_RAISE(NEED_WORKLOAD_FN, error);
        EXN_CASE_RAISE(UNKNOWN_STATE, error);
        EXN_CASE_RAISE(PTHREAD, error); // DEPRECATED; SERVER ONLY
        EXN_CASE_RAISE(PIPE_EOF, error); // DEPRECATED; SERVER ONLY
        EXN_CASE_RAISE(QUEUE_ERROR, error); // DEPRECATED; SERVER ONLY
        EXN_CASE_RAISE(FLUSH_DATA, error); // Internal state
        EXN_CASE_RAISE(SEND_BUFFER_TOO_SMALL, error);
        EXN_CASE_RAISE(IGNORE_PACKET, error); // Internal only
        EXN_CASE_RAISE(UNKNOWN_OPTION, error); // DEPRECATED
        EXN_CASE_RAISE(TIMEOUT, error);
        EXN_CASE_RAISE(ARGUMENT_TOO_LARGE, error);
        EXN_CASE_RAISE(INVALID_ARGUMENT, error);
        EXN_CASE_RAISE(INVALID_SERVER_OPTION, error); // Bad server option sent to server
        EXN_CASE_RAISE(MAX_RETURN, error); /* Always add new error code before */
        default:
            return 0;
    }
}

#define RET_CASE(RETTYPE) \
case GEARMAN_##RETTYPE: { \
    ret_code_desc = #RETTYPE; \
    break; \
}

/* Return value: New reference */
static PyObject* pygear_describe_returncode(void* self, PyObject* args) {
    int return_code;
    if (!PyArg_ParseTuple(args, "i", &return_code)) {
        return NULL;
    }

    char* ret_code_desc = NULL;

    switch (return_code) {
        RET_CASE(SUCCESS);
        RET_CASE(IO_WAIT);
        RET_CASE(SHUTDOWN);
        RET_CASE(SHUTDOWN_GRACEFUL);
        RET_CASE(ERRNO);
        RET_CASE(EVENT); // DEPRECATED; SERVER ONLY
        RET_CASE(TOO_MANY_ARGS);
        RET_CASE(NO_ACTIVE_FDS); // No servers available
        RET_CASE(INVALID_MAGIC);
        RET_CASE(INVALID_COMMAND);
        RET_CASE(INVALID_PACKET);
        RET_CASE(UNEXPECTED_PACKET);
        RET_CASE(GETADDRINFO);
        RET_CASE(NO_SERVERS);
        RET_CASE(LOST_CONNECTION);
        RET_CASE(MEMORY_ALLOCATION_FAILURE);
        RET_CASE(JOB_EXISTS); // see gearman_client_job_status()
        RET_CASE(JOB_QUEUE_FULL);
        RET_CASE(SERVER_ERROR);
        RET_CASE(WORK_ERROR);
        RET_CASE(WORK_DATA);
        RET_CASE(WORK_WARNING);
        RET_CASE(WORK_STATUS);
        RET_CASE(WORK_EXCEPTION);
        RET_CASE(WORK_FAIL);
        RET_CASE(NOT_CONNECTED);
        RET_CASE(COULD_NOT_CONNECT);
        RET_CASE(SEND_IN_PROGRESS); // DEPRECATED; SERVER ONLY
        RET_CASE(RECV_IN_PROGRESS); // DEPRECATED; SERVER ONLY
        RET_CASE(NOT_FLUSHING);
        RET_CASE(DATA_TOO_LARGE);
        RET_CASE(INVALID_FUNCTION_NAME);
        RET_CASE(INVALID_WORKER_FUNCTION);
        RET_CASE(NO_REGISTERED_FUNCTION);
        RET_CASE(NO_REGISTERED_FUNCTIONS);
        RET_CASE(NO_JOBS);
        RET_CASE(ECHO_DATA_CORRUPTION);
        RET_CASE(NEED_WORKLOAD_FN);
        RET_CASE(UNKNOWN_STATE);
        RET_CASE(PTHREAD); // DEPRECATED; SERVER ONLY
        RET_CASE(PIPE_EOF); // DEPRECATED; SERVER ONLY
        RET_CASE(QUEUE_ERROR); // DEPRECATED; SERVER ONLY
        RET_CASE(FLUSH_DATA); // Internal state
        RET_CASE(SEND_BUFFER_TOO_SMALL);
        RET_CASE(IGNORE_PACKET); // Internal only
        RET_CASE(UNKNOWN_OPTION); // DEPRECATED
        RET_CASE(TIMEOUT);
        RET_CASE(ARGUMENT_TOO_LARGE);
        RET_CASE(INVALID_ARGUMENT);
        RET_CASE(INVALID_SERVER_OPTION); // Bad server option sent to server
        RET_CASE(MAX_RETURN); /* Always add new error code before */
        default:
            ret_code_desc = "GEARMAN_UNKNOWN";
    }

    return Py_BuildValue("s", ret_code_desc);
}
