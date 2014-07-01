#include "pygear.h"

PyMODINIT_FUNC initpygear(void){
    PyObject* m;

    pygear_ClientType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&pygear_ClientType) < 0){
        return;
    }

    // Initialize pygear module
    m = Py_InitModule3("pygear", pygear_class_methods, pygear_class_docstring);

    // Add Client class
    Py_INCREF(&pygear_ClientType);
    PyModule_AddObject(m, "Client", (PyObject *)&pygear_ClientType);
}

#define RET_CASE(RETTYPE) \
case GEARMAN_##RETTYPE: { \
    ret_code_desc = #RETTYPE; \
    break; \
}

static PyObject* pygear_describe_returncode(void* self, PyObject* args){
    int return_code;
    if (!PyArg_ParseTuple(args, "i", &return_code)){
        return NULL;
    }

    char* ret_code_desc = NULL;

    switch (return_code){
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
