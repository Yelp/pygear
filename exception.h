#ifndef EXCEPTION_H
#define EXCEPTION_H

// Generic pygear exception
PyObject* PyGearExn_ERROR;

// Exceptions corresponding to non-zero libgearman return codes
PyObject* PyGearExn_SHUTDOWN;
PyObject* PyGearExn_SHUTDOWN_GRACEFUL;
PyObject* PyGearExn_ERRNO;
PyObject* PyGearExn_EVENT; // DEPRECATED; SERVER ONLY
PyObject* PyGearExn_TOO_MANY_ARGS;
PyObject* PyGearExn_NO_ACTIVE_FDS; // No servers available
PyObject* PyGearExn_INVALID_MAGIC;
PyObject* PyGearExn_INVALID_COMMAND;
PyObject* PyGearExn_INVALID_PACKET;
PyObject* PyGearExn_UNEXPECTED_PACKET;
PyObject* PyGearExn_GETADDRINFO;
PyObject* PyGearExn_NO_SERVERS;
PyObject* PyGearExn_LOST_CONNECTION;
PyObject* PyGearExn_MEMORY_ALLOCATION_FAILURE;
PyObject* PyGearExn_JOB_EXISTS; // see gearman_client_job_status()
PyObject* PyGearExn_JOB_QUEUE_FULL;
PyObject* PyGearExn_SERVER_ERROR;
PyObject* PyGearExn_WORK_ERROR;
PyObject* PyGearExn_WORK_DATA;
PyObject* PyGearExn_WORK_WARNING;
PyObject* PyGearExn_WORK_STATUS;
PyObject* PyGearExn_WORK_EXCEPTION;
PyObject* PyGearExn_WORK_FAIL;
PyObject* PyGearExn_NOT_CONNECTED;
PyObject* PyGearExn_COULD_NOT_CONNECT;
PyObject* PyGearExn_SEND_IN_PROGRESS; // DEPRECATED; SERVER ONLY
PyObject* PyGearExn_RECV_IN_PROGRESS; // DEPRECATED; SERVER ONLY
PyObject* PyGearExn_NOT_FLUSHING;
PyObject* PyGearExn_DATA_TOO_LARGE;
PyObject* PyGearExn_INVALID_FUNCTION_NAME;
PyObject* PyGearExn_INVALID_WORKER_FUNCTION;
PyObject* PyGearExn_NO_REGISTERED_FUNCTION;
PyObject* PyGearExn_NO_REGISTERED_FUNCTIONS;
PyObject* PyGearExn_NO_JOBS;
PyObject* PyGearExn_ECHO_DATA_CORRUPTION;
PyObject* PyGearExn_NEED_WORKLOAD_FN;
PyObject* PyGearExn_UNKNOWN_STATE;
PyObject* PyGearExn_PTHREAD; // DEPRECATED; SERVER ONLY
PyObject* PyGearExn_PIPE_EOF; // DEPRECATED; SERVER ONLY
PyObject* PyGearExn_QUEUE_ERROR; // DEPRECATED; SERVER ONLY
PyObject* PyGearExn_FLUSH_DATA; // Internal state; should never be seen by either client or worker.
PyObject* PyGearExn_SEND_BUFFER_TOO_SMALL;
PyObject* PyGearExn_IGNORE_PACKET; // Internal only
PyObject* PyGearExn_UNKNOWN_OPTION; // DEPRECATED
PyObject* PyGearExn_TIMEOUT;
PyObject* PyGearExn_ARGUMENT_TOO_LARGE;
PyObject* PyGearExn_INVALID_ARGUMENT;
PyObject* PyGearExn_INVALID_SERVER_OPTION; // Bad server option sent to server
PyObject* PyGearExn_MAX_RETURN; /* Always add new error code before */

/**
 * Check a gearman return code, and set a python exception if the return value
 * indicates an error.
 *
 * Return: 0 on no error, 1 on error.
 */
int _pygear_check_and_raise_exn(gearman_return_t return_code);

#endif
