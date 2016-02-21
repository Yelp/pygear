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
int _pygear_check_and_raise_exn(gearman_return_t return_code, const char* error);

#endif
