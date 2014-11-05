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

#include "admin.h"


/*
 * Class constructor / destructor methods
 */

PyObject* Admin_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    pygear_AdminObject* self;
    self = (pygear_AdminObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->sockfd = -1;
        self->host = NULL;
        self->port = 4730;
        self->timeout = ADMIN_DEFAULT_TIMEOUT;
    }
    return (PyObject *)self;
}

int Admin_init(pygear_AdminObject* self, PyObject* args, PyObject*kwds) {
    char* host;
    int port;
    if (!PyArg_ParseTuple(args, "si", &host, &port)) {
        return -1;
    }
    self->host = strdup(host);
    self->port = port;
    self->timeout = ADMIN_DEFAULT_TIMEOUT;
    self->sockfd = -1;
    return 0;
}

void Admin_dealloc(pygear_AdminObject* self) {
    if (self->host != NULL) {
        free(self->host);
    }
    if (self->sockfd > 0) {
        close(self->sockfd);
    }
    self->ob_type->tp_free((PyObject*)self);
}


/*******************
 * Private methods *
 *******************/

/*
 * Verify that the connection to the gearman server exists and is open.
 * On success, return a nonnegative integer, the socket file descriptor.
 * On failure, return -1 and errno is set to indicate the error.
 */
static int _pygear_admin_check_server_connection(pygear_AdminObject* self) {
    if (self->sockfd < 0) {
        self->sockfd = socket(AF_INET, SOCK_STREAM, 0);
        // socket() - creates an unbound socket in a communication domain, and returns a file descriptor
        // AF_INET - Internet domain sockets domain
        // SOCK_STREAM - Byte-stream socket type
        // 0 - Use an unspecified default protocol
        if (self->sockfd < 0) {
            PyErr_SetString(PyGearExn_ERROR, "Failed to open socket");
            return -1;
        }
        struct hostent* server = gethostbyname(self->host);
        // struct hostent - represent an entry in the hosts database, with the following members:
        // char *h_name, char **h_aliases, int h_addrtype, int h_length, char **h_addr_list, char *h_addr
        if (server == NULL) {
            PyObject* err_string = PyString_FromFormat("Failed to connect: No such host: %s", self->host);
            PyErr_SetObject(PyGearExn_ERROR, err_string);
            Py_XDECREF(err_string);
            close(self->sockfd);
            self->sockfd = -1;
            return self->sockfd;
        }
        struct sockaddr_in server_addr;
        // struct sockaddr_in - for handling internet addresses, with the following members:
        // short sin_family, unsigned short sin_port, struct in_addr sin_addr, char sin_zero[8]
        memset((char *) &server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        memcpy((char *) &server_addr.sin_addr.s_addr, (char *)server->h_addr, server->h_length);
        server_addr.sin_port = htons(self->port);
        // htons() - converts unsigned short integer hostshort from host byte order to network byte order
        struct timeval tv; // elapsed time
        tv.tv_sec = self->timeout;
        tv.tv_usec = 0;
        if (setsockopt(self->sockfd, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(struct timeval)) == -1) {
            // setsockopt() - set the socket options
            // SOL_SOCKET - set options at the socket level
            // SO_RCVTIMEO - sets the timeout value an input function waits until it completes
            PyObject* err_string = PyString_FromFormat("Failed to set timeout: Socket error %s", strerror(errno));
            PyErr_SetObject(PyGearExn_ERROR, err_string);
            Py_XDECREF(err_string);
            close(self->sockfd);
            self->sockfd = -1;
            return self->sockfd;
        }
        if (connect(self->sockfd,(struct sockaddr *) &server_addr, sizeof(server_addr)) < 0) {
            // connect() - initiate a connection on a socket, return 0 on success
            PyObject* err_string = PyString_FromFormat("Failed to connect: Socket error %s", strerror(errno));
            PyErr_SetObject(PyGearExn_ERROR, err_string);
            Py_XDECREF(err_string);
            close(self->sockfd);
            self->sockfd = -1;
            return self->sockfd;
        }
    }
    return self->sockfd;
}


/*
 * Parse single-line responses that are formatted "OK [result]"
 * On success, put a pointer to the list of string elements after OK in *result
 * On failure, set *result to NULL
 */
static bool _pygear_extract_response_success(PyObject* result_string, PyObject** result) {
    PyObject* stripped_result = NULL;
    PyObject* split_result = NULL;
    PyObject* ok_string = NULL;
    *result = NULL;
    stripped_result = PyObject_CallMethod(result_string, "strip", "");
    if (!stripped_result) {
        goto catch;
    }
    split_result = PyObject_CallMethod(stripped_result, "split", "s", " ");
    if (!split_result) {
        goto catch;
    }
    if (!PyList_Check(split_result)) {
        goto catch;
    }
    PyObject* gearman_status = PyList_GetItem(split_result, 0);
    ok_string = PyString_FromString("OK");
    int cmp_result = PyObject_RichCompareBool(gearman_status, ok_string, Py_EQ);
    if (cmp_result == 1) {
        *result = PyList_GetSlice(split_result, 1, PyList_Size(split_result)); // new ref
    }
catch:
    Py_XDECREF(stripped_result);
    Py_XDECREF(split_result);
    Py_XDECREF(ok_string);
    if (*result == NULL) {
        return false;
    }
    return true;
}


/*
 * Verifies that the gearman server's response was prepended with "OK".
 */
static bool _pygear_admin_response_with_ok(PyObject* response_str) {
    if (response_str == NULL || response_str == Py_None) {
        return false;
    }
    char* resp = PyString_AsString(response_str);
    if (resp == NULL) {
        return false;
    }
    if (strncmp(resp, "OK", 2) == 0) {
        return true;
    }
    return false;
}


/*
 * Verifies that parsed_result is a list of size one.
 * Raise an appropriate exception on failure.
 */
static bool _pygear_admin_check_list_size_is_one_and_raise(PyObject* raw_result, PyObject* parsed_result) {
    PyTypeObject* ret_type = NULL;
    bool success = false;
    if (!PyList_Check(parsed_result)) {
        ret_type = (PyTypeObject *) PyObject_Type(parsed_result);
        if (!ret_type) {
            PyErr_SetString(PyGearExn_ERROR, "An error was encountered while attempting to throw an exception.");
            goto catch;
        }
        PyObject* err_string = PyString_FromFormat("Unexpected internal result: Expected list, got %s", ret_type->tp_name);
        PyErr_SetObject(PyGearExn_ERROR, err_string);
        Py_XDECREF(err_string);
        goto catch;
    }
    if (PyList_Size(parsed_result) != 1) {
        char* result_line = PyString_AsString(raw_result);
        PyObject* err_string = PyString_FromFormat("Unexpected number of values in response (%s)", result_line);
        PyErr_SetObject(PyGearExn_ERROR, err_string);
        Py_XDECREF(err_string);
        goto catch;
    }
    success = true;
catch:
    Py_XDECREF(ret_type);
    return success;
}


/*
 * Called when _pygear_extract_response_success fails.
 * Sets the appropriate Python excecption, nothing more.
 */
void _pygear_admin_raise_exception(PyObject* raw_result) {
    char* raw_result_string;
    if (raw_result == NULL) {
        PyErr_SetString(PyGearExn_COULD_NOT_CONNECT, "Connection Failed: socket error communicating with the host");
        return;
    }
    if (raw_result == Py_None) {
        raw_result_string = "no data received";
    } else {
        raw_result_string = PyString_AsString(raw_result);
    }
    if (raw_result_string != NULL) {
        PyObject* err_string = PyString_FromFormat("Failed to parse server response (%s)", raw_result_string);
        PyErr_SetObject(PyGearExn_ERROR, err_string);
        Py_XDECREF(err_string);
    } else {
        PyErr_SetString(PyGearExn_ERROR, "An error was encountered while attempting to throw an exception.");
    }
}


/*
 * Check if the server responded with ERR ...
 * If so, set an exception and return true.
 */
static bool _check_and_raise_server_error(PyObject* response_string) {
    char* resp = PyString_AsString(response_string);
    if (resp == NULL) {
        return true;
    }
    if (!strncmp(resp, "ERR", 3)) {
        PyErr_SetString(PyGearExn_ERROR, resp);
        return true;
    }
    return false;
}


static bool string_endswith(const char* haystack, const char* needle) {
    size_t haystack_len, needle_len;
    haystack_len = strlen(haystack);
    needle_len = strlen(needle);
    if (haystack_len < needle_len) {
        return false;
    }
    if (strncmp(&(haystack[haystack_len - needle_len]), needle, needle_len) == 0) {
        return true;
    }
    return false;
}


static PyObject* _pygear_admin_make_call(pygear_AdminObject* self, char* command, char* eom_mark) {
    if (_pygear_admin_check_server_connection(self) < 0) {
        return NULL;
    }
    size_t bytes_written = write(self->sockfd, command, strlen(command));
    // write() - write to a file descriptor
    // return number of bytes written on success, return -1 and set errno on failure
    if (bytes_written < 0) {
        return NULL;
    }
    PyObject* ret = NULL;

#define SOCKET_BUFSIZE 4096  // read 4K each time

    char* result = malloc(sizeof(char) * SOCKET_BUFSIZE);
    char buf[SOCKET_BUFSIZE];
    size_t result_bytes = 0;
    size_t bytes_read = 0;
    do {
        errno = 0;
        bytes_read = read(self->sockfd, buf, SOCKET_BUFSIZE);
        // read() - read data on a socket
        int read_err = errno;
        if (read_err == EAGAIN) { // EAGAIN - there is no data available right now
            break;
        }
        if (bytes_read < 0) {
            PyObject* err_string = PyString_FromFormat("Failed to read from socket: %s", strerror(read_err));
            PyErr_SetObject(PyGearExn_ERROR, err_string);
            Py_XDECREF(err_string);
            goto catch;
        }
        // append buf to result
        result = realloc(result, sizeof(char) * (result_bytes + bytes_read + 1));
        strncpy(&(result[result_bytes]), buf, bytes_read);
        result_bytes += bytes_read;
        result[result_bytes] = '\0';
        if (string_endswith(result, eom_mark)) {
            break;
        }
    } while (1);
    ret = Py_BuildValue("s#", result, result_bytes);

catch:
    if (result != NULL) {
        free(result);
    }
    return ret;
}


/********************
 * Instance methods *
 ********************/

static PyObject* pygear_admin_cancel_job(pygear_AdminObject* self, PyObject* args) {
    char* job_handle;
    int job_handle_len;
    if (!PyArg_ParseTuple(args, "s#", &job_handle, &job_handle_len)) {
        return NULL;
    }
    char* format_string = "cancel job %s\r\n";
    char* buffer = malloc(sizeof(char) * (strlen(format_string) + job_handle_len));
    sprintf(buffer, format_string, job_handle);
    PyObject* raw_result = _pygear_admin_make_call(self, buffer, "\n");
    if (buffer != NULL) {
        free(buffer);
    }
    if (!_pygear_admin_response_with_ok(raw_result)) {
        _pygear_admin_raise_exception(raw_result);
        Py_XDECREF(raw_result);
        return NULL;
    }
    Py_XDECREF(raw_result);
    Py_RETURN_NONE;
}


static PyObject* pygear_admin_clone(pygear_AdminObject* self) {
    PyObject *argList = Py_BuildValue("(O, O)", Py_None, Py_None);
    pygear_AdminObject* python_admin = (pygear_AdminObject*) PyObject_CallObject((PyObject *) &pygear_AdminType, argList);
    python_admin->host = strdup(self->host);
    python_admin->port = self->port;
    python_admin->timeout = self->timeout;
    PyObject* ret = Py_BuildValue("O", python_admin);
    Py_XDECREF(argList);
    Py_XDECREF(python_admin);
    return ret;
}


static PyObject* pygear_admin_create_function(pygear_AdminObject* self, PyObject* args) {
    char* function_name;
    int function_name_len;
    if (!PyArg_ParseTuple(args, "s#", &function_name, &function_name_len)) {
        return NULL;
    }
    char* format_string = "create function  %s\r\n";
    char* command_buffer = malloc(sizeof(char) * (strlen(format_string) + function_name_len));
    sprintf(command_buffer, format_string, function_name);
    PyObject* raw_result = _pygear_admin_make_call(self, command_buffer, "\n");
    if (command_buffer != NULL) {
        free(command_buffer);
    }
    if (!_pygear_admin_response_with_ok(raw_result)) {
        _pygear_admin_raise_exception(raw_result);
        Py_XDECREF(raw_result);
        return NULL;
    }
    Py_XDECREF(raw_result);
    Py_RETURN_NONE;
}


static PyObject* pygear_admin_drop_function(pygear_AdminObject* self, PyObject* args) {
    char* function_name;
    int function_name_len;
    if (!PyArg_ParseTuple(args, "s#", &function_name, &function_name_len)) {
        return NULL;
    }
    char* format_string = "drop function  %s\r\n";
    char* command_buffer = malloc(sizeof(char) * (strlen(format_string) + function_name_len));
    sprintf(command_buffer, format_string, function_name);
    PyObject* raw_result = _pygear_admin_make_call(self, command_buffer, "\n");
    if (command_buffer != NULL) {
        free(command_buffer);
    }
    if (!_pygear_admin_response_with_ok(raw_result)){
        _pygear_admin_raise_exception(raw_result);
        Py_XDECREF(raw_result);
        return NULL;
    }
    Py_XDECREF(raw_result);
    Py_RETURN_NONE;
}


static PyObject* pygear_admin_getpid(pygear_AdminObject* self) {
    PyObject* raw_result = NULL;
    PyObject* parsed_result = NULL;
    PyObject* pid_str = NULL;
    bool success = false;
    raw_result = _pygear_admin_make_call(self, "getpid\r\n", "\n");
    if (!_pygear_extract_response_success(raw_result, &parsed_result)) {
        _pygear_admin_raise_exception(raw_result);
        goto catch;
    }
    if (!_pygear_admin_check_list_size_is_one_and_raise(raw_result, parsed_result)) {
        goto catch;
    }
    success = true;
    pid_str = PyList_GetItem(parsed_result, 0);

catch:
    Py_XDECREF(raw_result);
    Py_XDECREF(parsed_result);
    if (success) {
        return PyNumber_Int(pid_str);
    }
    return NULL;
}


static PyObject* pygear_admin_maxqueue(pygear_AdminObject* self, PyObject* args) {
    char* command_buffer = NULL;
    char* queue_name = NULL;
    char* format_string = "maxqueue %s %d\r\n";
    int queue_name_len, queue_size;
    PyObject* raw_result = NULL;
    bool success = false;
    if (!PyArg_ParseTuple(args, "s#i", &queue_name, &queue_name_len, &queue_size)) {
        goto catch;
    }
    command_buffer = malloc(sizeof(char) * (queue_name_len + strlen(format_string) + 32));
    sprintf(command_buffer, format_string, queue_name, queue_size);
    raw_result = _pygear_admin_make_call(self, command_buffer, "\n");
    if (!_pygear_admin_response_with_ok(raw_result)) {
        _pygear_admin_raise_exception(raw_result);
        goto catch;
    }
    success = true;

catch:
    Py_XDECREF(raw_result);
    if (command_buffer != NULL) {
        free(command_buffer);
    }
    if (success) {        
        Py_RETURN_NONE;
    }
    return NULL;
}


static PyObject* pygear_admin_set_server(pygear_AdminObject* self, PyObject* args) {
    char* host;
    if (!PyArg_ParseTuple(args, "si", &host, &self->port)) {
        return NULL;
    }
    if (self->host) {
        free(self->host);
    }
    self->host = strdup(host);
    if (self->sockfd) {
        close(self->sockfd);
        self->sockfd = -1;
    }
    Py_RETURN_NONE;
}


/*
 * Set the socket timeout (in seconds).
 * To take effect we need to recreate the socket, so clear it here.
 * It will be recreated with the new timeout next time _check_server_connection
 * is called.
 */
static PyObject* pygear_admin_set_timeout(pygear_AdminObject* self, PyObject* args) {
    float timeout;
    if (!PyArg_ParseTuple(args, "f", &timeout)) {
        return NULL;
    }
    self->timeout = timeout;
    close(self->sockfd);
    self->sockfd = -1;
    Py_RETURN_NONE;
}


static PyObject* pygear_admin_show_jobs(pygear_AdminObject* self) {
    PyObject* raw_result = NULL;
    PyObject* status_string = NULL;
    PyObject* status_string_trip = NULL;
    PyObject* status_list = NULL;
    PyObject* status_dict_list = NULL;
    PyObject* status_dict = NULL;
    PyObject* status_line_list = NULL;

    PyObject* key0 = PyString_FromString("handle");
    PyObject* key1 = PyString_FromString("retries");
    PyObject* key2 = PyString_FromString("ignore_job");
    PyObject* key3 = PyString_FromString("job_queued");

    PyObject* statfield_1_new = NULL;
    PyObject* statfield_2_new = NULL;
    PyObject* statfield_3_new = NULL;

    raw_result = _pygear_admin_make_call(self, "show jobs\r\n", "\n.\n");
    if (!raw_result) {
        goto catch;
    }
    if (_check_and_raise_server_error(raw_result)) {
        goto catch;
    }
    status_string = PyObject_CallMethod(raw_result, "replace", "ss", ".\n", " ");
    if (!status_string) {
        goto catch;
    }
    status_string_trip = PyObject_CallMethod(status_string, "strip", "");
    if (!status_string_trip) {
        goto catch;
    }
    status_list = PyObject_CallMethod(status_string_trip, "split", "s", "\n");
    if (!status_list) {
        goto catch;
    }
    status_dict_list = PyList_New(0);
    if (!status_dict_list) {
        goto catch;
    }

    int status_i;
    for (status_i = 0; status_i < PyList_Size(status_list); status_i++) {

        PyObject* status_line = PyList_GetItem(status_list, status_i);
        if (!status_line) {
            goto catch;
        }
        status_line_list = PyObject_CallMethod(status_line, "split", "s", "\t");
        if (!status_line_list) {
            goto catch;
        }

        // If the server status is empty, we will get a line with only one entry.
        // It should be skipped.
        if (PyList_Size(status_line_list) < 4) {
            continue;
        }

        PyObject* statfield_0 = PyList_GetItem(status_line_list, 0);
        if (!statfield_0) {
            goto catch;
        }
        PyObject* statfield_1 = PyList_GetItem(status_line_list, 1);
        if (!statfield_1) {
            goto catch;
        }
        PyObject* statfield_2 = PyList_GetItem(status_line_list, 2);
        if (!statfield_2) {
            goto catch;
        }
        PyObject* statfield_3 = PyList_GetItem(status_line_list, 3);
        if (!statfield_3) {
            goto catch;
        }

        statfield_1_new = PyNumber_Int(statfield_1);
        if (!statfield_1) {
            goto catch;
        }
        statfield_2_new = PyNumber_Int(statfield_2);
        if (!statfield_2) {
            goto catch;
        }
        statfield_3_new = PyNumber_Int(statfield_3);
        if (!statfield_3) {
            goto catch;
        }
        status_dict = PyDict_New();
        if (!status_dict) {
            goto catch;
        }

        PyDict_SetItem(status_dict, key0, statfield_0);
        PyDict_SetItem(status_dict, key1, statfield_1_new);
        PyDict_SetItem(status_dict, key2, statfield_2_new);
        PyDict_SetItem(status_dict, key3, statfield_3_new);

        Py_XDECREF(statfield_1_new);
        Py_XDECREF(statfield_2_new);
        Py_XDECREF(statfield_3_new);

        PyList_Append(status_dict_list, status_dict);

        Py_XDECREF(status_dict);
        Py_XDECREF(status_line_list);
    }

catch:
    Py_XDECREF(raw_result);
    Py_XDECREF(status_string);
    Py_XDECREF(status_string_trip);
    Py_XDECREF(status_list);
    Py_XDECREF(status_dict_list);
    Py_XDECREF(status_dict);
    Py_XDECREF(status_line_list);
    Py_XDECREF(key0);
    Py_XDECREF(key1);
    Py_XDECREF(key2);
    Py_XDECREF(key3);
    Py_XDECREF(statfield_1_new);
    Py_XDECREF(statfield_2_new);
    Py_XDECREF(statfield_3_new);

    return status_dict_list;
}


static PyObject* pygear_admin_show_unique_jobs(pygear_AdminObject* self) {
    PyObject* raw_result = NULL;
    PyObject* status_string = NULL;
    PyObject* status_string_trip = NULL;
    PyObject* uuid_list = NULL;

    raw_result = _pygear_admin_make_call(self, "show unique jobs\r\n", "\n.\n");
    if (!raw_result) {
        goto catch;
    }
    if (_check_and_raise_server_error(raw_result)) {
        goto catch;
    }
    status_string = PyObject_CallMethod(raw_result, "replace", "ss", ".\n", " ");
    if (!status_string) {
        goto catch;
    }
    status_string_trip = PyObject_CallMethod(status_string, "strip", "");
    if (!status_string_trip) {
        goto catch;
    }
    uuid_list = PyObject_CallMethod(status_string_trip, "split", "s", "\n");

catch:
    Py_XDECREF(raw_result);
    Py_XDECREF(status_string);
    Py_XDECREF(status_string_trip);
    return uuid_list;
}


static PyObject* pygear_admin_shutdown(pygear_AdminObject* self, PyObject* args) {
    int graceful;
    if (!PyArg_ParseTuple(args, "i", &graceful)){
        return NULL;
    }
    PyObject* raw_result = NULL;
    if (graceful) {
         raw_result = _pygear_admin_make_call(self, "shutdown\r\n", "\n");
    } else {
         raw_result = _pygear_admin_make_call(self, "shutdown graceful\r\n", "\n");
    }
    if (!_pygear_admin_response_with_ok(raw_result)) {
        _pygear_admin_raise_exception(raw_result);
        Py_XDECREF(raw_result);
        return NULL;
    }
    Py_XDECREF(raw_result);
    Py_RETURN_NONE;
}


static PyObject* pygear_admin_status(pygear_AdminObject* self) {
    PyObject* raw_result = NULL; 
    PyObject* status_string = NULL;
    PyObject* status_string_trip = NULL;
    PyObject* status_list = NULL;
    PyObject* status_dict_list = NULL;
    PyObject* status_line_list = NULL;

    PyObject* key0 = PyString_FromString("function");
    PyObject* key1 = PyString_FromString("total");
    PyObject* key2 = PyString_FromString("running");
    PyObject* key3 = PyString_FromString("available_workers");

    PyObject* statfield_1_new = NULL;
    PyObject* statfield_2_new = NULL;
    PyObject* statfield_3_new = NULL;

    raw_result = _pygear_admin_make_call(self, "status\r\n", "\n.\n");
    if (!raw_result) {
        goto catch;
    }
    if (_check_and_raise_server_error(raw_result)) {
        goto catch;
    }
    status_string = PyObject_CallMethod(raw_result, "replace", "ss", ".\n", " ");
    if (!status_string) {
        goto catch;
    }
    status_string_trip = PyObject_CallMethod(status_string, "strip", "");
    if (!status_string_trip) {
        goto catch;
    }
    status_list = PyObject_CallMethod(status_string_trip, "split", "s", "\n");
    if (!status_list) {
        goto catch;
    }
    status_dict_list = PyList_New(0);
    if (!status_dict_list) {
        goto catch;
    }

    int status_i;
    for (status_i = 0; status_i < PyList_Size(status_list); status_i++) {

        PyObject* status_line = PyList_GetItem(status_list, status_i);
        if (!status_line) {
            goto catch;
        }
        status_line_list = PyObject_CallMethod(status_line, "split", "s", "\t");
        if (!status_line_list) {
            goto catch;
        }
        // If the server status is empty, we will get a line with only one entry.
        // It should be skipped.
        if (PyList_Size(status_line_list) < 4) {
            continue;
        }

        PyObject* statfield_0 = PyList_GetItem(status_line_list, 0);
        if (!statfield_0) {
            goto catch;
        }
        PyObject* statfield_1 = PyList_GetItem(status_line_list, 1);
        if (!statfield_1) {
            goto catch;
        }
        PyObject* statfield_2 = PyList_GetItem(status_line_list, 2);
        if (!statfield_2) {
            goto catch;
        }
        PyObject* statfield_3 = PyList_GetItem(status_line_list, 3);
        if (!statfield_3) {
            goto catch;
        }

        statfield_1_new = PyNumber_Int(statfield_1);
        if (!statfield_1_new) {
            goto catch;
        }
        statfield_2_new = PyNumber_Int(statfield_2);
        if (!statfield_2_new) {
            goto catch;
        }
        statfield_3_new = PyNumber_Int(statfield_3);
        if (!statfield_3_new) {
            goto catch;
        }

        PyObject* status_dict = PyDict_New();
        if (!status_dict) {
            goto catch;
        }

        // PyDict_SetItem does not steal references
        PyDict_SetItem(status_dict, key0, statfield_0);
        PyDict_SetItem(status_dict, key1, statfield_1_new);
        PyDict_SetItem(status_dict, key2, statfield_2_new);
        PyDict_SetItem(status_dict, key3, statfield_3_new);

        Py_XDECREF(statfield_1_new);
        Py_XDECREF(statfield_2_new);
        Py_XDECREF(statfield_3_new);

        PyList_Append(status_dict_list, status_dict);

        Py_XDECREF(status_dict);
        Py_XDECREF(status_line_list);
    }

catch:
    Py_XDECREF(raw_result); 
    Py_XDECREF(status_string);
    Py_XDECREF(status_string_trip);
    Py_XDECREF(status_list);
    Py_XDECREF(status_dict_list);
    Py_XDECREF(status_line_list);
    Py_XDECREF(key0);
    Py_XDECREF(key1);
    Py_XDECREF(key2);
    Py_XDECREF(key3);
    Py_XDECREF(statfield_1_new);
    Py_XDECREF(statfield_2_new);
    Py_XDECREF(statfield_3_new);

    return status_dict_list;
}


static PyObject* pygear_admin_verbose(pygear_AdminObject* self) {
    PyObject* raw_result = NULL;
    PyObject* parsed_result = NULL;
    PyObject* ret = NULL;
    raw_result = _pygear_admin_make_call(self, "verbose\r\n", "\n");
    if (!_pygear_extract_response_success(raw_result, &parsed_result)) {
        _pygear_admin_raise_exception(raw_result);
        goto catch;
    }
    if (!_pygear_admin_check_list_size_is_one_and_raise(raw_result, parsed_result)) {
        goto catch;
    }
    ret = PyList_GetItem(parsed_result, 0);
catch:
    Py_XDECREF(raw_result);
    Py_XDECREF(parsed_result);
    return ret;
}


static PyObject* pygear_admin_version(pygear_AdminObject* self) {
    PyObject* raw_result = _pygear_admin_make_call(self, "version\r\n", "\n");
    PyObject* parsed_result = NULL;
    PyObject* ret = NULL;
    if (!_pygear_extract_response_success(raw_result, &parsed_result)) { // parsed_result will be assigned a new ref
        _pygear_admin_raise_exception(raw_result);
        goto catch;
    }
    if (!_pygear_admin_check_list_size_is_one_and_raise(raw_result, parsed_result)) {
        goto catch;
    }
    ret = PyList_GetItem(parsed_result, 0);
catch:
    Py_XDECREF(raw_result);
    Py_XDECREF(parsed_result);
    return ret;
}


static PyObject* pygear_admin_workers(pygear_AdminObject* self) {
    PyObject* raw_result = NULL;
    PyObject* worker_string = NULL;
    PyObject* worker_string_trip = NULL;
    PyObject* worker_list = NULL;
    PyObject* worker_dict_list = NULL;
    PyObject* worker_line_list = NULL;
    PyObject* worker_function_list = NULL;
    PyObject* worker_dict = NULL;

    PyObject* key0 = PyString_FromString("fd");
    PyObject* key1 = PyString_FromString("ip_address");
    PyObject* key2 = PyString_FromString("client_id");
    PyObject* key3 = PyString_FromString("functions");

    raw_result = _pygear_admin_make_call(self, "workers\r\n", "\n.\n");
    if (!raw_result) {
        goto catch;
    }
    if (_check_and_raise_server_error(raw_result)) {
        goto catch;
    }
    worker_string = PyObject_CallMethod(raw_result, "replace", "ss", ".\n", " ");
    if (!worker_string) {
        goto catch;
    }
    worker_string_trip = PyObject_CallMethod(worker_string, "strip", "");
    if (!worker_string_trip) {
        goto catch;
    }
    worker_list = PyObject_CallMethod(worker_string_trip, "split", "s", "\n");
    if (!worker_list) {
        goto catch;
    }
    worker_dict_list = PyList_New(0);
    if (!worker_dict_list) {
        goto catch;
    }

    int worker_i;
    for (worker_i = 0; worker_i < PyList_Size(worker_list); worker_i++) {

        PyObject* worker_line = PyList_GetItem(worker_list, worker_i);
        if (!worker_line) {
            goto catch;
        }
        worker_line_list = PyObject_CallMethod(worker_line, "split", "s", " ");
        if (!worker_line_list) {
            goto catch;
        }

        // worker_line_list should have at least 4 elements -
        // 'FD IP-ADDRESS CLIENT-ID :' and potentially 'FUNCTION ...'
        if (PyList_Size(worker_line_list) < 4) {
            PyObject* err_string = PyString_FromFormat("Malformed response line from server: '%s'",
                    PyString_AsString(worker_line));
            PyErr_SetObject(PyGearExn_ERROR, err_string);
            Py_XDECREF(err_string);
            goto catch;
        }

        worker_function_list = PyList_New(0);
        int list_i;
        for (list_i = 5; list_i < PyList_Size(worker_line_list); list_i++) {
            PyObject* method_result = NULL;
            method_result = PyObject_CallMethod(worker_function_list, "append", "O", PyList_GetItem(worker_line_list, list_i));
            Py_XDECREF(method_result);
        }

        PyObject* worker_fd = PyList_GetItem(worker_line_list, 0);
        if (!worker_fd) {
            goto catch;
        }
        PyObject* worker_ip_addr = PyList_GetItem(worker_line_list, 1);
        if (!worker_ip_addr) {
            goto catch;
        }
        PyObject* worker_client_id = PyList_GetItem(worker_line_list, 2);
        if (!worker_client_id) {
            goto catch;
        }

        worker_dict = PyDict_New();
        if (!worker_dict) {
            goto catch;
        }

        PyDict_SetItem(worker_dict, key0, worker_fd);
        PyDict_SetItem(worker_dict, key1, worker_ip_addr);
        PyDict_SetItem(worker_dict, key2, worker_client_id);
        PyDict_SetItem(worker_dict, key3, worker_function_list);

        PyList_Append(worker_dict_list, worker_dict);

        Py_XDECREF(worker_dict);
        Py_XDECREF(worker_line_list);
        Py_XDECREF(worker_function_list);
    }

catch:
    Py_XDECREF(raw_result);
    Py_XDECREF(worker_string);
    Py_XDECREF(worker_string_trip);
    Py_XDECREF(worker_list);
    Py_XDECREF(worker_dict_list);
    Py_XDECREF(worker_line_list);
    Py_XDECREF(worker_function_list);
    Py_XDECREF(worker_dict);
    Py_XDECREF(key0);
    Py_XDECREF(key1);
    Py_XDECREF(key2);
    Py_XDECREF(key3);
    return worker_dict_list;
}
