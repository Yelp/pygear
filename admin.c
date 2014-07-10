#include "admin.h"

#define SOCKET_BUFSIZE 4096

/*
 * Class constructor / destructor methods
 */

PyObject* Admin_new(PyTypeObject *type, PyObject *args, PyObject *kwds){
    pygear_AdminObject* self;

    self = (pygear_AdminObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->socket_module = NULL;
        self->conn = NULL;
        self->host = NULL;
        self->port = 4730;
        self->timeout = 5;
    }

    return (PyObject *)self;
}

int Admin_init(pygear_AdminObject *self, PyObject *args, PyObject *kwds){
    char* host;
    int port;
    if (!PyArg_ParseTuple(args, "si", &host, &port)){
        return -1;
    }

    self->host = strdup(host);
    self->port = port;

    self->socket_module = PyImport_ImportModule("socket");
    self->socket_error = PyObject_GetAttrString(self->socket_module, "error");
    if (self->socket_module == NULL
    ||  self->socket_error  == NULL) {
        return -1;
    }

    return 0;
}

void Admin_dealloc(pygear_AdminObject* self){
    if (self->host != NULL){
        free(self->host);
    }

    Py_XDECREF(self->socket_module);
    Py_XDECREF(self->conn);

    self->ob_type->tp_free((PyObject*)self);
}

/********************
 * Instance methods *
 ********************
 */

/*
 * Verify that the connection to the gearman server exists and is open.
 * If the socket is extant and connected, return a pointer to the PyObject.
 * If there is an error connecting, set an error and return NULL
 */
static PyObject* _pygear_admin_check_connection(pygear_AdminObject* self){
    if (self->conn){
        // Test the connection
    } else {
        // Try to create a connection.
        PyObject* af_inet = PyObject_GetAttrString(self->socket_module, "AF_INET");
        PyObject* sock_stream = PyObject_GetAttrString(self->socket_module, "SOCK_STREAM");
        self->conn = PyObject_CallMethod(self->socket_module, "socket", "O, O", af_inet, sock_stream);
        if (self->conn){
            PyObject* args = Py_BuildValue("(s, i)", self->host, self->port);
            PyObject_CallMethod(self->conn, "connect", "(O)", args);
            if (PyErr_Occurred()){
                // Check if the problem was a socket error.
                if (PyErr_ExceptionMatches(self->socket_error)){
                    Py_XDECREF(self->conn);
                    self->conn = NULL;
                }
            }
        }
    }
    return self->conn;
}

/*
 * Parse single-line responses that are formatted "OK [result]"
 * Return 1 on success, 0 on failure
 * On success, puts a pointer to the list of string elements after OK in
 * the pointer *result
 * On fail, *result is set to NULL
 */
static int _pygear_extract_response(PyObject* result_string, PyObject** result){
    PyObject* stripped_result = PyObject_CallMethod(result_string, "strip", "");
    if (!stripped_result){
        *result = NULL;
        return 0;
    }
    PyObject* split_result = PyObject_CallMethod(stripped_result, "split", "s", " ");
    if (!split_result){
        *result = NULL;
        return 0;
    }
    if (!PyList_Check(split_result)){
        *result = NULL;
        return 0;
    }
    PyObject* gearman_status = PyList_GetItem(split_result, 0);
    PyObject* ok_string = PyString_FromString("OK");
    int cmp_result = PyObject_RichCompareBool(gearman_status, ok_string, Py_EQ);
    if (cmp_result == -1){
        *result = NULL;
        return 0;
    } else if (cmp_result == 1){
        *result = PyList_GetSlice(split_result, 1, PyList_Size(split_result));
        return 1;
    } else {
        *result = NULL;
        return 0;
    }
}

/*
 *  Verifies that the gearman server's response was prepended with "OK".
 *  Returns 0 on failure, nonzero on success.
 */
static int _pygear_admin_response_ok(PyObject* response_str){
    if (response_str == NULL || response_str == Py_None){
        return 0;
    }
    char* resp = PyString_AsString(response_str);
    if (resp == NULL){
        return 0;
    }
    return !strncmp(resp, "OK", 2);
}

/*
 * Verifies that parsed_result is a list of size one.
 * If this is not the case, raise an appropriate exception
 * Returns 0 on failure, 1 on success
 */
static int _pygear_admin_check_list_and_raise(PyObject* raw_result, PyObject* parsed_result){
    if (!PyList_Check(parsed_result)){
        PyTypeObject* ret_type = (PyTypeObject *) PyObject_Type(parsed_result);
        if (!ret_type){
            PyErr_SetString(PyGearExn_ERROR, "An error was encountered while attempting to throw an exception.");
            return 0;
        }
        PyErr_SetObject(PyGearExn_ERROR, PyString_FromFormat("Unexpected internal result: Expected list, got %s", ret_type->tp_name));
        return 0;
    }

    if (PyList_Size(parsed_result) != 1){
        char* result_line = PyString_AsString(raw_result);
        PyErr_SetObject(PyGearExn_ERROR, PyString_FromFormat("Unexpected number of values in response (%s)", result_line));
        return 0;
    }
    return 1;
}

/*
 * Called when _pygear_extract_response fails.
 * Sets the appropriate Python excecption, nothing more.
 */
void _pygear_admin_raise_exception(PyObject* raw_result){
    char* raw_result_string;
    if (raw_result == NULL){
        // There was a socket exception
        PyErr_SetString(PyGearExn_COULD_NOT_CONNECT, "Connection Failed: socket error communicating with the host");
        return;
    }
    if (raw_result == Py_None){
        raw_result_string = "no data received";
    } else {
        raw_result_string = PyString_AsString(raw_result);
    }
    if (raw_result_string != NULL){
        PyErr_SetObject(PyGearExn_ERROR, PyString_FromFormat("Failed to parse server response (%s)", raw_result_string));
    } else {
        PyErr_SetString(PyGearExn_ERROR, "An error was encountered while attempting to throw an exception.");
    }
}


static PyObject* pygear_admin_set_server(pygear_AdminObject* self, PyObject* args){
    char* host;
    if (!PyArg_ParseTuple(args, "si", &host, &self->port)){
        return NULL;
    }
    if (self->host){
        free(self->host);
    }
    self->host = strdup(host);
    Py_XDECREF(self->conn);
    self->conn = NULL;
    Py_RETURN_NONE;
}

static PyObject* pygear_admin_clone(pygear_AdminObject* self){
    PyObject *argList = Py_BuildValue("(O, O)", Py_None, Py_None);
    pygear_AdminObject* python_admin = (pygear_AdminObject*) PyObject_CallObject((PyObject *) &pygear_AdminType, argList);
    python_admin->host = strdup(self->host);
    python_admin->port = self->port;
    python_admin->timeout = self->timeout;
    return Py_BuildValue("O", python_admin);
}

static PyObject* _pygear_admin_make_call(pygear_AdminObject* self, char* command){
    if (!_pygear_admin_check_connection(self)){
        return NULL;
    }
    PyObject *argList = Py_BuildValue("(i)", SOCKET_BUFSIZE);
    PyObject* buf = PyObject_CallObject((PyObject *) &PyByteArray_Type, argList);
    if (!buf){
        return NULL;
    }

    PyObject_CallMethod(self->conn, "send", "s", command);

    char* result = malloc(sizeof(char) * SOCKET_BUFSIZE);
    size_t result_bytes = 0;
    long bytes_read = 0;
    do {
        PyObject* pybytes_read = PyObject_CallMethod(self->conn, "recv_into", "Oi", buf, SOCKET_BUFSIZE);
        if (!pybytes_read){
            return NULL;
        }
        bytes_read = PyInt_AsLong(pybytes_read);
        if (bytes_read == -1){
            return NULL;
        }
        result = realloc(result, sizeof(char) * (result_bytes + bytes_read));
        strncpy(&(result[result_bytes]), PyByteArray_AsString(buf), bytes_read);
        result_bytes += bytes_read;

    } while (bytes_read == SOCKET_BUFSIZE);

    // Check if anything went wrong
    if (PyErr_Occurred()){
        // Check if the problem was a socket error.
        if (PyErr_ExceptionMatches(self->socket_error)){
            Py_XDECREF(self->conn);
            self->conn = NULL;
        }
        return NULL;
    }

    return Py_BuildValue("s#", result, result_bytes);
}

static PyObject* pygear_admin_status(pygear_AdminObject* self){
    PyObject* raw_result = _pygear_admin_make_call(self, "status\r\n");
    if (!raw_result) { return NULL; }
    PyObject* status_string = PyObject_CallMethod(raw_result, "replace", "ss", ".\n", " ");
    if (!status_string) { return NULL; }
    PyObject* status_string_trip = PyObject_CallMethod(status_string, "strip", "");
    if (!status_string_trip) { return NULL; }
    PyObject* status_list = PyObject_CallMethod(status_string_trip, "split", "s", "\n");
    if (!status_list) { return NULL; }
    PyObject* status_tuple_list = PyList_New(0);
    if (!status_tuple_list) { return NULL; }
    int status_i;
    for (status_i=0; status_i <  PyList_Size(status_list); status_i++){
        PyObject* status_line = PyList_GetItem(status_list, status_i);
        if (!status_line) { return NULL; }
        PyObject* status_line_list = PyObject_CallMethod(status_line, "split", "s", "\t");
        if (!status_line_list) { return NULL; }

        // If the server status is empty, we will get a line with only one entry.
        // It should be skipped.
        if (PyList_Size(status_line_list) < 4){
            continue;
        }

        PyObject* statfield_0 = PyList_GetItem(status_line_list, 0);
        if (!statfield_0) { return NULL; }
        PyObject* statfield_1 = PyList_GetItem(status_line_list, 1);
        if (!statfield_1) { return NULL; }
        PyObject* statfield_2 = PyList_GetItem(status_line_list, 2);
        if (!statfield_2) { return NULL; }
        PyObject* statfield_3 = PyList_GetItem(status_line_list, 3);
        if (!statfield_3) { return NULL; }

        PyObject* status_tuple = PyTuple_Pack(4,
                                              statfield_0,
                                              statfield_1,
                                              statfield_2,
                                              statfield_3);
        if (!status_tuple) { return NULL; }
        Py_INCREF(statfield_0);
        Py_INCREF(statfield_1);
        Py_INCREF(statfield_2);
        Py_INCREF(statfield_3);
        PyList_Append(status_tuple_list, status_tuple);
    }
    return status_tuple_list;
}

static PyObject* pygear_admin_workers(pygear_AdminObject* self){
    PyObject* raw_result = _pygear_admin_make_call(self, "workers\r\n");
    if (raw_result == NULL) { return NULL; }
    PyObject* worker_string = PyObject_CallMethod(raw_result, "replace", "ss", ".\n", " ");
    if (worker_string == NULL) { return NULL; }
    PyObject* worker_string_trip = PyObject_CallMethod(worker_string, "strip", "");
    if (worker_string_trip == NULL) { return NULL; }
    PyObject* worker_list = PyObject_CallMethod(worker_string_trip, "split", "s", "\n");
    return worker_list;
}

static PyObject* pygear_admin_version(pygear_AdminObject* self){
    PyObject* raw_result = _pygear_admin_make_call(self, "version\r\n");
    PyObject* parsed_result;
    if (!_pygear_extract_response(raw_result, &parsed_result)){
        _pygear_admin_raise_exception(raw_result);
        return NULL;
    }

    if (!_pygear_admin_check_list_and_raise(raw_result, parsed_result)){
        return NULL;
    }

    return PyList_GetItem(parsed_result, 0);
}

static PyObject* pygear_admin_maxqueue(pygear_AdminObject* self, PyObject* args){
    char command_buffer[128];
    int queuesize;
    if (!PyArg_ParseTuple(args, "i", &queuesize)){
        return NULL;
    }
    sprintf(command_buffer, "maxqueue %d\r\n", queuesize);
    PyObject* raw_result = _pygear_admin_make_call(self, command_buffer);
    if (!_pygear_admin_response_ok(raw_result)){
        _pygear_admin_raise_exception(raw_result);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_admin_shutdown(pygear_AdminObject* self, PyObject* args){
    int graceful;
    if (!PyArg_ParseTuple(args, "i", &graceful)){
        return NULL;
    }
    PyObject* raw_result;
    if (graceful){
         raw_result = _pygear_admin_make_call(self, "shutdown\r\n");
    } else {
         raw_result = _pygear_admin_make_call(self, "shutdown graceful\r\n");
    }
    if (!_pygear_admin_response_ok(raw_result)){
        _pygear_admin_raise_exception(raw_result);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_admin_verbose(pygear_AdminObject* self){
    PyObject* raw_result = _pygear_admin_make_call(self, "verbose\r\n");
    PyObject* parsed_result;
    if (!_pygear_extract_response(raw_result, &parsed_result)){
        _pygear_admin_raise_exception(raw_result);
    }

    if (!_pygear_admin_check_list_and_raise(raw_result, parsed_result)){
        return NULL;
    }

    return PyList_GetItem(parsed_result, 0);
}

static PyObject* pygear_admin_getpid(pygear_AdminObject* self){
    PyObject* raw_result = _pygear_admin_make_call(self, "getpid\r\n");
    PyObject* parsed_result;
    if (!_pygear_extract_response(raw_result, &parsed_result)){
        _pygear_admin_raise_exception(raw_result);
        return NULL;
    }

    if (!_pygear_admin_check_list_and_raise(raw_result, parsed_result)){
        return NULL;
    }

    PyObject* pid_str = PyList_GetItem(parsed_result, 0);
    return PyNumber_Int(pid_str);
}

static PyObject* pygear_admin_drop_function(pygear_AdminObject* self, PyObject* args){
    char* fn_name;
    int fn_len;
    if (!PyArg_ParseTuple(args, "s#", &fn_name, &fn_len)){
        return NULL;
    }
    char* format_string = "drop function  %s\r\n";
    char* command_buffer = malloc(sizeof(char) * (strlen(format_string) + fn_len));
    sprintf(command_buffer, format_string, fn_name);
    PyObject* raw_result = _pygear_admin_make_call(self, command_buffer);
    if (!_pygear_admin_response_ok(raw_result)){
        _pygear_admin_raise_exception(raw_result);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* pygear_admin_create_function(pygear_AdminObject* self, PyObject* args){
    char* fn_name;
    int fn_len;
    if (!PyArg_ParseTuple(args, "s#", &fn_name, &fn_len)){
        return NULL;
    }
    char* format_string = "create function  %s\r\n";
    char* command_buffer = malloc(sizeof(char) * (strlen(format_string) + fn_len));
    sprintf(command_buffer, format_string, fn_name);
    PyObject* raw_result = _pygear_admin_make_call(self, command_buffer);
    if (!_pygear_admin_response_ok(raw_result)){
        _pygear_admin_raise_exception(raw_result);
    }
    Py_RETURN_NONE;
}
