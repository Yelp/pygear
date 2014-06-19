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

#define CLIENT_OPT_NON_BLOCKING "non_blocking"
#define CLIENT_OPT_UNBUFFERED_RESULT "unbuffered_result"
#define CLIENT_OPT_FREE_TASKS "free_tasks"
#define CLIENT_OPT_GENERATE_UNIQUE "generate_unique"

static PyObject* pygear_client_set_options(pygear_ClientObject* self, PyObject* args, PyObject* kwargs){
    static char *kwlist[] = {
        CLIENT_OPT_NON_BLOCKING,
        CLIENT_OPT_UNBUFFERED_RESULT,
        CLIENT_OPT_FREE_TASKS,
        CLIENT_OPT_GENERATE_UNIQUE,
        NULL
    };

    static int options_t_value[] = {
        GEARMAN_CLIENT_NON_BLOCKING,
        GEARMAN_CLIENT_UNBUFFERED_RESULT,
        GEARMAN_CLIENT_FREE_TASKS,
        GEARMAN_CLIENT_GENERATE_UNIQUE
    };

    int client_options[4];

    if (! PyArg_ParseTupleAndKeywords(args, kwargs, "|iiii", kwlist,
                                      &client_options[0],
                                      &client_options[1],
                                      &client_options[2],
                                      &client_options[3]
                                      )){
        return NULL;
    }

    int i;
    for (i=0; i < 4; i++){
        if (client_options[i]){
            gearman_client_add_options(self->g_Client, options_t_value[i]);
        } else {
            gearman_client_remove_options(self->g_Client, options_t_value[i]);
        }
    }

    Py_RETURN_NONE;
}

static PyObject* pygear_client_get_options(pygear_ClientObject* self){
    static int options_t_value[] = {
        GEARMAN_CLIENT_NON_BLOCKING,
        GEARMAN_CLIENT_UNBUFFERED_RESULT,
        GEARMAN_CLIENT_FREE_TASKS,
        GEARMAN_CLIENT_GENERATE_UNIQUE
    };

    PyObject* client_options[4];
    int i;
    for (i=0; i < 4; i++){
        client_options[i] = (gearman_client_has_option(self->g_Client, options_t_value[i]) ? Py_True : Py_False);
    }
    PyObject* option_dictionary = Py_BuildValue(
        "{s:O, s:O, s:O, s:O}",
        CLIENT_OPT_NON_BLOCKING, client_options[0],
        CLIENT_OPT_UNBUFFERED_RESULT, client_options[1],
        CLIENT_OPT_FREE_TASKS, client_options[2],
        CLIENT_OPT_GENERATE_UNIQUE, client_options[3]
    );
    return option_dictionary;
}
