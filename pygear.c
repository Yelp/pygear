#include "pygear.h"

PyMODINIT_FUNC initpygear(void){
    PyObject* m;

    pygear_ClientType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&pygear_ClientType) < 0){
        fprintf(stderr, "PyType unready!\n");
        return;
    }

    // Initialize pygear module
    m = Py_InitModule3("pygear", client_module_methods, client_module_docstring);

    // Add Client class
    Py_INCREF(&pygear_ClientType);
    PyModule_AddObject(m, "Client", (PyObject *)&pygear_ClientType);
}
