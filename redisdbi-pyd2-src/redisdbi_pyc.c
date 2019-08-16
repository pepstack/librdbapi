/***********************************************************************
* Copyright (c) 2008-2080 syna-tech.com, pepstack.com, 350137278@qq.com
*
* ALL RIGHTS RESERVED.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions
* are met:
*
*   Redistributions of source code must retain the above copyright
*    notice, this list of conditions and the following disclaimer.
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
* "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
* LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
* A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
* OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
* SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
* LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
* DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
* THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
* OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
***********************************************************************/

/**
* redisdbi_pyc.c
*
*   librdbapi for python2.x
*
* 2019-08
*/
#include "redisdbi_pyc.h"

#ifdef __WINDOWS__

# pragma comment(lib, "librdbapi-w32lib.lib")
# pragma comment(lib, "hiredis.lib")
# pragma comment(lib, "Win32_Interop.lib")

#else

/* Linux: link librdbapi.lib */

#endif


/**
 * Error Objects
 */

extern PyObject *RDBIError;

extern PyObject *RedisApiError;


void RDBICon_dealloc (RDBICon *self)
{
}


PyObject *RDBICon_new (PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return NULL;
}


int RDBICon_init (RDBICon *self, PyObject *args, PyObject *kwds)
{
    return 0;
}


/**
 * PyCFunction of module
 */
PyObject* pycGetApiVersion (PyObject* self, PyObject* args)
{
    return (PyObject *)Py_BuildValue("s", RDBAPI_VERSION);
}




//////////////////////////////// DELETE ///////////////////////////

PyObject * cursor (PyObject *obj, PyObject *args)
{
    printf("Hello world!\n");
    return Py_None;
}


PyObject* py_execute (PyObject* self, PyObject* args)
{
    PyObject* rdb;
    char *sql = 0;

    if (! PyArg_ParseTuple(args, "Os", &rdb, &sql)) {
        PyErr_SetString(RDBIError, "failed parse tuple");
        return NULL;
    }

    RDBCtx ctx = PyCObject_AsVoidPtr(rdb);

    printf("sql=%s\n", sql);

    PyObject* ret = Py_BuildValue("s", "success");
    return ret;
}


PyObject* py_connect (PyObject* self, PyObject* args)
{
    char *cluster = 0;
    int ctxtimeout = 0;
    int sotimeo_ms = RDB_CLUSTER_SOTIMEO_MS;

    PyObject* rdb = NULL;

    RDBCtx ctx;

    if (! PyArg_ParseTuple(args, "sii", &cluster, &ctxtimeout, &sotimeo_ms)) {
        return NULL;
    }

    ctx = connect(cluster, ctxtimeout, sotimeo_ms);
    if (! ctx) {
        PyErr_SetString(RedisApiError, "failed connect to redis");
        return NULL;
    }

    rdb = PyCObject_FromVoidPtrAndDesc(ctx, RDBCtxGetEnv(ctx), rdbconn_free);

    /*
    PyObject *mrdb = Py_InitModule("redisdbi.rdbmodule", NULL);

    // Create a capsule containing the ctx object
    PyObject * capobj = PyCapsule_New((void *) ctx, "rdb", NULL);

    // and add it to the module
    PyModule_AddObject(mrdb, "rdb", capobj);
  

    PyObject *module = PyModule_New("rdbmodule");

    PyObject *moduleDict = PyModule_GetDict(module);
    PyObject *classDict = PyDict_New();
    PyObject *className = PyString_FromString("Foo");
    PyObject *fooClass = PyClass_New(NULL, classDict, className);

    PyDict_SetItemString(moduleDict, "Foo", fooClass);

    Py_DECREF(classDict);
    Py_DECREF(className);
    Py_DECREF(fooClass);

    // add methods to class
    PyMethodDef *def;

    for (def = FooMethods; def->ml_name != NULL; def++) {
        PyObject *func = PyCFunction_New(def, NULL);
        PyObject *method = PyMethod_New(func, NULL, fooClass);

        PyDict_SetItemString(classDict, def->ml_name, method);

        Py_DECREF(func);
        Py_DECREF(method);
    }

    PyErr_Clear();

    Py_INCREF(fooClass);
  */

    return NULL;
}