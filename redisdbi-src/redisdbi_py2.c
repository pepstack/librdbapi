/***********************************************************************
* Copyright (c) 2008-2080 pepstack.com, 350137278@qq.com
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
 * redisdbipy.c
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */

/**
 * https://blog.csdn.net/wuchuanpingstone/article/details/77763455
 * https://www.cnblogs.com/chengxuyuancc/p/6374239.html
 * https://blog.csdn.net/yueguanghaidao/article/details/11708417
 *
 * 1) Build C module for python
 *
 * fix <pyconfig.h> (same location with Python.h):
 *     'pragma comment(lib,"python27_d.lib")'
 *	replaced by:
 *     'pragma comment(lib,"python27.lib")'
 * and:
 *   #undef Py_DEBUG
 *
 * 2) Call C module from python
 * 
 *   After build, open cmd
 *
 *   > chcp 1252
 *   > cp redisdbi.dll /path/to/redisdbi.pyd
 *
 *-------------------------------------
 * /path/to/test.py:
 *-------------------------------------
 *   import os
 *   
 *   dllpath=".\\"
 *   if dllpath not in os.sys.path:
 *       os.sys.path.append(dllpath)
 *   #print os.sys.path
 *
 *   import redisdbi
 *
 *   r = redisdbi.fab(100)
 *   print r
 *
 *   print redisdbi.reverse("hello")
 *-------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <Python.h>

#include "redisdbi_py2.h"

#define APPNAME  "redisdbi"
#define APPVER   "0.0.1"


#ifdef __WINDOWS__

# pragma comment(lib, "librdbapi-w32lib.lib")
# pragma comment(lib, "hiredis.lib")
# pragma comment(lib, "Win32_Interop.lib")

#else

/* Linux: link librdbapi.lib */

#endif

/**
 * PyErr_SetString(RedisdbiError, "your error message!");
 * PyErr_SetString(RedisdbiIOError, "your error message!");
 *
 */
static PyObject *RedisdbiError;
static PyObject *RedisdbiIOError;

static PyObject* Foo_init(PyObject *self, PyObject *args)
{
    printf("Foo.__init__ called\n");
    Py_INCREF(Py_None);
    return Py_None;
}
static PyObject* Foo_doSomething(PyObject *self, PyObject *args)
{
    printf("Foo.doSomething called\n");
    Py_INCREF(Py_None);
    return Py_None;
}
static PyMethodDef FooMethods[] =
{
    {"__init__", Foo_init, METH_VARARGS, "doc string"},
    {"doSomething", Foo_doSomething, METH_VARARGS, "doc string"},
    {0, 0},
};
static PyMethodDef ModuleMethods[] = { {0, 0} };


/*********************************************************************
 *                redisdbi methods for python                        *
 *                                                                   *
 * https://blog.csdn.net/solid_sdu/article/details/79401263          *
 *********************************************************************/
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
        PyErr_SetString(RedisdbiError, "failed parse tuple");
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
        PyErr_SetString(RedisdbiError, "failed connect to redis");
        return NULL;
    }

    rdb = PyCObject_FromVoidPtrAndDesc(ctx, RDBCtxGetEnv(ctx), rdbconn_free);

    /*
    PyObject *mrdb = Py_InitModule("redisdbi.rdbmodule", NULL);

    // Create a capsule containing the ctx object
    PyObject * capobj = PyCapsule_New((void *) ctx, "rdb", NULL);

    // and add it to the module
    PyModule_AddObject(mrdb, "rdb", capobj);
    */

    PyObject *module = PyModule_New("rdbmodule");

    PyObject *moduleDict = PyModule_GetDict(module);
    PyObject *classDict = PyDict_New();
    PyObject *className = PyString_FromString("Foo");
    PyObject *fooClass = PyClass_New(NULL, classDict, className);

    PyDict_SetItemString(moduleDict, "Foo", fooClass);

    Py_DECREF(classDict);
    Py_DECREF(className);
    Py_DECREF(fooClass);

    /* add methods to class */
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
    return fooClass;
}


static PyMethodDef redisdbiMethods[] = {
    {"connect", py_connect,   METH_VARARGS, "connect redis cluster"},
    {"execute", py_execute,   METH_VARARGS, "execute a redis command"}

    ,{NULL, NULL, 0, NULL}
}; 


void initredisdbi ()
{
    PyObject *m;

    PyObject *mversion = PyString_FromString(APPVER);
    PyObject *mauthor = PyString_FromString("350137278@qq.com");

    m = Py_InitModule("redisdbi", redisdbiMethods);
    if (m == NULL) {
        return;
    }

    RedisdbiError = PyErr_NewException("redisdbi.error", NULL, NULL);
    RedisdbiIOError = PyErr_NewException("redisdbi.IOError", NULL, NULL);

    Py_INCREF(RedisdbiError);
    Py_INCREF(RedisdbiIOError);

    PyModule_AddObject(m, "error", RedisdbiError);
    PyModule_AddObject(m, "ioerror", RedisdbiIOError);
    PyModule_AddObject(m, "version", mversion);
    PyModule_AddObject(m, "author", mauthor);

    /**
     * https://www.tutorialspoint.com/How-to-attach-a-C-method-to-existing-Python-class
     * https://stackoverflow.com/questions/37281669/how-to-define-a-new-class-with-methods-via-the-c-api-with-python-3
     */
    PyObject *moduleDict = PyModule_GetDict(m);
    PyObject *classDict = PyDict_New();
    PyObject *className = PyString_FromString("Foo");
    PyObject *fooClass = PyClass_New(NULL, classDict, className);

    PyDict_SetItemString(moduleDict, "Foo", fooClass);

    Py_DECREF(classDict);
    Py_DECREF(className);
    Py_DECREF(fooClass);

    /* add methods to class */
    PyMethodDef *def;

    for (def = FooMethods; def->ml_name != NULL; def++) {
        PyObject *func = PyCFunction_New(def, NULL);
        PyObject *method = PyMethod_New(func, NULL, fooClass);

        PyDict_SetItemString(classDict, def->ml_name, method);

        Py_DECREF(func);
        Py_DECREF(method);
    }
}
