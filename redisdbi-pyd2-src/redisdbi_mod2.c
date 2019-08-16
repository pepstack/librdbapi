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
 * redisdbi_mod2.c
 *
 *  redisdbi python2.x module for librdbapi
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */

/**
 * refer:
 *  https://blog.csdn.net/wuchuanpingstone/article/details/77763455
 *  https://www.cnblogs.com/chengxuyuancc/p/6374239.html
 *  https://blog.csdn.net/yueguanghaidao/article/details/11708417
 *  https://github.com/mysql/mysql-connector-python
 *
 *  https://www.tutorialspoint.com/How-to-attach-a-C-method-to-existing-Python-class
 *  https://stackoverflow.com/questions/37281669/how-to-define-a-new-class-with-methods-via-the-c-api-with-python-3
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
 *   After build, open cmd (set PYTHONIOENCODING=utf-8)
 *
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

#include "redisdbi_pyc.h"

#define PYD_MODULE_DEF(ob, name, methods, doc)  \
        ob = Py_InitModule3(name, methods, doc)

#define PYD_MODULE_SOK(val)
#define PYD_MODULE_ERR


/**
 * Raises RDBIError when an error is retured by this module.
 *   PyErr_SetString(RDBIError, "your error message!");
 */
PyObject *RDBIError;

/**
 * Raises RedisAPIError when an error is retured by redis server.
 *   PyErr_SetString(RedisApiError, "your error message!");
 */
PyObject *RedisApiError;


/**
 * class RDBICon
 */

static PyMemberDef RDBICon_members[]=
{
    {"have_result_set", T_OBJECT, offsetof(RDBICon, have_result_set), 0, "True if current session has result set"}
    ,{NULL}  /* Sentinel */
};


static PyMethodDef RDBICon_methods[]=
{
    {"connect", NULL, METH_VARARGS, "connect redis cluster"}
    ,{NULL}  /* Sentinel */
};


PyTypeObject RDBIConType =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "redisdbi.connect",                 /* tp_name */
    sizeof(RDBICon),                    /* tp_basicsize */
    0,                                  /* tp_itemsize */
    (destructor)RDBICon_dealloc,        /* tp_dealloc */
    0,                                  /* tp_print */
    0,                                  /* tp_getattr */
    0,                                  /* tp_setattr */
    0,                                  /* tp_compare */
    0,                                  /* tp_repr */
    0,                                  /* tp_as_number */
    0,                                  /* tp_as_sequence */
    0,                                  /* tp_as_mapping */
    0,                                  /* tp_hash */
    0,                                  /* tp_call */
    0,                                  /* tp_str */
    0,                                  /* tp_getattro */
    0,                                  /* tp_setattro */
    0,                                  /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                 /* tp_flags */
    "RDBICon objects",                  /* tp_doc */
    0,                                  /* tp_traverse */
    0,                                  /* tp_clear */
    0,                                  /* tp_richcompare */
    0,                                  /* tp_weaklistoffset */
    0,                                  /* tp_iter */
    0,                                  /* tp_iternext */
    RDBICon_methods,                    /* tp_methods */
    RDBICon_members,                    /* tp_members */
    0,                                  /* tp_getset */
    0,                                  /* tp_base */
    0,                                  /* tp_dict */
    0,                                  /* tp_descr_get */
    0,                                  /* tp_descr_set */
    0,                                  /* tp_dictoffset */
    (initproc)RDBICon_init,             /* tp_init */
    0,                                  /* tp_alloc */
    RDBICon_new,                        /* tp_new */
};


/*********************************************************************
 *                redisdbi methods for python                        *
 *                                                                   *
 * https://blog.csdn.net/solid_sdu/article/details/79401263          *
 *********************************************************************/
static PyMethodDef module_methods[] = {
    {"getApiVersion", (PyCFunction) pycGetApiVersion, METH_NOARGS, "Get version of librdbapi module"}
    ,{NULL}
};


REDISDBI_PYDAPI void initredisdbi()
{
    PyObject *mod = NULL;

    PYD_MODULE_DEF(mod, REDISDBI_PYDNAME, module_methods, "Python C Extension for librdbapi/C");
    if (! mod) {
        return;
    }

/*TODO
    if (PyType_Ready(&RDBIConType) < 0) {
        return PYD_MODULE_ERR;
    }

    if (PyType_Ready(&RDBIExecuteType) < 0) {
        return PYD_MODULE_ERR;
    }
*/

    RDBIError = PyErr_NewException("redisdbi.RDBIError", PyExc_Exception, NULL);
    Py_INCREF(RDBIError);
    PyModule_AddObject(mod, "RDBIError", RDBIError);

    RedisApiError = PyErr_NewException("redisdbi.RedisApiError", PyExc_Exception, NULL);
    Py_INCREF(RedisApiError);
    PyModule_AddObject(mod, "RedisApiError", RedisApiError);

    PyObject *_version = PyString_FromString(REDISDBI_PYDVER);
    PyObject *_author = PyString_FromString(REDISDBI_AUTHOR);

    PyModule_AddObject(mod, "version", _version);
    PyModule_AddObject(mod, "author", _author);

    return PYD_MODULE_SOK(mod);
}
