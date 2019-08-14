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
* redisdbi_py2.h
*
*   librdbapi wrapper for python2.7
*
* 2019-08
*/
#ifndef REDISDBI_PY2_H_INCLUDED
#define REDISDBI_PY2_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#ifdef DLL_EXPORT
# define REDISDBI_API extern __declspec(dllexport)
#else
# define REDISDBI_API extern
#endif

#include <rdbapi.h>

REDISDBI_API void initredisdbi();


static void rdbconn_free (void * _ctx, void * _env)
{
    if (_ctx) {
        RDBCtxFree((RDBCtx)_ctx);
    }

    if (_env) {
        RDBEnvDestroy((RDBEnv)_env);
    }
}


static RDBCtx connect (const char *cluster, int ctxtimeout, int sotimeo_ms)
{
    RDBEnv env;
    RDBAPI_RESULT res;

    res = RDBEnvCreate(cluster, ctxtimeout, sotimeo_ms, &env);
    if (res == RDBAPI_SUCCESS) {
        RDBCtx ctx;

        res = RDBCtxCreate(env, &ctx);
        if (res == RDBAPI_SUCCESS) {
            return ctx;
        }

        RDBEnvDestroy(env);
    }

    return NULL;
}


#ifdef __cplusplus
}
#endif

#endif /* REDISDBI_PY2_H_INCLUDED */
