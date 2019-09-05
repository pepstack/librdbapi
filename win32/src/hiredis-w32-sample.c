// hiredis-w32-sample.c
//   hiredis client for windows x64
//
//   https://blog.csdn.net/xumaojun/article/details/51558128
//
///////////////////////////////////////////////////////////////////////

#if (defined(WIN32) || defined(_WIN16) || defined(_WIN32) || defined(_WIN32_WINNT) || defined(_WIN64)) && !defined(__WINDOWS__)
    #ifdef _WIN32_WINNT
        #if _WIN32_WINNT  < 0x0500
            #error  Windows version is too lower than 0x0500
        #endif
    #endif

    # define __WINDOWS__
#endif

#ifdef __WINDOWS__

# ifdef _DEBUG
/** memory leak auto-detect in MSVC
 * https://blog.csdn.net/lyc201219/article/details/62219503
 */
#   define _CRTDBG_MAP_ALLOC
#   include <stdlib.h>
#   include <malloc.h>
#   include <crtdbg.h>
# else
#   include <stdlib.h>
#   include <malloc.h>
# endif

# pragma comment(lib, "librdbapi-w32.lib")
# pragma comment(lib, "hiredis.lib")
# pragma comment(lib, "Win32_Interop.lib")
#endif


#include <assert.h>

#include "../../rdbapi.h"


int main(int argc, const char *argv[])
{
#ifdef __WINDOWS__

# ifdef _DEBUG
    int dbgFlag = _CrtSetDbgFlag(_CRTDBG_REPORT_FLAG);
    dbgFlag |= _CRTDBG_LEAK_CHECK_DF;
    _CrtSetDbgFlag(dbgFlag);
#endif

#endif

    RDBAPI_RESULT res;

    RDBThreadCtx thrctx;

    RDBEnv env;
    RDBCtx ctx;

    redisReply *reply = NULL;

    char ts[24];

    ub8 startTime = RDBCurrentTime(1, ts);

    printf("start on: %s\n", ts);

    thrctx = RDBThreadCtxCreate(0, 0, 0);
    assert(thrctx);

    res = RDBEnvCreate("test@192.168.39.111:7001-7009", 0, 0, &env);
    assert(res == RDBAPI_SUCCESS);

    res = RDBCtxCreate(env, &ctx);
    assert(res == RDBAPI_SUCCESS);

    RDBThreadCtxAttach(thrctx, ctx);

    RDBCtxNode ctxnode = RDBCtxGetActiveNode(ctx, NULL, -1);
    assert(ctxnode);

    printf("redis open success.\n");

    RDBCtxCheckInfo(ctx, MAX_NODEINFO_SECTIONS, 0);

    RDBCtxPrintInfo(ctx, -1);

    RDBThreadCtxDestroy(thrctx);

    printf("redis close success.\n");

    ub8 endTime = RDBCurrentTime(1, ts);

    printf("end on: %s. elapsed: %.3lf seconds.\n", ts, (endTime - startTime) * 0.001);
 	return 0;
}
