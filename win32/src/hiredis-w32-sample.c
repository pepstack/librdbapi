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

    RDBCtxCheckInfo(ctx, MAX_NODEINFO_SECTIONS);

    RDBCtxPrintInfo(ctx, -1);

    RDBFieldDes_t fielddefs[] = {
        {
            {"sid"}
            ,3
            ,RDBVT_UB8
            ,0
            ,0
            ,1
            ,0
            ,{""}
        },
        {
            {"uid"}
            ,3
            ,RDBVT_UB8
            ,0
            ,0
            ,2
            ,0
            ,{""}
        },
        {
            {"entrykey"}
            ,8
            ,RDBVT_STR
            ,32
            ,0
            ,3
            ,0
            ,{""}
        },
        {
            {"status"}
            ,6
            ,RDBVT_UB4
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"addtime"}
            ,7
            ,RDBVT_UB4
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"speed_low"}
            ,9
            ,RDBVT_UB4
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"speed_high"}
            ,10
            ,RDBVT_UB4
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"updtime"}
            ,7
            ,RDBVT_UB8
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"filetime"}
            ,8
            ,RDBVT_UB8
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"entryid"}
            ,7
            ,RDBVT_UB8X
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"filesize"}
            ,8
            ,RDBVT_UB8
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"cretime"}
            ,7
            ,RDBVT_UB8
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"position"}
            ,8
            ,RDBVT_UB8
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"pathid"}
            ,6
            ,RDBVT_STR
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"logmd5"}
            ,6
            ,RDBVT_STR
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"logfile"}
            ,7
            ,RDBVT_STR
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"logstash"}
            ,8
            ,RDBVT_STR
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"filemd5"}
            ,7
            ,RDBVT_STR
            ,0
            ,0
            ,0
            ,1
            ,{"md5sum for file"}
        },
        {
            {"route"}
            ,5
            ,RDBVT_STR
            ,0
            ,0
            ,0
            ,1
            ,{"route path"}
        }     
    };

    res = RDBTableCreate(ctx, "xsdb", "logentry", NULL, sizeof(fielddefs)/sizeof(fielddefs[0]), fielddefs);
    if (res != RDBAPI_SUCCESS) {
        printf("RDBTableCreate failed: %s\n", RDBCtxErrMsg(ctx));
        exit(-1);
    }

    double retval;
    res = RedisIncrFloatField(ctx, "zl", "age", DBL_MAX, &retval, 1);
    assert(res == RDBAPI_SUCCESS);

    res = RedisIncrFloatField(ctx, "zl", "age", 3.14159, &retval, 1);
    assert(res == RDBAPI_SUCCESS);

    sb8 retl;
    res = RedisIncrIntegerField(ctx, "test", "count", 9223372036854775807, &retl, 1);
    assert(res == RDBAPI_SUCCESS);

    res = RedisIncrIntegerField(ctx, "test", "count", 9223372036854775807, &retl, 1);
    assert(res == RDBAPI_SUCCESS);

    RedisFreeReplyObject(&reply);

    RDBResultMap resultMap;

    const char * fields[] = {
        "cretime",
        "cretime",
        "status"
    };

    RDBFilterExpr fieldexprs[] = {
        RDBFIL_LESS_THAN,
        RDBFIL_GREAT_THAN,
        RDBFIL_EQUAL
    };

    const char * fieldvals[] = {
        "1564543300",
        "1564543181",
        "0"
    };

    ub8 offset = 0;
    ub8 total = 0;

    res = RDBTableScanFirst(ctx, RDBSQL_SELECT, "xsdb", "logentry", 0, 0, 0, 0, 0, fields, fieldexprs, fieldvals, 0, 0, -1, NULL, &resultMap);
    if (res == RDBAPI_SUCCESS) {
        while ((offset = RDBTableScanNext(resultMap, offset, 20)) != RDB_ERROR_OFFSET) {
            RDBResultMapPrintOut(resultMap, 1);

            total += RDBResultMapSize(resultMap);

            RDBResultMapClean(resultMap);
        }

        printf("total rows=%"PRIu64".\n", total);

        RDBResultMapFree(resultMap);
    }

    RDBThreadCtxDestroy(thrctx);

    printf("redis close success.\n");

    ub8 endTime = RDBCurrentTime(1, ts);

    printf("end on: %s. elapsed: %.3lf seconds.\n", ts, (endTime - startTime) * 0.001);
 	return 0;
}
