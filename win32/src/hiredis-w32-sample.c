// hiredis-w32-sample.c
//   hiredis client for windows x64
//
//   https://blog.csdn.net/xumaojun/article/details/51558128
//
///////////////////////////////////////////////////////////////////////

#if (defined(WIN32) || defined(_WIN16) || defined(_WIN32) || defined(_WIN64)) && !defined(__WINDOWS__)
# define __WINDOWS__
#endif

#ifdef __WINDOWS__
// memory leak auto-detect
// https://blog.csdn.net/lyc201219/article/details/62219503
# define _CRTDBG_MAP_ALLOC
# include <stdlib.h>
# include <crtdbg.h>

# pragma comment(lib, "librdbapi-w32.lib")
# pragma comment(lib, "hiredis.lib")
# pragma comment(lib, "Win32_Interop.lib")
#endif


#include <assert.h>

#include "../../rdbapi.h"


int main(int argc, const char *argv[])
{
#ifdef __WINDOWS__
    int dbgFlag = _CrtSetDbgFlag(_CRTDBG_REPORT_FLAG);
    dbgFlag |= _CRTDBG_LEAK_CHECK_DF;
    _CrtSetDbgFlag(dbgFlag);
#endif

    RDBAPI_RESULT res;

    RDBThreadCtx thrctx;

    RDBEnv env;
    RDBCtx ctx;

    redisReply *reply = NULL;

    char buf[RDBAPI_PROP_MAXSIZE];
    char ts[24];

    ub8 startTime = RDBCurrentTime(1, ts);

    printf("start on: %s\n", ts);

    thrctx = RDBThreadCtxCreate(0, 0, 0);
    assert(thrctx);

    res = RDBEnvCreate(0, 9, &env);
    assert(res == RDBAPI_SUCCESS);

    res = RDBEnvInitAllNodes(env, "test@192.168.39.111:7001-7009", -1, 0, 12000);
    assert(res == RDBAPI_SUCCESS);

    res = RDBCtxCreate(env, &ctx);
    assert(res == RDBAPI_SUCCESS);

    RDBThreadCtxAttach(thrctx, ctx);

    RDBCtxNode ctxnode = RDBCtxGetActiveNode(ctx, NULL, -1);
    assert(ctxnode);

    printf("redis open success.\n");

    RDBCtxCheckInfo(ctx, MAX_NODEINFO_SECTIONS);

    RDBCtxPrintInfo(ctx, -1);

    res = RDBCtxNodeInfoProp(ctxnode, NODEINFO_CLUSTER, "cluster_enabled", buf);
    if (res > 0) {
        printf("cluster_enabled:%s\n", buf);
    }

    RDBFieldDesc fielddefs[] = {
        {
            {"sid"}
            ,RDBVTYPE_UB8
            ,0
            ,0
            ,1
            ,0
            ,{""}
        },
        {
            {"uid"}
            ,RDBVTYPE_UB8
            ,0
            ,0
            ,2
            ,0
            ,{""}
        },
        {
            {"entrykey"}
            ,RDBVTYPE_STR
            ,32
            ,0
            ,3
            ,0
            ,{""}
        },
        {
            {"status"}
            ,RDBVTYPE_UB4
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"addtime"}
            ,RDBVTYPE_UB4
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"speed_low"}
            ,RDBVTYPE_UB4
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"speed_high"}
            ,RDBVTYPE_UB4
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"updtime"}
            ,RDBVTYPE_UB8
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"filetime"}
            ,RDBVTYPE_UB8
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"entryid"}
            ,RDBVTYPE_UB8X
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"filesize"}
            ,RDBVTYPE_UB8
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"cretime"}
            ,RDBVTYPE_UB8
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"position"}
            ,RDBVTYPE_UB8
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"pathid"}
            ,RDBVTYPE_STR
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"logmd5"}
            ,RDBVTYPE_STR
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"logfile"}
            ,RDBVTYPE_STR
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"logstash"}
            ,RDBVTYPE_STR
            ,0
            ,0
            ,0
            ,1
            ,{""}
        },
        {
            {"filemd5"}
            ,RDBVTYPE_STR
            ,0
            ,0
            ,0
            ,1
            ,{"md5sum for file"}
        },
        {
            {"route"}
            ,RDBVTYPE_STR
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

/*
    do {
        redisReply *replys[9];

        int i;
        int num = RedisExecCommandOnAllNodes(ctx, "keys *", replys, NULL);
        assert(num == 9);

        for (i = 0; i < 9; i++) {
            RedisReplyObjectPrint(replys[i], RDBCtxGetNode(ctx, i));
        }

        RedisFreeReplyObjects(replys, 9);
    } while(0);
*/

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
    ub8 totalRows = 0;

    const char * fields[] = {
        "cretime",
        "cretime",
        "status"
    };

    RDBSqlExpr fieldexprs[] = {
        RDBSQLEX_LESS_THAN,
        RDBSQLEX_GREAT_THAN,
        RDBSQLEX_EQUAL
    };

    const char * fieldvals[] = {
        "1564543300",
        "1564543181",
        "0"
    };

    ub8 offset = 0;
    ub8 total = 0;

    res = RDBTableScanFirst(ctx, "xsdb", "logentry", 0, 0, 0, 0, 0, fields, fieldexprs, fieldvals, 0, 0, &resultMap);
    if (res == RDBAPI_SUCCESS) {
        while ((offset = RDBTableScanNext(resultMap, offset, 20)) != RDB_ERROR_OFFSET) {
            RDBResultMapPrintOut(resultMap, 1);

            total += RDBResultMapSize(resultMap);

            RDBResultMapClean(resultMap);
        }

        printf("total rows=%"PRIu64".\n", total);

        RDBResultMapFree(resultMap);
    }

    RDBThreadCtxFree(thrctx);

    printf("redis close success.\n");

    ub8 endTime = RDBCurrentTime(1, ts);

    printf("end on: %s. elapsed: %.3lf seconds.\n", ts, (endTime - startTime) * 0.001);
 	return 0;
}
