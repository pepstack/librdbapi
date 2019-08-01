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
 * test.c
 *   librdbapi test c app
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-07-26
 * @update:
 */
#include "rdbapi.h"

#define TESTRDB_HOSTS    "test@127.0.0.1:7001-7009"
// "test@192.168.39.111:7001-7009"

int main (int argc, const char *argv[])
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

    char ts[24];
    char buf[RDBAPI_PROP_MAXSIZE];

    ub8 startTime = RDBCurrentTime(1, ts);

    printf("start on: %s\n", ts);

    res = RDBEnvCreate(0, 9, &env);
    assert(res == RDBAPI_SUCCESS);

    res = RDBEnvInitAllNodes(env, TESTRDB_HOSTS, -1, 0, 12000);
    assert(res == RDBAPI_SUCCESS);

    res = RDBCtxCreate(env, &ctx);
    assert(res == RDBAPI_SUCCESS);

    printf("redis open success.\n");

    RDBCtxCheckInfo(ctx, MAX_NODEINFO_SECTIONS);
    RDBCtxPrintInfo(ctx, -1);

    {
        RDBResultMap resultMap;
        ub8 totalRows = 0;

        const char * fields[] = {
            "cretime",
            "status"
        };

        RDBSqlExpr fieldexprs[] = {
            RDBSQLEX_LESS_THAN,
            RDBSQLEX_EQUAL
        };

        const char * fieldvals[] = {
            "9563939080",
            "0"
        };

        int limit = 20;
        ub8 offset = 10000;

        int numrows;

        res = RDBTableScanFirst(ctx,
                "xsdb",                             // database prefix
                "logentry",                         // table prefix
                0, 0, 0, 0,                         // filter keys
                0, fields, fieldexprs, fieldvals,   // filter fields
                0,                                  // groupby: not impl
                0,                                  // orderby: not impl
                limit,
                offset,
                &resultMap);

        if (res == RDBAPI_SUCCESS) {
            while ((numrows = RDBTableScanNext(resultMap, &offset)) > 0) {
                if (numrows) {
                    // RDBResultMapTraverse(resultMap, RedisReplyCallbackPrint, 0);
                    totalRows += numrows;

                    printf("rows=%"PRIu64", total=%"PRIu64", next offset=%"PRIu64"\n", numrows, totalRows, offset);
                }

                RDBResultMapClean(resultMap);
            }

            printf("total=%"PRIu64". scaned=%"PRIu64"\n", totalRows, RDBResultMapGetOffset(resultMap));

            RDBResultMapDestroy(resultMap);
        }        
    }

    RDBCtxFree(ctx);
    RDBEnvDestroy(env);

    printf("redis close success.\n");

    ub8 endTime = RDBCurrentTime(1, ts);

    printf("end on: %s. elapsed: %.3lf seconds.\n", ts, (endTime - startTime) * 0.001);

    return 0;
}
