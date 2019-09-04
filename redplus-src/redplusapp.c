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
 * redplusapp.c
 *   redis command tool application.
 *
 * create: 2019-08-01
 * update: 2019-08-23
 * version: 0.0.1
 */
#include "redplusapp.h"


int main(int argc, const char *argv[])
{
    WINDOWS_CRTDBG_ON

    int ch, index;

    const char *line;
    char cshbuf[1024];
    int interactive = 0;

    const struct option lopts[] = {
        {"help", no_argument, 0, 'h'},
        {"version", no_argument, 0, 'V'},
        {"rediscluster", required_argument, 0, 'R'},
        {"command", required_argument, 0, 'C'},
        {"rdbsql", required_argument, 0, 'S'},
        {"interactive", no_argument, 0, 'I'},
        {"output", required_argument, 0, 'O'},
        {0, 0, 0, 0}
    };

    char cluster[1024] = {0};
    char command[1024] = {0};
    char rdbsql[2048] = {0};

    char sqlfile[256] = {0};
    char output[256] = {0};

    char appcfg[260] = {0};

    char *nodenames[RDB_CLUSTER_NODES_MAX] = {0};

    char ts[24];
    ub8 startTime, endTime;

    RDBEnv env = NULL;

    startTime = RDBCurrentTime(1, ts);
    printf("# %s-%s start : %s\n", APPNAME, APPVER, ts);

    strcpy(appcfg, "file://");

    ch = get_app_path(argv[0], appcfg+7, 240);
    if (ch == -1) {
        exit(-1);
    }

    snprintf_chkd_V1(appcfg+7+ch, strlen(APPNAME) + 6, "%c%s.cfg", PATH_SEPARATOR_CHAR, APPNAME);

    while ((ch = getopt_long_only(argc, (char *const *) argv, "hVIR:C:S:O:F:", lopts, &index)) != -1) {
        switch (ch) {
        case '?':
            fprintf(stderr, "ERROR: option not defined.\n");
            exit(-1);

        case 0:
            // flag not used
            switch (index) {
            case 1:
            case 2:
            default:
                break;
            }
            break;

        case 'R':
            snprintf_chkd_V1(cluster, sizeof(cluster), "%s", optarg);
            break;

        case 'C':
            snprintf_chkd_V1(command, sizeof(command), "%s", optarg);
            break;

        case 'S':
            snprintf_chkd_V1(rdbsql, sizeof(rdbsql), "%s", optarg);
            break;

        case 'F':
            snprintf_chkd_V1(sqlfile, sizeof(sqlfile), "%s", optarg);
            break;

        case 'O':
            snprintf_chkd_V1(output, sizeof(output), "%s", optarg);
            break;

        case 'I':
            interactive = 1;
            break;

        case 'h':
            print_usage();
            exit(0);
            break;

        case 'V':
            fprintf(stdout, "%s-%s, build: %s %s\n\n", APPNAME, APPVER, __DATE__, __TIME__);
            exit(0);
            break;
        }
    }

    if (cluster[0]) {
        printf("# (%s:%d) redis cluster: %s\n", __FILE__, __LINE__, cluster);

        RDBEnvCreate(cluster, 0, 0, &env);
    } else {
        printf("# (%s:%d) load config: %s\n", __FILE__, __LINE__, appcfg);

        RDBEnvCreate(appcfg, 0, 0, &env);
    }

    if (! env) {
        printf("# Error No Cluster.\n");
        exit(EXIT_FAILURE);
    }

    if (interactive) {
        int clen;

        ub8 tm0, tm2;
        char tmstr0[24];
        char tmstr2[24];

        CSH_chunk cache = CSH_chunk_new (1);

        print_guide();

        while (interactive) {
            line = CSH_get_input(cache->offset? "+ ":"redplus> ", cshbuf, sizeof(cshbuf), &clen);

            if (line) {
                if (clen) {
                    if (CSH_quit_or_exit(line, clen) == CSH_ANSWER_YES) {
                        break;
                    }

                    CSH_chunk_add(cache, line, clen);
                    CSH_chunk_add(cache, (void*)" ", 1);

                    if (line[0] == '?') {
                        print_usage();

                        CSH_chunk_reset(cache);
                        continue;
                    }

                    if (line[clen - 1] == ';') {
                        tm0 = RDBCurrentTime(1, tmstr0);

                        redplusExecuteRdbsql(env, cache->chunk, output);

                        tm2 = RDBCurrentTime(1, tmstr2);

                        printf("\n# elapsed  : %.3lf seconds. (%"PRIu64" ms)"
                               "\n# duration : %s ~ %s\n\n",
                            (tm2 - tm0) * 0.001, (ub8)(tm2 - tm0),
                            tmstr0, tmstr2);

                        CSH_chunk_reset(cache);
                        continue;
                    }
                }
            }
        }

        CSH_chunk_free(cache);
    }

    if (*command) {
        redplusExecuteCommand(env, command, output);
    }

    if (*sqlfile) {
        redplusExecuteSqlfile(env, sqlfile, output);
    }

    if (*rdbsql) {
        redplusExecuteRdbsql(env, rdbsql, output);
    }

    RDBEnvDestroy(env);

    endTime = RDBCurrentTime(1, ts);
    printf("\n\n# end on: %s. elapsed: %.3lf seconds.\n", ts, (endTime - startTime) * 0.001);
 	return 0;
}


void redplusExecuteCommand (RDBEnv env, const char *command, const char *output)
{

}


void redplusExecuteSqlfile (RDBEnv env, const char *sqlfile, const char *output)
{
    RDBAPI_RESULT err = RDBAPI_ERROR;
    RDBCtx ctx = NULL;

    int nmaps = 0;
    RDBResultMap resultmap = NULL;

    err = RDBCtxCreate(env, &ctx);
    if (err) {
        printf("RDBCtxCreate error(%d).\n", err);
        return;
    }

    RDBCtxExecuteFile(ctx, sqlfile, &resultmap);

    RDBResultMapDestroy(resultmap);

    RDBCtxFree(ctx);
}


void redplusExecuteRdbsql (RDBEnv env, const char *rdbsql, const char *output)
{
    RDBAPI_RESULT err = RDBAPI_ERROR;

    RDBCtx ctx = NULL;

    RDBResultMap resultMap = NULL;
    RDBSQLStmt sqlstmt = NULL;

    RDBBlob_t sqlblob;

    sqlblob.str = (char *) rdbsql;
    sqlblob.length = cstr_length(rdbsql, -1);
    sqlblob.maxsz = sqlblob.length + 1;

    ub8 offset = RDBAPI_ERROR;

    err = RDBCtxCreate(env, &ctx);
    if (err) {
        printf("# failed on RDBCtxCreate error(%d).\n", err);
        goto error_exit;
    }

    offset = RDBCtxExecuteSQL(ctx, &sqlblob, &resultMap);
    if (offset == RDBAPI_ERROR) {
        printf("# failed on %s\n", RDBCtxErrMsg(ctx));
        goto error_exit;
    }

    // success
    RDBResultMapPrintOut(resultMap, "# success result:\n\n");

    RDBResultMapDestroy(resultMap);
    RDBCtxFree(ctx);
    return;

error_exit:
    if (resultMap) {
        RDBResultMapDestroy(resultMap);
    }
    if (ctx) {
        RDBCtxFree(ctx);
    }
}
