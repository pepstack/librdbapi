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

RDBZString cshbuf = NULL;
RDBZString cluster = NULL;
RDBZString command = NULL;    
RDBZString rdbsql  = NULL;
RDBZString sqlfile = NULL;
RDBZString output  = NULL;

static void redplus_cleanup(void)
{
    RDBZStringFree(cluster);
    RDBZStringFree(command);
    RDBZStringFree(rdbsql);
    RDBZStringFree(sqlfile);
    RDBZStringFree(output);
    RDBZStringFree(cshbuf);

    fprintf(stdout, "\n\nredplus exit.\n");
}


int main(int argc, const char *argv[])
{
    WINDOWS_CRTDBG_ON

    int clen, flag, optindex;

    int interactive = 0;    

    ub8 startTime;
    
    int ctxtimout = RDBCTX_TIMEOUT;
    int sotimeoms = RDBCTX_SOTIMEOMS;

    char appcfg[256] = {0};

    const struct option lopts[] = {
        {"help", no_argument, 0, 'h'},
        {"version", no_argument, 0, 'V'},
        {"rediscluster", required_argument, 0, 'R'},
        {"command", required_argument, 0, 'C'},
        {"rdbsql", required_argument, 0, 'S'},
        {"sqlfile", required_argument, 0, 'F'},
        {"output", required_argument, 0, 'O'},
        {"interactive", no_argument, 0, 'I'},
        {"ctxtimout", required_argument, &flag, 101},
        {"sotimeoms", required_argument, &flag, 102},
        {0, 0, 0, 0}
    };

    RDBEnv env = NULL;
    RDBCtx ctx = NULL;

    cshbuf = RDBZStringNew(NULL, CSHELL_BUFSIZE - 1);

    atexit(redplus_cleanup);

    startTime = RDBCurrentTime(1, appcfg);
    printf("# %s-%s start : %s\n", APPNAME, APPVER, appcfg);

    // makeup default config pathfile
    strcpy(appcfg, "file://");
    clen = get_app_path(argv[0], appcfg+7, sizeof(appcfg) - strlen(APPNAME) - 10);
    if (clen == -1) {
        exit(EXIT_FAILURE);
    }
    snprintf_chkd_V1(appcfg+7+clen, strlen(APPNAME) + 6, "%c%s.cfg", PATH_SEPARATOR_CHAR, APPNAME);

    while ((clen = getopt_long_only(argc, (char *const *) argv, "hVIR:C:S:O:F:", lopts, &optindex)) != -1) {
        switch (clen) {
        case '?':
            fprintf(stderr, "ERROR: option not defined.\n");
            exit(-1);

        case 0:
            switch (flag) {
            case 101:
                ctxtimout = atoi(optarg);
                break;
            case 102:
                sotimeoms = atoi(optarg);
                break;
            }
            break;

        case 'R':
            cluster = RDBZStringNew(optarg, CSHELL_BUFSIZE);
            break;

        case 'C':
            command = RDBZStringNew(optarg, CSHELL_BUFSIZE);
            break;

        case 'S':
            rdbsql = RDBZStringNew(optarg, CSHELL_BUFSIZE);
            break;

        case 'F':
            sqlfile = RDBZStringNew(optarg, sizeof(appcfg));
            break;

        case 'O':
            output = RDBZStringNew(optarg, sizeof(appcfg));
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

    if (cluster) {
        printf("# redis cluster: %s\n", (const char*)cluster);
        RDBEnvCreate((const char*)cluster, ctxtimout, sotimeoms, &env);
    } else {
        printf("# load config: %s\n", appcfg);
        RDBEnvCreate(appcfg, ctxtimout, sotimeoms, &env);
    }

    if (! env) {
        printf("# Error No Cluster.\n");
        exit(EXIT_FAILURE);
    }
    if (RDBCtxCreate(env, &ctx) != RDBAPI_SUCCESS) {
        printf("# RDBCtxCreate error).\n");
        exit(EXIT_FAILURE);
    }

    if (command) {
        redplusExecuteCommand(ctx, RDBCZSTR(command), RDBCZSTR(output));
    }
    if (sqlfile) {
        redplusExecuteSqlfile(ctx, RDBCZSTR(sqlfile), RDBCZSTR(output));
    }
    if (rdbsql) {
        redplusExecuteRdbsql(ctx, RDBCZSTR(rdbsql), RDBCZSTR(output));
    }

    RDBZStringFree(cluster);
    RDBZStringFree(command);
    RDBZStringFree(sqlfile);    
    RDBZStringFree(rdbsql);

    cluster = NULL;
    command = NULL;
    sqlfile = NULL;
    rdbsql = NULL;

    if (interactive) {
        ub8 tm0, tm2;
        char ts0[24];
        char ts2[24];

        CSH_chunk cache = CSH_chunk_new (1);

        print_guide();

        while (interactive) {
            char *end;
            const char *line;

            line = CSH_get_input(cache->offset? "+ ":"redplus> ", RDBZSTR(cshbuf), RDBZSTRLEN(cshbuf), &clen);

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

                    end = (char*) &line[clen - 1];

                    if (*end == '!') {
                        // cancel an execution for sql
                        fprintf(stdout, "\n# inputs are cancelled;"
                                        "\n# press ; to execute inputs;"
                                        "\n\n");
                        CSH_chunk_reset(cache);
                        continue;
                    }

                    // execute sql at once
                    if (*end == ';') {
                        line = cache->chunk;
                        while ((end = strchr(line, ';')) != NULL) {
                            *end++ = 0;

                            if (*line == '#') {
                                printf("%s;\n", line);
                            } else if (*line == '$') {
                                tm0 = RDBCurrentTime(1, ts0);

                                redplusExecuteCommand(ctx, cstr_Ltrim_whitespace((char*) &line[1]), RDBCZSTR(output));

                                tm2 = RDBCurrentTime(1, ts2);

                                printf("\n# elapsed : %.3lf seconds. (%"PRIu64" ms);"
                                       "\n# duration : %s ~ %s;\n\n", (tm2 - tm0) * 0.001, (ub8)(tm2 - tm0), ts0, ts2);
                            } else {
                                tm0 = RDBCurrentTime(1, ts0);

                                redplusExecuteRdbsql(ctx, line, RDBCZSTR(output));

                                tm2 = RDBCurrentTime(1, ts2);

                                printf("\n# elapsed : %.3lf seconds. (%"PRIu64" ms);"
                                       "\n# duration : %s ~ %s;\n\n", (tm2 - tm0) * 0.001, (ub8)(tm2 - tm0), ts0, ts2);
                            }

                            line = end;
                        }

                        CSH_chunk_reset(cache);
                    }
                }
            }
        }

        CSH_chunk_free(cache);
    }

    if (ctx) {
        RDBCtxFree(ctx);
    }
    if (env) {
        RDBEnvDestroy(env);
    }
 	exit(0);
}


void redplusExecuteCommand (RDBCtx ctx, const char *command, const char *output)
{
    printf("# RedisExecCommand: %s\n", command);

    RDBCtxNode ctxnode = NULL;
    redisReply *reply = RedisExecCommand(ctx, command, &ctxnode);

    if (reply) {
        printf("# SUCCESS:\n");
        RedisReplyObjectPrint(reply, ctxnode);
        RedisFreeReplyObject(&reply);
    } else {
        printf("# FAILED: %s\n", RDBCtxErrMsg(ctx));
    }    
}


void redplusExecuteSqlfile (RDBCtx ctx, const char *sqlfile, const char *output)
{
    RDBResultMap resultmap = NULL;

    if (RDBCtxExecuteFile(ctx, sqlfile, &resultmap) == RDBAPI_SUCCESS) {
        RDBResultMapPrint(ctx, resultmap, stdout);
    }

    RDBResultMapDestroy(resultmap);
}


void redplusExecuteRdbsql (RDBCtx ctx, const char *rdbsql, const char *output)
{
    RDBResultMap resultmap = NULL;

    RDBZString sqlstr = RDBZStringNew(rdbsql, -1);

    if (RDBCtxExecuteSql(ctx, sqlstr, &resultmap) == RDBAPI_SUCCESS) {
        RDBZStringFree(sqlstr);

        if (resultmap) {
            RDBResultMapPrint(ctx, resultmap, stdout);
        }
    } else {
        RDBZStringFree(sqlstr);
        printf("# RDBCtxExecuteSql failed: %s\n", RDBCtxErrMsg(ctx));
    }

    if (resultmap) {
        RDBResultMapDestroy(resultmap);
    }
}
