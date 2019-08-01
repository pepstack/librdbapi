/**
 * main.c
 *   redis command tool application.
 *
 * create: 2019-08-01
 * update: 2019-08-01
 * version: 0.0.1
 */

#include <rdbapi.h>

#define APPNAME  "redplus"
#define APPVER   "0.0.1"


#ifdef __WINDOWS__
/** memory leak auto-detect in MSVC
 * https://blog.csdn.net/lyc201219/article/details/62219503
 */
# define _CRTDBG_MAP_ALLOC
# include <stdlib.h>
# include <crtdbg.h>

#include <common/getoptw.h>
#include <common/log4c_logger.h>

# pragma comment(lib, "librdbapi-w32lib.lib")
# pragma comment(lib, "hiredis.lib")
# pragma comment(lib, "Win32_Interop.lib")

# pragma comment(lib, "liblog4c.lib")
#else
/* Linux: link liblog4c.lib */
# include <getopt.h>  /* getopt_long */
# include <common/log4c_logger.h>
#endif

#include <common/cstrutil.h>


static void redplus_check_result(const char *errsour, RDBCtx ctx, RDBAPI_RESULT res)
{
    if (res != RDBAPI_SUCCESS) {
        if (ctx) {
            printf("%s result(%d): %s.\n", errsour, res, RDBCtxErrMsg(ctx));
        }

        exit(EXIT_FAILURE);
    }
}

void redplus_print_usage();

int redplus_exec_command (const char *cluster, int numnodes, const char *command, const char *output);

int redplus_exec_redsql (const char *cluster, int numnodes, const char *redsql, const char *output);


int main(int argc, const char *argv[])
{
#ifdef __WINDOWS__
    int dbgFlag = _CrtSetDbgFlag(_CRTDBG_REPORT_FLAG);
    dbgFlag |= _CRTDBG_LEAK_CHECK_DF;
    _CrtSetDbgFlag(dbgFlag);
#endif

    int ch, index;

    const struct option lopts[] = {
        {"help", no_argument, 0, 'h'},
        {"version", no_argument, 0, 'V'},
        {"numnodes", required_argument, 0, 'N'},
        {"rediscluster", required_argument, 0, 'R'},
        {"command", required_argument, 0, 'C'},
        {"redsql", required_argument, 0, 'S'},
        {"output", required_argument, 0, 'O'},
        {0, 0, 0, 0}
    };

    char cluster[1024] = {0};
    char command[1024] = {0};
    char redsql[2048] = {0};
    char output[256] = {0};

    int numnodes = 9;

    while ((ch = getopt_long_only(argc, (char *const *) argv, "hVR:C:S:O:", lopts, &index)) != -1) {
        switch (ch) {
        case '?':
            fprintf(stderr, "\033[1;31m[error]\033[0m option not defined.\n");
            exit(-1);

        case 0:
            // flag ²»Îª 0
            switch (index) {
            case 1:
            case 2:
            default:
                break;
            }
            break;

        case 'R':
            snprintf(cluster, sizeof(cluster), "%s", optarg);
            break;

        case 'N':
            numnodes = atoi(optarg);
            break;

        case 'C':
            snprintf(command, sizeof(command), "%s", optarg);
            break;

        case 'S':
            snprintf(redsql, sizeof(redsql), "%s", optarg);
            break;

        case 'O':
            snprintf(output, sizeof(output), "%s", optarg);
            break;

        case 'h':
            redplus_print_usage();
            exit(0);
            break;

        case 'V':
            fprintf(stdout, "\033[1;35m%s-%s, build: %s %s\033[0m\n\n", APPNAME, APPVER, __DATE__, __TIME__);
            exit(0);
            break;
        }
    }

    cluster[1023] = 0;
    command[1023] = 0;
    redsql[2047] = 0;
    output[255] = 0;

    char ts[24];

    ub8 startTime = RDBCurrentTime(1, ts);

    printf("%s-%s start : %s\n"
           "redis cluster : %s\n"
           "numb of nodes : %d\n",
           APPNAME, APPVER, ts, cluster, numnodes);

    redplus_exec_command(cluster, numnodes, command, output);

    redplus_exec_redsql(cluster, numnodes, redsql, output);

 	return 0;
}


void redplus_print_usage()
{
}


int redplus_exec_command (const char *cluster, int numnodes, const char *command, const char *output)
{
    RDBAPI_RESULT err;
    RDBEnv env;
    RDBCtx ctx;

    RDBEnvCreate(0, numnodes, &env);

    err = RDBEnvInitAllNodes(env, cluster, -1, 0, 12000);
    if (err) {
        printf("RDBEnvInitAllNodes failed(%d).\n", err);
        exit(EXIT_FAILURE);
    }

    err = RDBCtxCreate(env, &ctx);
    if (err) {
        printf("RDBCtxCreate failed(%d).\n", err);
        exit(EXIT_FAILURE);
    }



    RDBCtxFree(ctx);
    RDBEnvDestroy(env);
    return 0;
}


int redplus_exec_redsql (const char *cluster, int numnodes, const char *redsql, const char *output)
{
    RDBAPI_RESULT err;
    RDBEnv env;
    RDBCtx ctx;

    int sqlen = (int) strlen(redsql);
    if (sqlen <= 20) {
        printf("redplus error: invalid sql!\n");
        exit(EXIT_FAILURE);
    }

    RDBEnvCreate(0, numnodes, &env);

    err = RDBEnvInitAllNodes(env, cluster, -1, 0, 12000);
    if (err) {
        printf("RDBEnvInitAllNodes failed(%d).\n", err);
        exit(EXIT_FAILURE);
    }

    err = RDBCtxCreate(env, &ctx);
    if (err) {
        printf("RDBCtxCreate failed(%d).\n", err);
        exit(EXIT_FAILURE);
    }

    if (cstr_startwith(redsql, sqlen, "select ", 7)) {
        // select $fields from $table where $condition offset $n limit $m
        int len;
        char *p;
        const char *from = strstr(redsql, " from ");

        char fields[1024] = {0};
        char table[256] = {0};

        char _where[1024] = {0};
        char _offset[40] = {0};
        char _limit[40] = {0};

        ub8 offset = 0;
        ub8 limit = -1;

        ub8 total = 0;

        if (! from) {
            printf("bad sql: %.*s ...\n", 20, redsql);
            exit(EXIT_FAILURE);
        }

        len = (int) (from - redsql) - 7;
        len = snprintf(fields, sizeof(fields) - 1, "%.*s", len, redsql + 7);
        if (len == sizeof(fields) - 1) {
            printf("bad sql: %.*s ...\n", 20, redsql);
            exit(EXIT_FAILURE);
        }

        if (strchr(from + 6, 32)) {
            len = (int)(strchr(from + 6, 32) - from - 6);
            len = snprintf(table, sizeof(table) - 1, "%.*s", len, from + 6);
            if (len == sizeof(table) - 1) {
                printf("bad sql: %.*s ...\n", 20, redsql);
                exit(EXIT_FAILURE);
            }
        } else if (strchr(from + 6, ';')) {
            len = (int)(strchr(from + 6, ';') - from - 6);
            len = snprintf(table, sizeof(table) - 1, "%.*s", len, from + 6);
            if (len == sizeof(table) - 1) {
                printf("bad sql: %.*s ...\n", 20, redsql);
                exit(EXIT_FAILURE);
            }
        } else {
            printf("bad sql: %.*s ...\n", 20, redsql);
            exit(EXIT_FAILURE);
        }

        p = strstr(from, " where ");
        if (p) {
            len = sqlen - (int)(p + 7 - redsql);
            len = snprintf(_where, sizeof(_where) - 1, "%.*s", len, p + 7);
            if (len == sizeof(_where) - 1) {
                printf("bad sql: %.*s ...\n", 20, redsql);
                exit(EXIT_FAILURE);
            }

            p = strstr(_where, " offset ");
            if (p) {
                *p = 0;
            }
            p = strstr(_where, " limit ");
            if (p) {
                *p = 0;
            }
        }

        p = strstr(from, " offset ");
        if (p) {
            len = (int) (sqlen - (p + 8 - redsql));
            len = snprintf(_offset, sizeof(_offset) - 1, "%.*s", len, p + 8);
            if (len == sizeof(_offset) - 1) {
                printf("bad sql: %.*s ...\n", 20, redsql);
                exit(EXIT_FAILURE);
            }

            p = strstr(_offset, " limit ");
            if (p) {
                *p = 0;
            }

            if (! cstr_to_ub8(10, _offset, (int) strlen(_offset), &offset)) {
                printf("error: offset %s\n", _offset);
                exit(EXIT_FAILURE);
            }
        }

        p = strstr(from, " limit ");
        if (p) {
            len = (int) (sqlen - (p + 7 - redsql));
            len = snprintf(_limit, sizeof(_limit) - 1, "%.*s", len, p + 7);
            if (len == sizeof(_limit) - 1) {
                printf("bad sql: %.*s ...\n", 20, redsql);
                exit(EXIT_FAILURE);
            }

            p = strstr(_limit, " offset ");
            if (p) {
                *p = 0;
            }

            if (! cstr_to_ub8(10, _limit, (int) strlen(_limit), &limit)) {
                printf("error: limit %s\n", _limit);
                exit(EXIT_FAILURE);
            }
        }
        

        printf("select %s from %s where (%s) offset %s limit %s;\n", fields, table, _where, _offset, _limit);

        do {
            RDBResultMap resultMap;

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

            err = RDBTableScanFirst(ctx, "xsdb", "logentry", 0, 0, 0, 0, 0, fields, fieldexprs, fieldvals, 0, 0, &resultMap);
            if (err == RDBAPI_SUCCESS) {
                while ((offset = RDBTableScanNext(resultMap, offset, (ub4) limit)) != RDB_ERROR_OFFSET) {
                    RDBResultMapPrintOut(resultMap, 1);

                    total += RDBResultMapSize(resultMap);

                    RDBResultMapClean(resultMap);
                }

                RDBResultMapFree(resultMap);
            }

        } while(0);

        printf("total rows=%"PRIu64".\n", total);
    } else if (cstr_startwith(redsql, sqlen, "delete from ", 12)) {
        // delete from $table where $condition
        printf("Not support: %.*s ...\n", 20, redsql);
    } else if (cstr_startwith(redsql, sqlen, "update ", 7)) {
        // update $table
        printf("Not support: %.*s ...\n", 20, redsql);
    } else if (cstr_startwith(redsql, sqlen, "create ", 7)) {
        // create $table
        printf("Not support: %.*s ...\n", 20, redsql);
    } else {
        printf("Not support: %.*s ...\n", 20, redsql);
    }

    RDBCtxFree(ctx);
    RDBEnvDestroy(env);
    return 0;
}