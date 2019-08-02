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


int redplus_exec_redsql (const char *cluster, int numnodes, const char *rdbsql, const char *output)
{
    RDBAPI_RESULT err = RDBAPI_ERROR;

    RDBEnv env = NULL;
    RDBCtx ctx = NULL;

    RDBResultMap resultMap = NULL;
    RDBSQLParser sqlparser = NULL;

    err = RDBEnvCreate(0, numnodes, &env);
    if (err) {
        printf("RDBEnvCreate error(%d).\n", err);
        goto error_exit;
    }

    err = RDBEnvInitAllNodes(env, cluster, -1, 0, 12000);
    if (err) {
        printf("RDBEnvInitAllNodes error(%d).\n", err);
        goto error_exit;
    }

    err = RDBCtxCreate(env, &ctx);
    if (err) {
        printf("RDBCtxCreate error(%d).\n", err);
        goto error_exit;
    }

    err = RDBSQLParserNew(ctx, rdbsql, -1, &sqlparser);
    if (err) {
        printf("RDBSQLParserNew failed: %s\n", RDBCtxErrMsg(ctx));
        goto error_exit;
    }

    err = RDBSQLExecute(ctx, sqlparser, &resultMap);
    if (err) {
        printf("RDBSQLExecute failed: %s\n", RDBCtxErrMsg(ctx));
        goto error_exit;
    }

    // TODO:


    err = RDBAPI_SUCCESS;

error_exit:
    if (sqlparser) {
        RDBSQLParserFree(sqlparser);
    }

    if (resultMap) {
        RDBResultMapFree(resultMap);
    }

    if (ctx) {
        RDBCtxFree(ctx);
    }

    if (env) {
        RDBEnvDestroy(env);
    }

    return err;
}