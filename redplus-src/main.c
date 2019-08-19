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


#ifndef __WINDOWS__
/**
 * realfilepath()
 *   for linux
 *
 * returns:
 *   success:
 *     > 0 - length of rpdir
 *   error:
 *     <= 0 - failed anyway
 */
static int realfilepath (const char * file, char * rpdirbuf, size_t maxlen)
{
    char * p;

    p = realpath(file, 0);

    if (p) {
        snprintf_chkd(rpdirbuf, maxlen, "%s", p);

        free(p);

        p = strrchr(rpdirbuf, '/');
        if (p) {
            *++p = 0;

            /* success return here */
            return (int)(p - rpdirbuf);
        }

        snprintf_chkd(rpdirbuf, maxlen, "invlid path: %s", file);
        return 0;
    }

    snprintf_chkd(rpdirbuf, maxlen, "realpath error(%d): %s", errno, strerror(errno));
    return (-2);
}
#endif


static int get_app_path (const char *argv0, char apppath[], size_t sz)
{
    bzero(apppath, sizeof(apppath[0]) * sz);

#ifdef __WINDOWS__
# define PATH_SEPARATOR_CHAR  '\\'

    GetModuleFileNameA(NULL, apppath, (DWORD) sz);

    if (strnlen(apppath, sz) == sz) {
        printf("app path too long: %s\n", argv0);
        return (-1);
    }

    if (! strrchr(apppath, PATH_SEPARATOR_CHAR)) {
        printf("invalid path: %s\n", apppath);
        return (-1);
    }

#else
# define PATH_SEPARATOR_CHAR  '/'

    int ret = realfilepath(argv0, apppath, sz - 1);

    if (ret <= 0) {
        fprintf(stderr, "\033[1;31m[error]\033[0m %s\n", apppath);
        return (-1);
    }

    if (strrchr(apppath, PATH_SEPARATOR_CHAR) == strchr(apppath, PATH_SEPARATOR_CHAR)) {
        fprintf(stderr, "\033[1;31m[error]\033[0m under root path: %s\n", apppath);
        return (-1);
    }

    if (ret >= sz) {
        fprintf(stderr, "\033[1;31m[error]\033[0m path is too long: %s\n", apppath);
        return (-1);
    }

    if (! strrchr(apppath, PATH_SEPARATOR_CHAR)) {
        printf("invalid path: %s\n", apppath);
        return (-1);
    }

#endif

    *strrchr(apppath, PATH_SEPARATOR_CHAR) = 0;

    return (int) strlen(apppath);
}


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

void redplus_exec_command (RDBEnv env, const char *command, const char *output);
void redplus_exec_sqlfile (RDBEnv env, const char *redsql, const char *output);
void redplus_exec_redsql (RDBEnv env, const char *redsql, const char *output);


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
        {"rediscluster", required_argument, 0, 'R'},
        {"command", required_argument, 0, 'C'},
        {"rdbsql", required_argument, 0, 'S'},
        {"output", required_argument, 0, 'O'},
        {0, 0, 0, 0}
    };

    char cluster[1024] = {0};
    char command[1024] = {0};
    char redsql[2048] = {0};

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

    snprintf_chkd(appcfg+7+ch, strlen(APPNAME) + 6, "%c%s.cfg", PATH_SEPARATOR_CHAR, APPNAME);

    while ((ch = getopt_long_only(argc, (char *const *) argv, "hVR:C:S:O:F:", lopts, &index)) != -1) {
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
            snprintf_chkd(cluster, sizeof(cluster), "%s", optarg);
            break;

        case 'C':
            snprintf_chkd(command, sizeof(command), "%s", optarg);
            break;

        case 'S':
            snprintf_chkd(redsql, sizeof(redsql), "%s", optarg);
            break;

        case 'F':
            snprintf_chkd(sqlfile, sizeof(sqlfile), "%s", optarg);
            break;

        case 'O':
            snprintf_chkd(output, sizeof(output), "%s", optarg);
            break;

        case 'h':
            redplus_print_usage();
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
        printf("# No Cluster.\n");
        exit(EXIT_FAILURE);
    }

    if (*command) {
        redplus_exec_command(env, command, output);
    }

    if (*sqlfile) {
        redplus_exec_sqlfile(env, sqlfile, output);
    }

    if (*redsql) {
        redplus_exec_redsql(env, redsql, output);
    }

    RDBEnvDestroy(env);

    endTime = RDBCurrentTime(1, ts);
    printf("\n\n# end on: %s. elapsed: %.3lf seconds.\n", ts, (endTime - startTime) * 0.001);
 	return 0;
}


void redplus_print_usage()
{
#ifndef __WINDOWS__
    printf("Usage: %s.exe [Options...] \n", APPNAME);
#else
    printf("Usage: %s [Options...] \n", APPNAME);
#endif
    printf("%s is a Redis SQL tool.\n", APPNAME);

    printf("Options:\n");
    printf("  -h, --help               Display help info\n");
    printf("  -V, --version            Show version info\n\n");
    printf("  -R, --rediscluster=HOSTS Redis cluster host nodes. (example: 'authpass@127.0.0.1:7001-7009')\n");
    printf("  -C, --command=REDISCMD   Redis command to call.\n");
    printf("  -S, --rdbsql=RDBSQL      SQL to execute on redisdb. (example: SELECT * FROM a.t WHERE ...)\n");
    printf("  -F, --sqlfile=PATHFILE   execute a SQL file.\n");
    printf("  -O, --output=PATHFILE    Pathfile to store output result map\n\n");
}


void redplus_exec_command (RDBEnv env, const char *command, const char *output)
{

}


void redplus_exec_sqlfile (RDBEnv env, const char *sqlfile, const char *output)
{
    RDBAPI_RESULT err = RDBAPI_ERROR;
    RDBCtx ctx = NULL;

    int nmaps = 0;
    RDBResultMap *resultMaps = NULL;

    err = RDBCtxCreate(env, &ctx);
    if (err) {
        printf("RDBCtxCreate error(%d).\n", err);
        return;
    }

    nmaps = RDBSQLExecuteFile(ctx, sqlfile, &resultMaps);

    RDBResultMapListFree(resultMaps, nmaps);

    RDBCtxFree(ctx);
}


void redplus_exec_redsql (RDBEnv env, const char *rdbsql, const char *output)
{
    RDBAPI_RESULT err = RDBAPI_ERROR;

    RDBCtx ctx = NULL;

    RDBResultMap resultMap = NULL;
    RDBSQLParser sqlparser = NULL;

    ub8 offset = RDBAPI_ERROR;

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

    offset = RDBSQLExecute(ctx, sqlparser, &resultMap);
    if (offset == RDBAPI_ERROR) {
        printf("RDBSQLExecute failed: %s", RDBCtxErrMsg(ctx));
        goto error_exit;
    }

    if (RDBSQLParserGetStmt(sqlparser, NULL, 0) == RDBSQL_SELECT) {
        RDBResultMapPrintOut(resultMap, 1);

        printf("# result rows=%"PRIu64".\n", RDBResultMapSize(resultMap));
        printf("# last offset=%"PRIu64".\n", offset);
    } else if (RDBSQLParserGetStmt(sqlparser, NULL, 0) == RDBSQL_CREATE) {
        RDBResultMapPrintOut(resultMap, 1);

        printf("table create success.\n");
    } else if (RDBSQLParserGetStmt(sqlparser, NULL, 0) == RDBSQL_DESC_TABLE) {
        printf("success.\n");
    }

    RDBResultMapClean(resultMap);

    RDBSQLParserFree(sqlparser);
    RDBResultMapFree(resultMap);
    RDBCtxFree(ctx);

    return;

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
}
