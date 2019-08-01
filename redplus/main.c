/**
 * main.c
 *   redis command tool application.
 *
 * create: 2019-08-01
 * update: 2019-08-01
 * version: 0.0.1
 */

#include <rdbapi.h>

#ifdef __WINDOWS__
/** memory leak auto-detect in MSVC
 * https://blog.csdn.net/lyc201219/article/details/62219503
 */
# define _CRTDBG_MAP_ALLOC
# include <stdlib.h>
# include <crtdbg.h>

#include <common/getoptw.h>

# pragma comment(lib, "librdbapi-w32lib.lib")
# pragma comment(lib, "hiredis.lib")
# pragma comment(lib, "Win32_Interop.lib")
#else
/* Linux */
# include <getopt.h>  /* getopt_long */
#endif


int main(int argc, const char *argv[])
{
#ifdef __WINDOWS__
    int dbgFlag = _CrtSetDbgFlag(_CRTDBG_REPORT_FLAG);
    dbgFlag |= _CRTDBG_LEAK_CHECK_DF;
    _CrtSetDbgFlag(dbgFlag);
#endif

    RDBEnv env;

    char ts[24];
    ub8 startTime = RDBCurrentTime(1, ts);

    printf("start on: %s\n", ts);

    RDBEnvCreate(0, 9, &env);

    RDBEnvInitAllNodes(env, "test@192.168.39.111:7001-7009", -1, 0, 12000);
  
    RDBEnvDestroy(env);

 	return 0;
}
