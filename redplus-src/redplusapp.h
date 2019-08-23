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
 * redplusapp.h
 *   redis command tool application.
 *
 * create: 2019-08-01
 * update: 2019-08-23
 * version: 0.0.1
 */
#ifndef REDPLUSAPP_H_
#define REDPLUSAPP_H_

#if defined(__cplusplus)
extern "C"
{
#endif

#define APPNAME  "redplus"
#define APPVER   "0.0.1"

#include <rdbapi.h>

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

# include <Windows.h>
# include <common/getoptw.h>

# pragma comment(lib, "librdbapi-w32lib.lib")
# pragma comment(lib, "hiredis.lib")
# pragma comment(lib, "Win32_Interop.lib")

# pragma comment(lib, "liblog4c.lib")

#else // __WINDOWS__

/* Linux: link liblog4c.lib */
# include <getopt.h>  /* getopt_long */
# include <common/log4c_logger.h>

#endif

#include <common/cshell.h>


#if defined(__WINDOWS__) && defined(_DEBUG)
    #define WINDOWS_CRTDBG_ON  \
        _CrtSetDbgFlag(_CrtSetDbgFlag(_CRTDBG_REPORT_FLAG) | _CRTDBG_LEAK_CHECK_DF);
#else
    #define WINDOWS_CRTDBG_ON
#endif


extern void redplusExecuteCommand (RDBEnv env, const char *command, const char *outfile);

extern void redplusExecuteSqlfile (RDBEnv env, const char *sqlfile, const char *outfile);

extern void redplusExecuteRdbsql (RDBEnv env, const char *rdbsql, const char *outfile);


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
        snprintf_chkd_V1(rpdirbuf, maxlen, "%s", p);

        free(p);

        p = strrchr(rpdirbuf, '/');
        if (p) {
            *++p = 0;

            /* success return here */
            return (int)(p - rpdirbuf);
        }

        snprintf_chkd_V1(rpdirbuf, maxlen, "invlid path: %s", file);
        return 0;
    }

    snprintf_chkd_V1(rpdirbuf, maxlen, "realpath error(%d): %s", errno, strerror(errno));
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
        fprintf(stdout, "app path too long: %s\n", argv0);
        return (-1);
    }

    if (! strrchr(apppath, PATH_SEPARATOR_CHAR)) {
        fprintf(stdout, "invalid path: %s\n", apppath);
        return (-1);
    }

#else
# define PATH_SEPARATOR_CHAR  '/'

    int ret = realfilepath(argv0, apppath, sz - 1);

    if (ret <= 0) {
        ffprintf(stdout, stderr, "\033[1;31m[error]\033[0m %s\n", apppath);
        return (-1);
    }

    if (strrchr(apppath, PATH_SEPARATOR_CHAR) == strchr(apppath, PATH_SEPARATOR_CHAR)) {
        ffprintf(stdout, stderr, "\033[1;31m[error]\033[0m under root path: %s\n", apppath);
        return (-1);
    }

    if (ret >= sz) {
        ffprintf(stdout, stderr, "\033[1;31m[error]\033[0m path is too long: %s\n", apppath);
        return (-1);
    }

    if (! strrchr(apppath, PATH_SEPARATOR_CHAR)) {
        fprintf(stdout, "invalid path: %s\n", apppath);
        return (-1);
    }

#endif

    *strrchr(apppath, PATH_SEPARATOR_CHAR) = 0;

    return (int) strlen(apppath);
}


static void print_guide ()
{
    fprintf(stdout, "# Press ; to execute commands!\n");
    fprintf(stdout, "# Press quit or Ctrl-C to exit.\n");
    fprintf(stdout, "# Press \? to show help.\n");
    fflush(stdout);
}


void print_usage()
{
#ifndef __WINDOWS__
    fprintf(stdout, "Usage: %s.exe [Options...] \n", APPNAME);
#else
    fprintf(stdout, "Usage: %s [Options...] \n", APPNAME);
#endif
    fprintf(stdout, "%s is a Redis SQL tool.\n", APPNAME);

    fprintf(stdout, "Options:\n");
    fprintf(stdout, "  -h, --help               Display help info\n");
    fprintf(stdout, "  -V, --version            Show version info\n\n");
    fprintf(stdout, "  -R, --rediscluster=HOSTS Redis cluster host nodes. (example: 'authpass@127.0.0.1:7001-7009')\n");
    fprintf(stdout, "  -C, --command=REDISCMD   Redis command to call.\n");
    fprintf(stdout, "  -S, --rdbsql=RDBSQL      SQL to execute on redisdb. (example: SELECT * FROM a.t WHERE ...)\n");
    fprintf(stdout, "  -F, --sqlfile=PATHFILE   execute a SQL file.\n");
    fprintf(stdout, "  -O, --output=PATHFILE    Pathfile to store output result map\n\n");

    fflush(stdout);
}

#if defined(__cplusplus)
}
#endif

#endif /* REDPLUSAPP_H_ */
