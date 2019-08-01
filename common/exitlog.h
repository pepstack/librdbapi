/***********************************************************************
* Copyright (c) 2008-2080 syna-tech.com, pepstack.com, 350137278@qq.com
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
 * @file: exitlog.h
 *    syslog marcos for both linux and windows
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-05-02
 * @update: 2019-06-17
 */
#ifndef EXITLOG_H_INCLUDED
#define EXITLOG_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#if (defined(WIN32) || defined(_WIN16) || defined(_WIN32) || defined(_WIN64)) && !defined(__WINDOWS__)
# define __WINDOWS__
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined (__WINDOWS__)
# include <time.h>
# include <WinSock2.h>
# include "syslog-win32/syslog.h"
#else
# include <sys/time.h>
# include <syslog.h>
#endif


#ifdef _WINSOCK2API_
/* output to sock: localhost:514 */
# define exitlog_msg(errcode, ident, message, ...) do { \
    char __messagebuf[256]; \
    _snprintf(__messagebuf, sizeof(__messagebuf) - 1, message, __VA_ARGS__); \
    __messagebuf[255] = '\0'; \
    if (! ident) { \
        openlog("exitlog.h", LOG_PID | LOG_NDELAY | LOG_NOWAIT | LOG_PERROR, 0); \
    } else { \
        openlog(ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT | LOG_PERROR, 0); \
    } \
    syslog(LOG_USER | LOG_CRIT, "%s %s (%s:%d) FATAL <%s> %s.\n", __DATE__, __TIME__, __FILE__, __LINE__, __FUNCTION__, __messagebuf); \
    closelog(); \
    exit(errcode); \
} while(0)
#else
/* output to file: /var/log/messages */
# define exitlog_msg(errcode, ident, message, args...) do { \
    char __messagebuf[256]; \
    snprintf(__messagebuf, sizeof(__messagebuf), message, ##args); \
    __messagebuf[255] = '\0'; \
    if (! ident) { \
        openlog("exitlog.h", LOG_PID | LOG_NDELAY | LOG_NOWAIT | LOG_PERROR, 0); \
    } else { \
        openlog(ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT | LOG_PERROR, 0); \
    } \
    syslog(LOG_USER | LOG_CRIT, "%s %s (%s:%d) FATAL <%s> %s.\n", __DATE__, __TIME__, __FILE__, __LINE__, __FUNCTION__, __messagebuf); \
    closelog(); \
    exit(errcode); \
} while(0)
#endif

#if defined(WIN32FIXES_H)
    /* <Win32_Interop/win32fixes.h> */
# define exitlog_oom(ident)  exit(EXIT_FAILURE)

# define exitlog_oom_check(p, ident)  if (!p) exit(EXIT_FAILURE)

# define exitlog_err_check(err, ident, message)  if (err) exit(EXIT_FAILURE)

#else

# define exitlog_oom(ident)  exitlog_msg(EXIT_FAILURE, ident, "ENOMEM")

# define exitlog_oom_check(p, ident)  if (!p) exitlog_msg(EXIT_FAILURE, ident, "ENOMEM")

# define exitlog_err_check(err, ident, message)  if (err) exitlog_msg(EXIT_FAILURE, ident, "%s error(%d)", message, (int)(err))

#endif

#if defined(__cplusplus)
}
#endif

#endif /* EXITLOG_H_INCLUDED */
