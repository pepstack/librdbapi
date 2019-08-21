/***********************************************************************
* COPYRIGHT (C) 2018 PEPSTACK, PEPSTACK.COM
*
* THIS SOFTWARE IS PROVIDED 'AS-IS', WITHOUT ANY EXPRESS OR IMPLIED
* WARRANTY. IN NO EVENT WILL THE AUTHORS BE HELD LIABLE FOR ANY DAMAGES
* ARISING FROM THE USE OF THIS SOFTWARE.
*
* PERMISSION IS GRANTED TO ANYONE TO USE THIS SOFTWARE FOR ANY PURPOSE,
* INCLUDING COMMERCIAL APPLICATIONS, AND TO ALTER IT AND REDISTRIBUTE IT
* FREELY, SUBJECT TO THE FOLLOWING RESTRICTIONS:
*
*  THE ORIGIN OF THIS SOFTWARE MUST NOT BE MISREPRESENTED; YOU MUST NOT
*  CLAIM THAT YOU WROTE THE ORIGINAL SOFTWARE. IF YOU USE THIS SOFTWARE
*  IN A PRODUCT, AN ACKNOWLEDGMENT IN THE PRODUCT DOCUMENTATION WOULD
*  BE APPRECIATED BUT IS NOT REQUIRED.
*
*  ALTERED SOURCE VERSIONS MUST BE PLAINLY MARKED AS SUCH, AND MUST NOT
*  BE MISREPRESENTED AS BEING THE ORIGINAL SOFTWARE.
*
*  THIS NOTICE MAY NOT BE REMOVED OR ALTERED FROM ANY SOURCE DISTRIBUTION.
***********************************************************************/

/**
 * @file: common.h
 *
 *
 * @author: master@pepstack.com
 *
 * @version: 1.8.8
 *
 * @create: 2018-01-09
 *
 * @update: 2019-06-14
 */

#ifndef COMMON_INCL_H_INCLUDED
#define COMMON_INCL_H_INCLUDED

/* must firstly be included */
#include "memapi.h"
#include "randctx.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>          /* close access */
#include <fcntl.h>           /* open */
#include <sys/mman.h>        /* mmap */
#include <signal.h>

#include <pthread.h>         /* link: -pthread */
#include <sys/types.h>       /* fstat */
#include <sys/wait.h>        /* waitpid */
#include <sys/socket.h>
#include <sys/stat.h>        /* mkdir */
#include <sys/resource.h>
#include <sys/time.h>
#include <sys/sendfile.h>    /* sendfile */
#include <sys/ioctl.h>       /* ioctl, FIONREAD */

#include <dirent.h>          /* open, opendir, closedir, rewinddir, seekdir, telldir, scandir */

#include <time.h>

#include <netinet/in.h>      /* sockaddr_in */
#include <netinet/tcp.h>

#include <arpa/inet.h>       /* inet_addr */
#include <netdb.h>
#include <semaphore.h>
#include <stdarg.h>

#include <getopt.h>          /* getopt_long */
#include <regex.h>           /* regex_t */

/* openssl should be installed first */
#ifdef OPENSSL_NO_MD5
  #undef OPENSSL_NO_MD5
#endif
#ifndef OPENSSL_NO_MD5
  #include <openssl/md5.h>
#endif


/**
 * PATH_MAX 在 limits.h 中被定义 = 4096
 */
#ifndef PATH_MAX
#  define PATH_MAX  4096
#endif

/**
 * NAME_MAX 在 dirent.h 中被定义 = 255
 */
#ifndef NAME_MAX
#  define NAME_MAX  255
#endif

/**
 * max number of characters of file name NOT including path and end null char '\0'.
 */
#ifndef FILENAME_MAXLEN
#  define FILENAME_MAXLEN  NAME_MAX
#endif

/**
 * max number of characters of path and file name NOT including the end null char '\0'.
 */
#ifndef PATHFILE_MAXLEN
#  define PATHFILE_MAXLEN  (PATH_MAX - 1)
#endif

#ifndef HOSTNAME_MAXLEN
#  define HOSTNAME_MAXLEN  127
#endif

#ifndef ERRORMSG_MAXLEN
#  define ERRORMSG_MAXLEN  255
#endif

#ifndef BUFFER_MAXSIZE
#  define BUFFER_MAXSIZE   16384
#endif


#ifndef snprintf_chkd
    #if (_BSD_SOURCE || _XOPEN_SOURCE >= 500 || _ISOC99_SOURCE || _POSIX_C_SOURCE >= 200112L)
        #define snprintf_chkd(str, size, format, args...) do { \
			    if (snprintf(str, (size), format, ##args) < 0) { \
                    perror("snprintf error"); \
                    abort(); \
                } \
            } while(0)
    #else
        #define snprintf_chkd(str, size, format, args...)  do {\
                perror("snprintf not supported"); \
                abort(); \
            } while(0)
    #endif
#endif


#ifndef snprintf_chkd_len
    #if (_BSD_SOURCE || _XOPEN_SOURCE >= 500 || _ISOC99_SOURCE || _POSIX_C_SOURCE >= 200112L)
        #define snprintf_chkd_len(len, str, size, format, args...)  do {\
                (len) = snprintf(str, size, format, ##args); \
                if ((len) < 0) { \
                    perror("snprintf error"); \
                    abort(); \
                } else if ((len) >= (int) (size)) { \
                    fprintf(stderr, "snprintf error: output was truncated"); \
                    abort(); \
                } \
            } while(0)
    #else
        #define snprintf_chkd_len(len, str, size, format, args...)  do {\
                perror("snprintf not supported"); \
                abort(); \
            } while(0)
    #endif
#endif


#endif /* COMMON_INCL_H_INCLUDED */
