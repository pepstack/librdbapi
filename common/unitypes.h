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
* unitypes.h
*
*   Universal Standard definitions and types, Bob Jenkins
*
* 2019-08-21: revised
*/
#ifndef UNITYPES_H_INCLUDED
#define UNITYPES_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#if (defined(WIN32) || defined(_WIN16) || defined(_WIN32) || defined(_WIN64)) && !defined(__WINDOWS__)
# define __WINDOWS__
#endif

#ifndef STDIO
# include <stdio.h>
# include <stdlib.h>
# include <string.h>
# include <assert.h>
# include <stdarg.h>

# include <float.h>
# include <limits.h>
# include <stdint.h>
# define STDIO
#endif

#ifndef STDDEF
# include <stddef.h>
# define STDDEF
#endif

#ifndef BCOPY
# if defined(__WINDOWS__)
#   define bcopy(s,d,n)    memcpy((void*)(d), (const void*)(s), (size_t)(n))
# endif
# define BCOPY
#endif

#ifndef BZERO
# if defined(__WINDOWS__)
#   define bzero(s,n)    memset((void*)(s), 0, (size_t)(n))
# endif
# define BZERO
#endif

#ifndef SNPRINTF
# if defined(__WINDOWS__)
#   define snprintf    _snprintf
# endif
# define SNPRINTF
#endif

#if defined(__WINDOWS__)
# include <basetsd.h>  /* Type definitions for the basic sized types. */
  typedef SSIZE_T ssize_t;
#endif


#if defined (_SVR4) || defined (SVR4) || defined (__OpenBSD__) || \
    defined (_sgi) || defined (__sun) || defined (sun) || \
    defined (__digital__) || defined (__HP_cc)
#include <inttypes.h>
#elif defined (_MSC_VER) && _MSC_VER < 1600
    /* VS 2010 (_MSC_VER 1600) has stdint.h */
    typedef __int8 int8_t;
    typedef unsigned __int8 uint8_t;
    typedef __int16 int16_t;
    typedef unsigned __int16 uint16_t;
    typedef __int32 int32_t;
    typedef unsigned __int32 uint32_t;
    typedef __int64 int64_t;
    typedef unsigned __int64 uint64_t;
#elif defined (_AIX)
# include <sys/inttypes.h>
#else
# include <inttypes.h>
#endif


typedef uint64_t ub8;
#define UB8MAXVAL 0xffffffffffffffffLL
#define UB8BITS 64

typedef int64_t sb8;
#define SB8MAXVAL 0x7fffffffffffffffLL

/* unsigned 4-byte quantities */
typedef uint32_t ub4;
#define UB4MAXVAL 0xffffffff

typedef int32_t sb4;
#define UB4BITS 32
#define SB4MAXVAL 0x7fffffff

typedef uint16_t ub2;
#define UB2MAXVAL 0xffff
#define UB2BITS 16

typedef int16_t sb2;
#define SB2MAXVAL 0x7fff

/* unsigned 1-byte quantities */
typedef unsigned char ub1;
#define UB1MAXVAL 0xff
#define UB1BITS 8

/* signed 1-byte quantities */
typedef signed char sb1;
#define SB1MAXVAL 0x7f

#if defined (_MSC_VER)
    // warning C4996: 'vsnprintf': This function or variable may be unsafe.
    // Consider using vsnprintf_s instead.
    //  To disable deprecation, use _CRT_SECURE_NO_WARNINGS
    #pragma warning(disable:4996)
#endif

/**
 * snprintf_chkd_V1()
 *   A checked V1 version of snprintf() for both GCC and MSVC
 *   No error.
 * see:
 *   <stdarg.h>
 *   https://linux.die.net/man/3/snprintf
 *
 *   The functions snprintf() and vsnprintf() do not write more than size bytes
 *    (including the terminating null byte ('\0')).
 *   If the output was truncated due to this limit then the return value is the
 *    number of characters (excluding the terminating null byte) which would have
 *    been written to the final string if enough space had been available.
 *   Thus, a return value of size or more means that the output was truncated.
 */
static int snprintf_chkd_V1 (char *outputbuf, size_t bufsize, const char *format, ...)
{
    int len;

    va_list args;
    va_start(args, format);
    len = vsnprintf(outputbuf, bufsize, format, args);
    va_end(args);

    if (len < 0 || len >= (int) bufsize) {
        /* output was truncated due to bufsize limit */
        len = (int) bufsize - 1;

        /* for MSVC */
        outputbuf[len] = '\0';
    }

    return len;
}


/**
 * snprintf_chkd_V2()
 *   A crashed on error version of snprintf.
 *    
 *    If exitcode not given (= 0), same as snprintf_safe()
 */
static int snprintf_chkd_V2 (int exitcode, char *outputbuf, size_t bufsize, const char *format, ...)
{
    int len;

    va_list args;
    va_start(args, format);
    len = vsnprintf(outputbuf, bufsize, format, args);
    va_end(args);

    if (len < 0 || len >= (int) bufsize) {
        /* output was truncated due to bufsize limit */
        len = (int) bufsize - 1;

        /* for MSVC */
        outputbuf[len] = '\0';

        /* exit on error if exitcode given (not 0) */
        if (exitcode) {
            fprintf(stderr, "(%s:%d) fatal: output was truncated. (%s...)\n", __FILE__, __LINE__, outputbuf);
            exit(exitcode);
        }
    }

    return len;
}


#define snprintf_V1    snprintf

#ifdef __cplusplus
}
#endif

#endif /* UNITYPES_H_INCLUDED */
