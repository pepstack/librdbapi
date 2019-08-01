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
* 2019-07-01: revised
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

#ifdef __cplusplus
}
#endif

#endif /* UNITYPES_H_INCLUDED */
