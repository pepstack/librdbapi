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
 * @file: memapi.h
 *
 *   memory helper api both for linux and windows
 *
 * @author: master@pepstack.com
 *
 * @create: 2018-10-25
 *
 * @update: 2019-06-17
 */
#ifndef MEMAPI_H_INCLUDED
#define MEMAPI_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#if (defined(WIN32) || defined(_WIN16) || defined(_WIN32) || defined(_WIN64)) && !defined(__WINDOWS__)
# define __WINDOWS__
#endif

#include <assert.h>  /* assert */
#include <string.h>  /* memset */
#include <stdio.h>   /* printf, perror */
#include <limits.h>  /* realpath, PATH_MAX=4096 */
#include <stdbool.h> /* memset */
#include <ctype.h>
#include <stdlib.h>  /* malloc, alloc */
#include <errno.h>

#include "exitlog.h"

#if defined (__WINDOWS__)
/* Microsoft Windows */
# pragma warning(push)
# pragma warning(disable : 4996)
# define __INLINE__

#else
/* Linux */
# define __INLINE__ inline

# ifdef MEMAPI_USE_LIBJEMALLOC
/* need to link: libjemalloc.a */
#  include <jemalloc/jemalloc.h>
# endif

#endif


/**
 * mem_alloc_zero() allocates memory for an array of nmemb elements of
 *  size bytes each and returns a pointer to the allocated memory.
 * THE MEMORY IS SET TO ZERO.
 */
static __INLINE__ void * mem_alloc_zero (int nmemb, size_t size)
{
    void * ptr =
    #ifdef MEMAPI_USE_LIBJEMALLOC
        je_calloc(nmemb, size);

        exitlog_oom_check(ptr, "je_calloc@memapi.h");        
    #else
        calloc(nmemb, size);

        exitlog_oom_check(ptr, "calloc@memapi.h");
    #endif
    return ptr;
}


/**
 * mem_alloc_unset() allocate with THE MEMORY NOT BE INITIALIZED.
 */
static __INLINE__ void * mem_alloc_unset (size_t size)
{
    void * ptr =
    #ifdef MEMAPI_USE_LIBJEMALLOC
        je_malloc(size);

        exitlog_oom_check(ptr, "je_malloc@memapi.h");
    #else
        malloc(size);

        exitlog_oom_check(ptr, "malloc@memapi.h");
    #endif
    return ptr;

}


/**
 * mem_realloc() changes the size of the memory block pointed to by ptr
 *  to size bytes. The contents will be unchanged in the range from the
 *  start of the region up to the minimum of the old and new sizes.
 * If the new size is larger than the old size,
 *  THE ADDED MEMORY WILL NOT BE INITIALIZED.
 */
static __INLINE__ void * mem_realloc (void * ptr, size_t size)
{
    void *newptr =
    #ifdef MEMAPI_USE_LIBJEMALLOC 
        je_realloc(ptr, size);

        exitlog_oom_check(newptr, "je_realloc@memapi.h");
    #else
        realloc(ptr, size);

        exitlog_oom_check(newptr, "realloc@memapi.h");
    #endif

    return newptr;
}


/**
 * mem_free() frees the memory space pointed to by ptr, which must have
 *  been returned by a previous call to malloc(), calloc() or realloc().
 *  IF PTR IS NULL, NO OPERATION IS PERFORMED. 
 */
static __INLINE__ void mem_free (void * ptr)
{
    if (ptr) {
    #ifdef MEMAPI_USE_LIBJEMALLOC
        je_free(ptr);
    #else
        free(ptr);
    #endif
    }
}


/**
 * mem_free_s() frees the memory pointed by the address of ptr and set
 *  ptr to zero. it is a safe version if mem_free().
 */
static __INLINE__ void mem_free_s (void **pptr)
{
    if (pptr) {
        void *ptr = *pptr;

        if (ptr) {
            *pptr = 0;

        #ifdef MEMAPI_USE_LIBJEMALLOC
            je_free(ptr);
        #else
            free(ptr);
        #endif

        }
    }
}

#if defined (_MSC_VER)
# pragma warning(pop)
#endif

#ifdef __cplusplus
}
#endif

#endif /* MEMAPI_H_INCLUDED */
