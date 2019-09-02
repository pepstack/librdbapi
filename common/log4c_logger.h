/***********************************************************************
* Copyright (c) 2018 pepstack, pepstack.com
*
* This software is provided 'as-is', without any express or implied
* warranty.  In no event will the authors be held liable for any damages
* arising from the use of this software.
* Permission is granted to anyone to use this software for any purpose,
* including commercial applications, and to alter it and redistribute it
* freely, subject to the following restrictions:
*
* 1. The origin of this software must not be misrepresented; you must not
*   claim that you wrote the original software. If you use this software
*   in a product, an acknowledgment in the product documentation would be
*   appreciated but is not required.
*
* 2. Altered source versions must be plainly marked as such, and must not be
*   misrepresented as being the original software.
*
* 3. This notice may not be removed or altered from any source distribution.
***********************************************************************/

/**
 * log4c_logger.h
 *   - A wrapper for LOG4C
 *
 * create: 2014-08-01
 * update: 2018-09-28
 * fixbug: 2018-01-31   'buf' as temp variable will cause conflicts with other files
 * fixbug: 2018-09-28   add log4c_logger.c for export global variable: __logger_cat_global
 *
 * 2015-11-17: add color output
 * ------ color output ------
 * 30: black
 * 31: red
 * 32: green
 * 33: yellow
 * 34: blue
 * 35: purple
 * 36: dark green
 * 37: white
 * --------------------------
 * "\033[31m RED   \033[0m"
 * "\033[32m GREEN \033[0m"
 * "\033[33m YELLOW \033[0m"
 *
 * Linux time commands:
 *   http://blog.sina.com.cn/s/blog_4171e80d01010rtr.html
 */

#ifndef LOG4C_LOGGER_H_INCLUDED
#define LOG4C_LOGGER_H_INCLUDED

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>

#include <log4c.h>

#ifndef LOGGER_BUF_LEN
    #define LOGGER_BUF_LEN  4410
#endif


#ifdef LOGGER_CATEGORY_NAME
    #define LOGGER_CATEGORY_NAME_REAL  LOGGER_CATEGORY_NAME
#else
    #define LOGGER_CATEGORY_NAME_REAL  "root"
#endif

#ifndef LOGGER_NOCOLOR_OUTPUT
    #define LOGGER_COLOR_OUTPUT
#else
    #undef LOGGER_COLOR_OUTPUT
#endif


#ifdef LOGGER_LEVEL_TRACE
#  undef LOGGER_TRACE_DISABLED
#  undef LOGGER_DEBUG_DISABLED
#  undef LOGGER_INFO_DISABLED
#  undef LOGGER_WARN_DISABLED
#endif


#ifdef LOGGER_LEVEL_DEBUG
#  undef LOGGER_TRACE_DISABLED
#  undef LOGGER_DEBUG_DISABLED
#  undef LOGGER_INFO_DISABLED
#  undef LOGGER_WARN_DISABLED

#  define LOGGER_TRACE_DISABLED
#endif


#ifdef LOGGER_LEVEL_INFO
#  undef LOGGER_TRACE_DISABLED
#  undef LOGGER_DEBUG_DISABLED
#  undef LOGGER_INFO_DISABLED
#  undef LOGGER_WARN_DISABLED

#  define LOGGER_TRACE_DISABLED
#  define LOGGER_DEBUG_DISABLED
#endif

#ifdef LOGGER_LEVEL_NOTICE
#  undef LOGGER_TRACE_DISABLED
#  undef LOGGER_DEBUG_DISABLED
#  undef LOGGER_INFO_DISABLED
#  undef LOGGER_NOTICE_DISABLED

#  define LOGGER_TRACE_DISABLED
#  define LOGGER_DEBUG_DISABLED
#  define LOGGER_INFO_DISABLED
#endif

#ifdef LOGGER_LEVEL_WARN
#  undef LOGGER_TRACE_DISABLED
#  undef LOGGER_DEBUG_DISABLED
#  undef LOGGER_INFO_DISABLED
#  undef LOGGER_NOTICE_DISABLED
#  undef LOGGER_WARN_DISABLED

#  define LOGGER_TRACE_DISABLED
#  define LOGGER_DEBUG_DISABLED
#  define LOGGER_INFO_DISABLED
#  define LOGGER_NOTICE_DISABLED
#endif

#ifdef LOGGER_LEVEL_ERROR
#  undef LOGGER_TRACE_DISABLED
#  undef LOGGER_DEBUG_DISABLED
#  undef LOGGER_INFO_DISABLED
#  undef LOGGER_NOTICE_DISABLED
#  undef LOGGER_WARN_DISABLED

#  define LOGGER_TRACE_DISABLED
#  define LOGGER_DEBUG_DISABLED
#  define LOGGER_INFO_DISABLED
#  define LOGGER_NOTICE_DISABLED
#  define LOGGER_WARN_DISABLED
#endif


extern  log4c_category_t * __logger_cat_global;


__attribute__((unused))
static inline log4c_category_t * logger_get_cat (int priority)
{
    if (__logger_cat_global && log4c_category_is_priority_enabled(__logger_cat_global, priority)) {
        return __logger_cat_global;
    } else {
        return 0;
    }
}


__attribute__((unused))
static inline void logger_write (int priority, const char *file, int line, const char *func, const char * msg)
{
    log4c_category_log(__logger_cat_global, priority, "(%s:%d) <%s> %s", file, line, func, msg);
}


__attribute__((unused))
static void LOGGER_INIT ()
{
    if (0 != log4c_init()) {
        printf("\n**** log4c_init(%s) failed.\n", LOGGER_CATEGORY_NAME_REAL);
        printf("\n* This error might occur on Redhat Linux if you had installed expat before log4c.\n");
        printf("\n* To solve this issue you can uninstall both log4c and expat and reinstall them.\n");
        printf("\n* Please make sure log4c must be installed before expat installation on Redhat.\n");
        printf("\n* The other reasons cause failure are not included here, please refer to the manual.\n");
    } else {
        __logger_cat_global = log4c_category_get(LOGGER_CATEGORY_NAME_REAL);

        printf("\n* log4c_init(logger:%p category:%s) success.\n", __logger_cat_global, LOGGER_CATEGORY_NAME_REAL);
    }
}


__attribute__((unused))
static void LOGGER_FINI ()
{
    printf("\n* log4c_fini.\n");
    log4c_fini();
}


/**
 * LOGGER_TRACE()
 */
#if defined(LOGGER_TRACE_UNKNOWN)
#   define LOGGER_UNKNOWN(message, args...)    do {} while (0)
#else
    #define LOGGER_UNKNOWN(message, args...)  \
        if (logger_get_cat(LOG4C_PRIORITY_UNKNOWN)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN+1]; \
            snprintf (__logger_buf_tmp, LOGGER_BUF_LEN, message, ##args); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_UNKNOWN, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#endif


#if defined(LOGGER_TRACE_DISABLED)
#   define LOGGER_TRACE(message, args...)    do {} while (0)
#else
#   define LOGGER_TRACE(message, args...)  \
    if (logger_get_cat(LOG4C_PRIORITY_TRACE)) { \
        char __logger_buf_tmp[LOGGER_BUF_LEN+1]; \
        snprintf(__logger_buf_tmp, LOGGER_BUF_LEN, message, ##args); \
        __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
        logger_write (LOG4C_PRIORITY_TRACE, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
    }
#endif


/**
 * LOGGER_DEBUG()
 */
#if defined(LOGGER_DEBUG_DISABLED)
#   define LOGGER_DEBUG(message, args...)    do {} while (0)
#else
#if defined(LOGGER_COLOR_OUTPUT)
    #define LOGGER_DEBUG(message, args...)  \
        if (logger_get_cat(LOG4C_PRIORITY_DEBUG)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN + 1]; \
            char *__psz_logger_buf = __logger_buf_tmp; \
            __psz_logger_buf = __logger_buf_tmp + snprintf(__psz_logger_buf, 20, "\033[36m"); \
            __psz_logger_buf = __psz_logger_buf + snprintf(__psz_logger_buf, LOGGER_BUF_LEN - 20, message, ##args); \
            snprintf(__psz_logger_buf, 20, "\033[0m"); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_DEBUG, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#else
    #define LOGGER_DEBUG(message, args...)  \
        if (logger_get_cat(LOG4C_PRIORITY_DEBUG)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN+1]; \
            snprintf (__logger_buf_tmp, LOGGER_BUF_LEN, message, ##args); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_DEBUG, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#endif
#endif


/**
 * LOGGER_INFO()
 */
#if defined(LOGGER_INFO_DISABLED)
    #define LOGGER_INFO(message, args...)    do {} while (0)
#else
#if defined(LOGGER_COLOR_OUTPUT)
    #define LOGGER_INFO(message, args...)  \
        if (logger_get_cat(LOG4C_PRIORITY_INFO)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN + 1]; \
            char *__psz_logger_buf = __logger_buf_tmp; \
            __psz_logger_buf = __logger_buf_tmp + snprintf(__psz_logger_buf, 20, "\033[32m"); \
            __psz_logger_buf = __psz_logger_buf + snprintf(__psz_logger_buf, LOGGER_BUF_LEN - 20, message, ##args); \
            snprintf(__psz_logger_buf, 20, "\033[0m"); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_INFO, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#else
    #define LOGGER_INFO(message, args...)  \
        if (logger_get_cat(LOG4C_PRIORITY_INFO)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN+1]; \
            snprintf (__logger_buf_tmp, LOGGER_BUF_LEN, message, ##args); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_INFO, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#endif
#endif


/**
 * LOGGER_NOTICE()
 */
#if defined(LOGGER_NOTICE_DISABLED)
    #define LOGGER_NOTICE(message, args...)    do {} while (0)
#else
#if defined(LOGGER_COLOR_OUTPUT)
    #define LOGGER_NOTICE(message, args...)  \
        if (logger_get_cat(LOG4C_PRIORITY_NOTICE)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN + 1]; \
            char *__psz_logger_buf = __logger_buf_tmp; \
            __psz_logger_buf = __logger_buf_tmp + snprintf(__psz_logger_buf, 20, "\033[1;36m"); \
            __psz_logger_buf = __psz_logger_buf + snprintf(__psz_logger_buf, LOGGER_BUF_LEN - 20, message, ##args); \
            snprintf(__psz_logger_buf, 20, "\033[0m"); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_NOTICE, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#else
    #define LOGGER_NOTICE(message, args...)  \
        if (logger_get_cat(LOG4C_PRIORITY_NOTICE)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN+1]; \
            snprintf (__logger_buf_tmp, LOGGER_BUF_LEN, message, ##args); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_NOTICE, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#endif
#endif


/**
 * LOGGER_WARN()
 */
#if defined(LOGGER_WARN_DISABLED)
#   define LOGGER_WARN(message, args...)    do {} while (0)
#else
#if defined(LOGGER_COLOR_OUTPUT)
    #define LOGGER_WARN(message, args...)  \
        if (logger_get_cat(LOG4C_PRIORITY_WARN)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN + 1]; \
            char *__psz_logger_buf = __logger_buf_tmp; \
            __psz_logger_buf = __logger_buf_tmp + snprintf(__psz_logger_buf, 20, "\033[33m"); \
            __psz_logger_buf = __psz_logger_buf + snprintf(__psz_logger_buf, LOGGER_BUF_LEN - 20, message, ##args); \
            snprintf(__psz_logger_buf, 20, "\033[0m"); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_WARN, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#else
    #define LOGGER_WARN(message, args...)  \
        if (logger_get_cat(LOG4C_PRIORITY_WARN)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN+1]; \
            snprintf (__logger_buf_tmp, LOGGER_BUF_LEN, message, ##args); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_WARN, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#endif
#endif


/**
 * LOGGER_ERROR()
 */
#if defined(LOGGER_ERROR_DISABLED)
    #define LOGGER_ERROR(message, args...)    do {} while (0)
#else
#if defined(LOGGER_COLOR_OUTPUT)
    #define LOGGER_ERROR(message, args...)    \
        if (logger_get_cat(LOG4C_PRIORITY_ERROR)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN + 1]; \
            char *__psz_logger_buf = __logger_buf_tmp; \
            __psz_logger_buf = __logger_buf_tmp + snprintf(__psz_logger_buf, 20, "\033[31m"); \
            __psz_logger_buf = __psz_logger_buf + snprintf(__psz_logger_buf, LOGGER_BUF_LEN - 20, message, ##args); \
            snprintf(__psz_logger_buf, 20, "\033[0m"); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_ERROR, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#else
    #define LOGGER_ERROR(message, args...)  \
        if (logger_get_cat(LOG4C_PRIORITY_ERROR)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN+1]; \
            snprintf (__logger_buf_tmp, LOGGER_BUF_LEN, message, ##args); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_ERROR, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#endif
#endif


/**
 * LOGGER_FATAL()
 */
#if defined(LOGGER_FATAL_DISABLED)
    #define LOGGER_FATAL(message, args...)    do {} while (0)
#else
#if defined(LOGGER_COLOR_OUTPUT)
    #define LOGGER_FATAL(message, args...)  \
        if (logger_get_cat(LOG4C_PRIORITY_FATAL)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN + 1]; \
            char *__psz_logger_buf = __logger_buf_tmp; \
            __psz_logger_buf = __logger_buf_tmp + snprintf(__psz_logger_buf, 20, "\033[31m"); \
            __psz_logger_buf = __psz_logger_buf + snprintf(__psz_logger_buf, LOGGER_BUF_LEN - 20, message, ##args); \
            snprintf(__psz_logger_buf, 20, "\033[0m"); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_FATAL, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#else
    #define LOGGER_FATAL(message, args...)  \
        if (logger_get_cat(LOG4C_PRIORITY_FATAL)) { \
            char __logger_buf_tmp[LOGGER_BUF_LEN+1]; \
            snprintf (__logger_buf_tmp, LOGGER_BUF_LEN, message, ##args); \
            __logger_buf_tmp[LOGGER_BUF_LEN] = 0; \
            logger_write (LOG4C_PRIORITY_FATAL, __FILE__, __LINE__, __FUNCTION__, __logger_buf_tmp); \
        }
#endif
#endif


#ifndef LOGGER_TRACE0
#  define LOGGER_TRACE0()    LOGGER_TRACE("-")
#endif

#endif /* LOG4C_LOGGER_H_INCLUDED */
