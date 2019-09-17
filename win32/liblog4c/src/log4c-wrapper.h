/**
 * log4c-wrapper.h
 *
 *   WITH_LOG4C, LOG4C_ENABLED, LOG4C_CATEGORY_NAME
 *     should be defined before including this file.
 */
#ifndef _LOG4C_WRAPPER_H_
#define _LOG4C_WRAPPER_H_

#ifdef __cplusplus
extern "C" {
#endif


#if (LOG4C_ENABLED == 1)
    #undef LOG4C_DISABLED

	#include <log4c-api.h>
	#include <stdarg.h>


    #ifndef LOG4C_CATEGORY_NAME_DEFAULT
		#define LOG4C_CATEGORY_NAME_DEFAULT  "root"
	#endif

	#ifdef LOG4C_CATEGORY_NAME
		#define LOG4C_CATEGORY_NAME_REAL  LOG4C_CATEGORY_NAME
	#else
		#define LOG4C_CATEGORY_NAME_REAL  LOG4C_CATEGORY_NAME_DEFAULT
	#endif


	static void log4c_proxy(int a_priority, const char* a_format, ...)
	{
		va_list va;
		va_start(va,a_format);
		log4c_category_vlog_name(LOG4C_CATEGORY_NAME_REAL,a_priority,a_format,va);
		va_end(va);
	}

    static void log4c_trace (int priority, const char *file, int line, const char *fun, const char *a_format, ...)
    {
        char new_fmt[1000];
        const char *head_fmt = "[ %s(%d) : %s ] ";
        va_list ap;
        int n;
        const char *p, *q;

        p = strrchr(file, '\\');
        q = strrchr(file, '/');

        if (p > q) {
            n = sprintf(new_fmt, head_fmt , ++p, line , fun);
        } else if (q > p) {
            n = sprintf(new_fmt, head_fmt , ++q, line , fun);
        } else {
            n = sprintf(new_fmt, head_fmt , file, line , fun);
        }

        strcat(new_fmt + n , a_format);
        va_start(ap , a_format);
        log4c_category_vlog_name(LOG4C_CATEGORY_NAME_REAL, priority, new_fmt, ap);
        va_end(ap);
    }


    #define LOG4C_INIT                log4c_init
    #define LOG4C_FINI                log4c_fini
    #define LOG4C_MSG	              log4c_proxy

    #if !defined(_MSC_VER)
        #if (LOG4C_USING_TRACE == 1)
            #define LOG4C_CRITICAL(fmt, args...)  log4c_trace(LOG4C_PRIORITY_CRIT, __FILE__, __LINE__, __FUNCTION__, fmt, ##args)
            #define LOG4C_ERROR(fmt, args...)     log4c_trace(LOG4C_PRIORITY_ERROR, __FILE__, __LINE__, __FUNCTION__, fmt, ##args)
            #define LOG4C_WARN(fmt, args...)      log4c_trace(LOG4C_PRIORITY_WARN, __FILE__, __LINE__, __FUNCTION__, fmt , ##args)
            #define LOG4C_INFO(fmt, args...)      log4c_trace(LOG4C_PRIORITY_INFO, __FILE__, __LINE__, __FUNCTION__, fmt , ##args)
            #define LOG4C_DEBUG(fmt, args...)     log4c_trace(LOG4C_PRIORITY_DEBUG, __FILE__, __LINE__, __FUNCTION__, fmt , ##args)
            #define LOG4C_TRACE(fmt, args...)     log4c_trace(LOG4C_PRIORITY_TRACE, __FILE__, __LINE__, __FUNCTION__, fmt, ##args)
        #else
            #define LOG4C_CRITICAL(fmt, args...)  log4c_proxy(LOG4C_PRIORITY_CRIT, fmt, ##args)
            #define LOG4C_ERROR(fmt, args...)     log4c_proxy(LOG4C_PRIORITY_ERROR, fmt, ##args)
            #define LOG4C_WARN(fmt, args...)      log4c_proxy(LOG4C_PRIORITY_WARN, fmt , ##args)
            #define LOG4C_INFO(fmt, args...)      log4c_proxy(LOG4C_PRIORITY_INFO, fmt , ##args)
            #define LOG4C_DEBUG(fmt, args...)     log4c_proxy(LOG4C_PRIORITY_DEBUG, fmt , ##args)
            #define LOG4C_TRACE(fmt, args...)     log4c_proxy(LOG4C_PRIORITY_TRACE, fmt , ##args)
        #endif

        #define LOG4C_TRACE0(args...)  log4c_trace(LOG4C_PRIORITY_TRACE, __FILE__, __LINE__, __FUNCTION__, "", ##args)
    #elif _MSC_VER>=1400
        // VC80
        #if (LOG4C_USING_TRACE == 1)
            #define LOG4C_CRITICAL(fmt, ...)  log4c_trace(LOG4C_PRIORITY_CRIT, __FILE__, __LINE__, __FUNCTION__, fmt, __VA_ARGS__)
            #define LOG4C_ERROR(fmt, ...)     log4c_trace(LOG4C_PRIORITY_ERROR, __FILE__, __LINE__, __FUNCTION__, fmt, __VA_ARGS__)
            #define LOG4C_WARN(fmt, ...)      log4c_trace(LOG4C_PRIORITY_WARN, __FILE__, __LINE__, __FUNCTION__, fmt , __VA_ARGS__)
            #define LOG4C_INFO(fmt, ...)      log4c_trace(LOG4C_PRIORITY_INFO, __FILE__, __LINE__, __FUNCTION__, fmt , __VA_ARGS__)
            #define LOG4C_DEBUG(fmt, ...)     log4c_trace(LOG4C_PRIORITY_DEBUG, __FILE__, __LINE__, __FUNCTION__, fmt , __VA_ARGS__)
            #define LOG4C_TRACE(fmt, ...)     log4c_trace(LOG4C_PRIORITY_TRACE, __FILE__, __LINE__, __FUNCTION__, fmt, __VA_ARGS__)
        #else
            #define LOG4C_CRITICAL(fmt, ...)  log4c_proxy(LOG4C_PRIORITY_CRIT, fmt, __VA_ARGS__)
            #define LOG4C_ERROR(fmt, ...)     log4c_proxy(LOG4C_PRIORITY_ERROR, fmt, __VA_ARGS__)
            #define LOG4C_WARN(fmt, ...)      log4c_proxy(LOG4C_PRIORITY_WARN, fmt , __VA_ARGS__)
            #define LOG4C_INFO(fmt, ...)      log4c_proxy(LOG4C_PRIORITY_INFO, fmt , __VA_ARGS__)
            #define LOG4C_DEBUG(fmt, ...)     log4c_proxy(LOG4C_PRIORITY_DEBUG, fmt , __VA_ARGS__)
            #define LOG4C_TRACE(fmt, ...)     log4c_proxy(LOG4C_PRIORITY_TRACE, fmt, __VA_ARGS__)
        #endif

        #define LOG4C_TRACE0(...)  log4c_trace(LOG4C_PRIORITY_TRACE, __FILE__, __LINE__, __FUNCTION__, "-", __VA_ARGS__)
    #else
        #error "unsupport compiler"
    #endif
    
#else
    #ifndef LOG4C_DISABLED
        #define LOG4C_DISABLED
    #endif

    #define LOG4C_INIT()           1
	#define LOG4C_FINI()	       1

    #if !defined(_MSC_VER)
        #define LOG4C_MSG(argss...)	   1
        #define LOG4C_CRITICAL(args...)  1
        #define LOG4C_ERROR(args...)     1
        #define LOG4C_WARN(args...)      1
        #define LOG4C_INFO(args...)      1
        #define LOG4C_DEBUG(args...)     1
        #define LOG4C_TRACE(args...)     1
        #define LOG4C_TRACE0(args...)     1
    #elif _MSC_VER>=1400
        // VC80
        #define LOG4C_MSG(...)	   1
        #define LOG4C_CRITICAL(...)  1
        #define LOG4C_ERROR(...)     1
        #define LOG4C_WARN(...)      1
        #define LOG4C_INFO(...)      1
        #define LOG4C_DEBUG(...)     1
        #define LOG4C_TRACE(...)     1
        #define LOG4C_TRACE0(...)    1
    #else
        #error "unsupport compiler"
    #endif

#endif /* LOG4C_ENABLED */


#ifdef __cplusplus
};
#endif


#endif /* _LOG4C_WRAPPER_H_ */
