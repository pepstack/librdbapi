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
* cstrutil.h
*
*   C String Utility
*
* 2019-08-01
*/
#ifndef _CSTRUTIL_H
#define _CSTRUTIL_H

#if defined(__cplusplus)
extern "C"
{
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory.h>
#include <errno.h>

#include "unitypes.h"


#define cstr_bool_true   1
#define cstr_bool_false  0


/**
 * trim specified character in given string
 */
static char * cstr_trim_chr (char * s, char c)
{
    return (*s==0)?s:(((*s!=c)?(((cstr_trim_chr(s+1,c)-1)==s)?s:(*(cstr_trim_chr(s+1,c)-1)=*s,*s=c,cstr_trim_chr(s+1,c))):cstr_trim_chr(s+1,c)));
}


static char * cstr_Ltrim_chr (char * str, char ch)
{
    char *p = str;
    while (*p && *p++ == ch) {
        str = p;
    }
    return str;
}


static char * cstr_Rtrim_chr (char * str, char ch)
{
    char *p = str;
    char *q = str;

    while (*p) {
        if (*p != ch) {
            q = p;            
        }

        p++;
    }

    if (++q != p) {
        *q = 0;
    }

    return str;
}


static char * cstr_LRtrim_chr (char * str, char c)
{
    return cstr_Rtrim_chr(cstr_Ltrim_chr(str, c), c);
}


static int cstr_to_dbl (const char *str, int slen, double *outval)
{
    if (slen == 0) {
        // null string
        return 0;
    } else {
        double val;
        char *endptr;

        /* To distinguish success/failure after call */
        errno = 0;

        val = strtod(str, &endptr);

        /* Check for various possible errors */
        if ((errno == ERANGE) || (errno != 0 && val == 0)) {
            // error: overflow or underflow
            return (-1);
        }

        if (endptr == str) {
            // No digits were found
            return 0;
        }

        /* success return */
        *outval = val;
        return 1;        
    }
}


/*
    =
    >
    >=
    <
    <=
    !=
    LIKE
    MATCH
*/

static int cstr_split_substr (char *str, const char *sub, int sublen, char **offs, int maxoffs)
{
    int i = 0;

    char *s = str;

    while (s && i < maxoffs) {
        char *p = strstr(s, sub);
        if (p) {
            *p = 0;
            p += sublen;
        }

        offs[i++] = s;

        s = p;
    }

    return i;
}


static int cstr_to_sb8 (int base, const char *str, int slen, sb8 *outval)
{
    if (slen == 0) {
        // null string
        return 0;
    } else {
        sb8 val;
        char *endptr;

        /* To distinguish success/failure after call */
        errno = 0;

        val = strtoll(str, &endptr, base);

        /* Check for various possible errors */
        if ((errno == ERANGE && (val == LLONG_MAX || val == LLONG_MIN)) || (errno != 0 && val == 0)) {
            // error
            return (-1);
        }

        if (endptr == str) {
            // No digits were found
            return (0);
        }

        /* success return */
        *outval = val;
        return 1;
    }
}



static int cstr_to_ub8 (int base, const char *str, int slen, ub8 *outval)
{
    if (slen == 0) {
        // null string
        return 0;
    } else {
        ub8 val;
        char *endptr;

        /* To distinguish success/failure after call */
        errno = 0;

        val = strtoull(str, &endptr, base);

        /* Check for various possible errors */
        if ((errno == ERANGE && (val == ULLONG_MAX || val == 0)) || (errno != 0 && val == 0)) {
            // error
            return (-1);
        }

        if (endptr == str) {
            // No digits were found
            return (0);
        }

        /* success return */
        *outval = val;
        return 1;
    }
}


static int cstr_notequal (const char *str1, const char *str2)
{
    if (str1 == str2) {
        return cstr_bool_false;
    }

    if (str1 && str2) {
        // str1 != str2
        return strcmp(str1, str2)? cstr_bool_true : cstr_bool_false;
    }

    // str1 != str2
    return cstr_bool_true;
}


static int cstr_notequal_len (const char *Astr, int Alen, const char *Bstr, int Blen)
{
    if (Alen != Blen) {
        // Astr != Bstr
        return cstr_bool_true;
    }

    if (Astr == Bstr) {
        return cstr_bool_false;
    }

    if (Astr && Bstr) {
        return strncmp(Astr, Bstr, Alen)? cstr_bool_true : cstr_bool_false;
    }

    // not eauql
    return cstr_bool_true;
}


/**
 * cstr_compare_len
 *   Safely compare two strings as strncmp(A, B) do
 *
 * returns:
 *    1: A > B
 *    0: A = B
 *   -1: A < B
 *
 * notes:
 *   1) null string is less than any non-null or empty one.
 *   2) shorter string is less than longer one.
 *   3) two null strings is equal (0 returned).
 */
static int cstr_compare_len (const char *Astr, int Alen, const char *Bstr, int Blen)
{
    if (Astr == Bstr) {
        return 0;
    }

    if (! Astr) {
        // A < B (B is non-null)
        return (-1);
    }

    if (! Bstr) {
        // A > B (B is null)
        return 1;
    }

    if (! Alen && ! Blen) {
        // A and B are all empty
        return 0;
    }

    if (Alen < 0 || Blen < 0) {
        // has invalid length
        return strcmp(Astr, Bstr);
    }

    if (Alen > Blen) {
        return 1;
    }

    if (Alen < Blen) {
        return -1;
    }
    
    // Alen == Blen
    return strncmp(Astr, Bstr, Alen);
}


/**
 * cstr_startwith("HelloWorld", 10, "Hello", 5) == cstr_bool_true
 * cstr_startwith("HelloWorld", 10, "World", 5) == cstr_bool_false
 * cstr_startwith("HelloWorld", 10, "hello", 5) == cstr_bool_false
 */
static int cstr_startwith (const char *str, int count, const char *start, int startlen)
{
    if (str == start) {
        return cstr_bool_true;
    }

    if (str && start && startlen <= count) {
        if (! memcmp(str, start, startlen)) {
            return cstr_bool_true;
        }
    }

    return cstr_bool_false;
}


static int cstr_endwith (const char *str, int count, const char *end, int endlen)
{
    if (str == end) {
        return cstr_bool_true;
    }

    if (str && end && endlen <= count) {
        //   str="aaaaBBBB"        
        //   end="aBBBB"
        return ! cstr_notequal_len(&str[count - endlen], endlen, end, endlen);
    }
   
    return cstr_bool_false;
}


static int cstr_containwith (const char *str, int count, const char *sub, int sublen)
{
    if (str == sub) {
        return cstr_bool_true;
    }

    if (str && sub && sublen <= count) {
        return strstr((char *)str, sub)? cstr_bool_true : cstr_bool_false;
    }

    return cstr_bool_false;
}

#ifdef __cplusplus
}
#endif

#endif /* _CSTRUTIL_H */
