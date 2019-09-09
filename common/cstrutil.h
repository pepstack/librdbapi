/***********************************************************************
* Copyright (c) 2008-2080 syna-tech.com, pepstack.com, 350137278@qq.comcstr_replace_new
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
* 2019-08-28
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
#include <ctype.h>
#include <errno.h>

#include "unitypes.h"


#define cstr_bool_true   1
#define cstr_bool_false  0


static void cstr_varray_free (char ** varr, int maxnum)
{
    char *p;

    int i = 0;
    while ((p = varr[i++]) != NULL) {
        free(p);

        if (i == maxnum) {
            break;
        }
    }
}


static char * cstr_toupper (char * s, int num)
{
    char *p = s;

    while (num-- > 0 && *p) {
       *p = toupper(*p);
        p++;
    }

    return s;
}


static char * cstr_tolower (char * s, int num)
{
    char *p = s;

    while (num-- > 0 && *p) {
       *p = tolower(*p);
        p++;
    }

    return s;
}


/**
 * trim specified character in given string
 */
static char * cstr_trim_chr (char * s, char c)
{
    return (*s==0)?s:(((*s!=c)?(((cstr_trim_chr(s+1,c)-1)==s)?s:(*(cstr_trim_chr(s+1,c)-1)=*s,*s=c,cstr_trim_chr(s+1,c))):cstr_trim_chr(s+1,c)));
}


static char * cstr_trim_chr_mul (char * str, const char * chrs, int num)
{
    char *s = str;
    while (num-- > 0) {
        char ch = chrs[num];
        s = cstr_trim_chr(s, ch);
    }
    return s;
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


static char * cstr_Lfind_chr (char * str, int len, char c)
{
    if (! str || len <= 0) {
        return NULL;
    } else {
        char *p = str;
        while ((int)(p - str) < len && *p) {
            if (*p == c) {
                return p;
            }
            p++;
        }
        return NULL;
    }
}


static char * cstr_Rfind_chr (char * str, int len, char c)
{
    if (! str || len <= 0) {
        return NULL;
    } else {
        char *p = &str[len-1];
        while (len-- > 0) {
            if (*p == c) {
                return p;
            }
            p--;
        }
        return NULL;
    }
}


static char * cstr_find_chrs (char * str, int len, const char *chrs, int nch)
{
    if (! str || len <= 0) {
        return NULL;
    } else {
        int i;
        char *p = str;

        while ((int)(p - str) < len && *p) {
            for (i = 0; i < nch; i++) {
                if (chrs[i] == *p) {
                    return p;
                }
            }

            p++;
        }

        return NULL;
    }
}


/**
 * int isspace(char c);
 *   Standard white-space characters are:
 *    ' '   (0x20)	space (SPC) arcii=32
 *    '\t'	(0x09)	horizontal tab (TAB)
 *    '\n'	(0x0a)	newline (LF)
 *    '\v'	(0x0b)	vertical tab (VT)
 *    '\f'	(0x0c)	feed (FF)
 *    '\r'	(0x0d)	carriage return (CR)
 */
static char * cstr_Ltrim_whitespace (char *str)
{
    char *p = str;
    while (p && isspace(*p)) {
        p++;
    }
    return p;
}


static int cstr_Rtrim_whitespace (char *str, int len)
{
    while (len-- > 0) {
        if (isspace(str[len])) {
            str[len] = 0;
        } else {
            break;
        }
    }
    return len + 1;
}


static char * cstr_replace_chr (char * str, char ch, char rpl)
{
    char *p = str;
    while (p && *p) {
        if (*p == ch) { 
            *p = rpl;
        }
        p++;
    }
    return str;
}


static int cstr_slpit_chr (const char * str, int len, char delim, char **outstrs, int maxoutnum)
{
    char *p;
    const char *s = str;

    int i = 0;

    int n = 1;
    while (s && (p = strchr(s, delim)) && (p < str +len)) {
        s = p+1;

        n++;
    }

    if (! outstrs) {
        // only to get count
        return n;
    }

    if (n > 0) {
        char *sb;

        char *s0 = malloc(len + 1);

        memcpy(s0, str, len);

        s0[len] = 0;

        sb = s0;
        while (sb && (p = strchr(sb, delim))) {
            *p++ = 0;

            if (i < maxoutnum) {
                // remove whitespaces
                outstrs[i++] = strdup(cstr_LRtrim_chr(sb, 32));
            } else {
                // overflow than maxoutnum
                break;
            }

            sb = p;
        }

        if (i < maxoutnum) {
            outstrs[i++] = strdup(cstr_LRtrim_chr(sb, 32));
        }

        free(s0);
    }

    return i;
}


static int cstr_replace_new (const char *original, const char *pattern, const char *replacement, char **outresult)
{
    size_t const replen = strlen(replacement);
    size_t const patlen = strlen(pattern);
    size_t const orilen = strlen(original);

    size_t patcnt = 0;

    const char * oriptr;
    const char * patloc;

    *outresult = 0;

    // find how many times the pattern occurs in the original string
    for (oriptr = original; patloc = strstr(oriptr, pattern); oriptr = patloc + patlen) {
        patcnt++;
    }

    if (patcnt) {
        // allocate memory for the new string
        size_t len = orilen + patcnt * (replen - patlen);

        char * result = (char *) malloc( sizeof(char) * (len + 1) );

        if (result) {
            // copy the original string, 
            // replacing all the instances of the pattern
            char * retptr = result;

            for (oriptr = original; patloc = strstr(oriptr, pattern); oriptr = patloc + patlen) {
                size_t const skplen = patloc - oriptr;

                // copy the section until the occurence of the pattern
                strncpy(retptr, oriptr, skplen);
                retptr += skplen;

                // copy the replacement 
                strncpy(retptr, replacement, replen);
                retptr += replen;
            }

            // copy the rest of the string.
            strcpy(retptr, oriptr);
        }

        *outresult = result;
        return (int) len;
    }

    return 0;
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


/**
 * cstr_split_substr
 *   split string by separator string (sep) into sub strings
 */
static int cstr_split_substr (char *str, const char *sep, int seplen, char **subs, int maxsubs)
{
    int i = 0;

    char *s = str;

    while (s && i < maxsubs) {
        char *p = strstr(s, sep);
        if (p) {
            *p = 0;
            p += seplen;
        }

        subs[i++] = s;

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


#define cstr_length(str, maxlen)    (str? ((maxlen)==-1? (int)strlen(str) : (int)strnlen(str, maxlen)) : 0)


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


static int cstr_startwith_mul (const char *str, int count, const char *starts[], const int *startslen, int startsnum)
{
    while (startsnum-- > 0) {
        const char *s = starts[startsnum];

        if (s) {
            int len = (startslen? startslen[startsnum] : (int) strlen(s));

            if (cstr_startwith(str, count, s, len)) {
                return startsnum;
            }
        }
    }

    return (-1);
}


static int cstr_endwith_mul (const char *str, int count, const char *ends[], const int *endslen, int endsnum)
{
    while (endsnum-- > 0) {
        const char *s = ends[endsnum];

        if (s) {
            int len = (endslen? endslen[endsnum] : (int) strlen(s));

            if (cstr_endwith(str, count, s, len)) {
                return endsnum;
            }
        }
    }

    return (-1);
}


static int cstr_findstr_in (const char *str, int count, const char *dests[], int destsnum)
{
    const char *dest;

    int len, i = 0;

    while ((dest = dests[i]) != NULL) {
        if (i == destsnum) {
            break;
        }

        len = cstr_length(dest, count + 1);
        if (len == count) {
            if (! strncmp(str, dest, count)) {
                return i;
            }
        }

        dest = dests[i++];
    }

    // not found
    return (-1);
}


static int cstr_readline (FILE *fp, char line[], size_t maxlen)
{
    int ch, len = 0;
    int whitespace = 1;

    while ((ch = fgetc(fp)) != EOF) {
        if (len < maxlen) {
            if (ch != '\r' && ch != '\n' && ch != '\\') {
                line[len++] = ch;
                if (ch != 32) {
                    whitespace = 0;
                }
            }
        }

        if (ch == '\n') {
            break;
        }
    }

    if (ch == EOF && len == 0) {
        // end of file
        return -1;
    }

    line[len] = 0;

    return (whitespace? 0 : len);
}


#ifdef __cplusplus
}
#endif

#endif /* _CSTRUTIL_H */
