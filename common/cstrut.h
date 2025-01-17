﻿/***********************************************************************
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
* cstrut.h
*
*   C String Utility Functions
*
* author: 350137278@qq.com
*
* create: 2017-08-28
* update: 2019-09-18
*/
#ifndef _CSTR_UT_H
#define _CSTR_UT_H

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
#include <time.h>

#include "unitypes.h"


#define cstr_bool_true   1
#define cstr_bool_false  0

#define cstr_length(str, maxlen)    (str? ((maxlen)==-1? (int)strlen(str) : (int)strnlen(str, maxlen)) : 0)


/**********************************************************************
 * zstringbuf
 *   Zero ended ansi String Buffer api
 *********************************************************************/
#define zstringbuf_err_size        ((ub4)(-1))
#define zstringbuf_len_max         536870911
#define zstringbuf_size_max        (zstring_length_max + 1)

#define zstringbuf_blksize(len)    ((ub4) (((len + 256) / 256)  * 256))

#define zstringbufGetLen(zs)        ((zs)? (zs)->len : 0)
#define zstringbufGetMaxsz(zs)      ((zs)? (zs)->maxsz : 0)
#define zstringbufGetStr(zs)        ((zs)? (zs)->str : NULL)

#define zstringbufPrint(zs)  printf("%.*s", zs->len, zs->str)


typedef struct _zstringbuf_t
{
    ub4 maxsz;
    ub4 len;
    char str[0];
} zstringbuf_t, *zstringbuf;


static zstringbuf zstringbufNew (ub4 maxsz, const char *str, ub4 len)
{
    zstringbuf_t *pzs;

    if (len == zstringbuf_err_size) {
        len = (ub4) cstr_length(str, zstringbuf_len_max);
    }

    if (maxsz == zstringbuf_err_size || maxsz <= len) {
        maxsz = zstringbuf_blksize(len);
    } else {
        maxsz = zstringbuf_blksize(maxsz);
    }

    pzs = (zstringbuf_t *) calloc(1, sizeof(*pzs) + maxsz);
    if (! pzs) {
        fprintf(stderr, "(%s:%d) out of memory.\n", __FILE__, __LINE__);
        exit(-1);
    }

    if (str) {
        memcpy(pzs->str, str, len);
        pzs->len = len;
    }

    pzs->maxsz = maxsz;
    return pzs;
}


static void zstringbufFree (zstringbuf *pzstr)
{
    if (pzstr) {
        zstringbuf zstr = *pzstr;
        if (zstr) {
            *pzstr = NULL;
            free(zstr);
        }
    }
}


static zstringbuf zstringbufCat (zstringbuf zs, const char *fmt, ...)
{
    int len;

    if (! zs) {
        zs = zstringbufNew(zstringbuf_blksize(0), NULL, 0);
    }

    do {
        va_list args;
        va_start(args, fmt);
        len = vsnprintf(&zs->str[zs->len], zs->maxsz - zs->len, fmt, args);
        va_end(args);
    } while(0);

    if (len < 0) {
        zs->str[zs->len] = 0;
        return zs;
    }

    if (zs->maxsz > len + zs->len) {
        zs->len += len;
        return zs;
    } else {
        ub4 newmaxsz = zstringbuf_blksize(len + zs->len);

        zstringbuf_t *newzs = (zstringbuf_t *) realloc(zs, newmaxsz);

        if (! newzs) {
            fprintf(stderr, "(%s:%d) out of memory.\n", __FILE__, __LINE__);
            exit(-1);
        }

        do {
            va_list args;
            va_start(args, fmt);
            len = vsnprintf(&newzs->str[newzs->len], newmaxsz - newzs->len, fmt, args);
            va_end(args);
        } while(0);

        if (len < 0) {
            newzs->maxsz = newmaxsz;
            newzs->str[newzs->len] = 0;
            return newzs;
        }

        if (newmaxsz > len + newzs->len) {
            newzs->len += len;
            newzs->maxsz = newmaxsz;
            return newzs;
        }

        fprintf(stderr, "(%s:%d) SHOULD NEVER RUN TO THIS!", __FILE__, __LINE__);
        free(newzs);
        return NULL;
    }
}


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


static char* cstr_Rtrim_chr (char * str, char ch, int *outlen)
{
    char *p = str;
    char *q = p;

    while (*p) {
        if (*p != ch) {
            q = p;            
        }

        p++;
    }

    if (++q <= p) {
        *q = 0;

        if (outlen) {
            *outlen = (int)(q - str);
        }
    } else {
        if (outlen) {
            *outlen = 0;
        }
    }

    return str;
}


#define cstr_LRtrim_chr(str, c, outlen)  cstr_Rtrim_chr(cstr_Ltrim_chr((str), (c)), (c), (outlen))


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


static char * cstr_LRtrim_whitespace (char *str)
{
    char *p = cstr_Ltrim_whitespace(str);
    cstr_Rtrim_whitespace(p, cstr_length(p, -1));
    return p;
}


static int cstr_shrink_whitespace (const char *str, int *start, int *end)
{
    int s = *start;
    int e = *end;

    for (; s < *end; s++) {
        if (! isspace(str[s])) {
            break;
        }
    }
    *start = s;

    for (; e >= *start; e--) {
        if (! isspace(str[e])) {
            break;
        }
    }
    *end = e;
    
    return (*end - *start + 1);
}


static char * cstr_trim_whitespace (char * s)
{
    return (*s==0)?s:((( ! isspace(*s) )?(((cstr_trim_whitespace(s+1)-1)==s)? s : (*(cstr_trim_whitespace(s+1)-1)=*s, *s=32 ,cstr_trim_whitespace(s+1))):cstr_trim_whitespace(s+1)));
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


static int cstr_slpit_chr (const char * str, int len, char delim, char **outstrs, int outstrslen[], int maxoutnum)
{
    char *p;
    const char *s = str;

    int outlen;
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
                outlen = 0;
                outstrs[i] = strdup( cstr_LRtrim_chr(sb, 32, &outlen) );
                if (outstrslen) {
                    outstrslen[i] = outlen;
                }
                i++;
            } else {
                // overflow than maxoutnum
                break;
            }

            sb = p;
        }

        if (i < maxoutnum) {
            outlen = 0;
            outstrs[i] = strdup( cstr_LRtrim_chr(sb, 32, &outlen) );
            if (outstrslen) {
                outstrslen[i] = outlen;
            }
            i++;
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
static int cstr_split_substr (const char *str, const char *sepstr, int seplen, char **subs, int maxsubs)
{
    int i = 0;

    char *s = (char*) str;

    while (s && i < maxsubs) {
        char *p = strstr(s, sepstr);
        if (p) {
            *p = 0;
            p += seplen;
        }

        subs[i++] = s;

        s = p;
    }

    return i;
}


static int cstr_split_multi_chrs (const char *str, int slen, const char *sepchrs, int count, char **outsubs, int outsubslen[], int maxsubs)
{
    char *sub;
    int substart, subend, sublen;

    int k;
    int i = 0;
    int len = 0;

    int start = i;
    int end = start;

    for (; i < slen; i++) {
        for (k = 0; k < count; k++) {
            if (str[i] == sepchrs[k]) {
                end = i;
                break;
            }
        }

        if (k < count && end > start && len < maxsubs) {
            substart = start;
            subend = end - 1;

            sublen = cstr_shrink_whitespace(str, &substart, &subend);

            if (sublen > 0) {
                sub = (char *) malloc(sublen + 1);
                if (! sub) {
                    /* no memory */
                    exit(EXIT_FAILURE);
                }
                memcpy(sub, str + substart, sublen);
                sub[sublen] = 0;

                outsubs[len] = sub;
                outsubslen[len] = sublen;

                if (++len == maxsubs) {
                    return len;
                }
            }

            start = end;
        }
    }

    end = slen;

    if (len < maxsubs && end > start) {
        substart = start;
        subend = end - 1;

        sublen = cstr_shrink_whitespace(str, &substart, &subend);

        if (sublen > 0) {
            sub = (char *) malloc(sublen + 1);
            if (! sub) {
                /* no memory */
                exit(EXIT_FAILURE);
            }
            memcpy(sub, str + substart, sublen);
            sub[sublen] = 0;

            outsubs[len] = sub;
            outsubslen[len] = sublen;

            len++;
        }
    }

    return len;
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


static int cstr_isdigit (const char *str, int len)
{
    while(len-- > 0) {
        if (! isdigit(str[len])) {
            return cstr_bool_false;
        }
    }

    return cstr_bool_true;
}


static int cstr_readline (FILE *fp, char line[], size_t maxlen, int ignore_whitespace)
{
    int ch, len = 0;

    if (ftell(fp) == 0) {
        int bomhead[3] = {fgetc(fp), fgetc(fp), fgetc(fp)};
        if (bomhead[0] == 0xEF && bomhead[1] == 0xBB && bomhead[2] == 0xBF) {
            fseek(fp, 3, SEEK_SET);
        } else {
            fseek(fp, 0, SEEK_SET);
        }
    }

    while ((ch = fgetc(fp)) != EOF) {
        if (len < maxlen) {
            if (ch != '\r' && ch != '\n' && ch != '\\') {
                if (! ignore_whitespace || ! isspace(ch)) {
                    line[len++] = ch;
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
    return len;
}


#define is_leap_year(year) \
    ((((year) % 400 == 0) || ((year) % 4 == 0 && (year) % 100 != 0)) ? 1 : 0)


/* 0: error; 1: ok */
static int time_is_valid (int year, int mon, int day, int hour, int min, int sec)
{
    if (year < 1900 || year > 9999 || mon <= 0 || mon > 12 || day <= 0 || day > 31 || hour < 0 || hour > 24 || min < 0 || min > 59 || sec < 0 || sec > 60) {
        return 0;
    }

    // The number of seconds after the minute, normally in the range 0 to 59,
    //  but can be up to 60 to allow for leap
    if (! is_leap_year(year) && sec == 60) {
        return 0;
    }

    if (mon == 1 || mon == 3 || mon == 5 || mon == 7 || mon == 8 || mon == 10 || mon == 12) {
        // 31 days ok
        return 1;
    } else if (mon == 4 || mon == 6 || mon == 9 || mon == 11) {
        if (day < 31) {
            return 1;
        }
    } else {
        // mon=2
        return (day > 29? 0 : (day < 29? 1: (is_leap_year(year)? 1 : 0)));
    }
    return 0;
}


static ub8 cstr_parse_timestamp (char *timestr)
{
    /**
     * '2019-12-22 12:36:59.065'
     * '2019-12-22 12:36:59'
     * '2019-12-22'
     */
    char Year[5] = {'0', '0', '0', '0', '\0'};
    char Mon[3] = {'0', '0', '\0'};
    char Day[3] = {'0', '0', '\0'};

    char hour[3] = {'0', '0', '\0'};
    char min[3] = {'0', '0', '\0'};
    char sec[3] = {'0', '0', '\0'};

    char msec[4] = {'0', '0', '0', '\0'};

    char *str = cstr_LRtrim_whitespace(timestr);

    char *a = strchr(str, 39);
    char *b = strrchr(str, 39);

    char *hms;

    int len = 0;

    if (a && b) {
        *a++ = 0;
        *b-- = 0;
        str = a;
        len = (int)(b - a) + 1;
    } else if (a || b) {
        // error char
        return (-1);
    } else {
        len = cstr_length(str, 30);
    }

    if (len == 10) {
        /* 2019-12-22 */
        a = strchr(str, '-');
        b = strrchr(str, '-');

        if (a && b && a - str == 4 && b-a == 3) {
            *a++ = 0;
            *b++ = 0;

            snprintf(Year, sizeof(Year), "%.*s", 4, str);
            snprintf(Mon, sizeof(Mon), "%.*s", 2, a);
            snprintf(Day, sizeof(Day), "%.*s", 2, b);
        } else {
            // error date format
            return (-1);
        }
    } else if (len == 19) {
        /* 2019-12-22 12:36:59 */
        a = strchr(str, 32);
        if (a && a - str == 10) {
            *a++ = 0;

            hms = a;

            a = strchr(str, '-');
            b = strrchr(str, '-');

            if (a && b && a - str == 4 && b-a == 3) {
                *a++ = 0;
                *b++ = 0;

                snprintf(Year, sizeof(Year), "%.*s", 4, str);
                snprintf(Mon, sizeof(Mon), "%.*s", 2, a);
                snprintf(Day, sizeof(Day), "%.*s", 2, b);
            } else {
                // error date format
                return (-1);
            }

            a = strchr(hms, ':');
            b = strrchr(hms, ':');

            if (a && b && a - hms == 2 && b-a == 3) {
                *a++ = 0;
                *b++ = 0;

                snprintf(hour, sizeof(hour), "%.*s", 2, hms);
                snprintf(min, sizeof(min), "%.*s", 2, a);
                snprintf(sec, sizeof(sec), "%.*s", 2, b);
            } else {
                // error date format
                return (-1);
            }
        } else {
            // error datetime format
            return (-1);
        }
    } else if (len == 23) {
        /* 2019-12-22 12:36:59.065 */
        a = strchr(str, 32);
        if (a && a - str == 10) {
            *a++ = 0;

            hms = a;

            a = strchr(str, '-');
            b = strrchr(str, '-');

            if (a && b && a - str == 4 && b-a == 3) {
                *a++ = 0;
                *b++ = 0;

                snprintf(Year, sizeof(Year), "%.*s", 4, str);
                snprintf(Mon, sizeof(Mon), "%.*s", 2, a);
                snprintf(Day, sizeof(Day), "%.*s", 2, b);
            } else {
                // error date format
                return (-1);
            }

            a = strchr(hms, ':');
            b = strrchr(hms, ':');

            if (a && b && a - hms == 2 && b-a == 3) {
                char *dot = strchr(b, '.');
                if (!dot || dot - b != 3) {
                    // error stamp format
                    return (-1);
                }

                *a++ = 0;
                *b++ = 0;
                *dot++ = 0;

                snprintf(hour, sizeof(hour), "%.*s", 2, hms);
                snprintf(min, sizeof(min), "%.*s", 2, a);
                snprintf(sec, sizeof(sec), "%.*s", 2, b);

                if (*dot) {
                    msec[0] = *dot++;

                    if (*dot) {
                        msec[1] = *dot++;

                        if (*dot) {
                            msec[2] = *dot++;
                        }
                    }
                }
            } else {
                // error date format
                return (-1);
            }
        } else {
            // error datetime format
            return (-1);
        }
    } else {
        // error format
        return (-1);
    }

    if (cstr_isdigit(Year, 4) &&
        cstr_isdigit(Mon, 2) &&
        cstr_isdigit(Day, 2) &&
        cstr_isdigit(hour, 2) &&
        cstr_isdigit(min, 2) &&
        cstr_isdigit(sec, 2) &&
        cstr_isdigit(msec, 3)) {
        time_t tsec;
        struct tm t = {0};

        t.tm_year = atoi(Year);
        t.tm_mon = atoi(Mon);
        t.tm_mday = atoi(Day);

        t.tm_hour = atoi(hour);
        t.tm_min = atoi(min);
        t.tm_sec = atoi(sec);

        if (! time_is_valid(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)) {
            // invalid time
            return (-1);
        }

        t.tm_year -= 1900;
        t.tm_mon  -= 1;

        // since 1970-01-01 UTChh:00:00 (china: hh=8)
        tsec = mktime(&t);

        if (tsec == (time_t)(-1)) {
            fprintf(stderr, "%s\n", strerror(errno));
            return (-1);
        }

        return (ub8)(tsec * 1000 + atoi(msec));
    }

    // error no digit
    return (-1);
}


static const char * cstr_timestamp_to_datetime (char *stampms, int mslen, char timestr[24])
{
    ub8 stamp = 0;
    time_t tsec;
    int msec;
    struct tm * t;

    if (mslen == -1) {
        mslen = cstr_length(stampms, 20);
    }

    if (! cstr_to_ub8(10, stampms, mslen, &stamp)) {
        // error stamp
        return NULL;
    }

    tsec = (time_t)(stamp / 1000);
    msec = (int)(stamp % 1000);

    t = localtime((const time_t*) &tsec);
    if ( !t ) {
        // error localtime
        return NULL;
    }

    t->tm_year += 1900;
    t->tm_mon  += 1;

    if (! time_is_valid(t->tm_year, t->tm_mon, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec)) {
        // invalid time
        return NULL;
    }

    if (msec) {
        /* 2012-12-22 17:45:59.875 */
        snprintf(timestr, 24, "%04d-%02d-%02d %02d:%02d:%02d.%03d", t->tm_year, t->tm_mon, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec, msec);
    } else {
        snprintf(timestr, 20, "%04d-%02d-%02d %02d:%02d:%02d", t->tm_year, t->tm_mon, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec);
    }

    return timestr;
}

#ifdef __cplusplus
}
#endif

#endif /* _CSTR_UT_H */
