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
 * cshell.h
 *   C Interactive Shell
 *
 * author: master@pepstack.com
 *
 * create: 2018-02-08
 * update: 2018-02-08
 */

#ifndef CSHELL_H_INCLUDED
#define CSHELL_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#include "cstrut.h"

#include <malloc.h>


#define CSH_ANSWER_YES  1
#define CSH_ANSWER_NO   0


/**
 * ------ color output ------
 * 30: black
 * 31: red
 * 32: green
 * 33: yellow
 * 34: blue
 * 35: purple
 * 36: cyan
 * 37: white
 * --------------------------
 * "\033[31m RED   \033[0m"
 * "\033[32m GREEN \033[0m"
 * "\033[33m YELLOW \033[0m"
 */

#define csh_red_msg(message)     "\033[31m"message"\033[0m"
#define csh_green_msg(message)   "\033[32m"message"\033[0m"
#define csh_yellow_msg(message)  "\033[33m"message"\033[0m"
#define csh_blue_msg(message)    "\033[34m"message"\033[0m"
#define csh_purple_msg(message)  "\033[35m"message"\033[0m"
#define csh_cyan_msg(message)    "\033[36m"message"\033[0m"

#define CSH_RED_MSG(message)     "\033[1;31m"message"\033[0m"
#define CSH_GREEN_MSG(message)   "\033[1;32m"message"\033[0m"
#define CSH_YELLOW_MSG(message)  "\033[1;33m"message"\033[0m"
#define CSH_BLUE_MSG(message)    "\033[1;34m"message"\033[0m"
#define CSH_PURPLE_MSG(message)  "\033[1;35m"message"\033[0m"
#define CSH_CYAN_MSG(message)    "\033[1;36m"message"\033[0m"

#ifndef CSH_BLOCK_SIZE
# define CSH_BLOCK_SIZE    4096
#endif


typedef struct
{
    size_t capacity;
    size_t offset;
    char chunk[0];
} CSH_chunk_t, *CSH_chunk;


static CSH_chunk CSH_chunk_new (size_t capacity)
{
    size_t sz = ((capacity + CSH_BLOCK_SIZE - 1) / CSH_BLOCK_SIZE) * CSH_BLOCK_SIZE;

    CSH_chunk_t *chk = (CSH_chunk_t *) calloc(1, sizeof(CSH_chunk_t) + sz);
    if (! chk) {
        exit(ENOMEM);
    }

    chk->capacity = sz;

    return chk;
}


static void CSH_chunk_free (CSH_chunk chk)
{
    free(chk);
}


static void CSH_chunk_reset (CSH_chunk chk)
{
    memset(chk->chunk, 0, chk->capacity);
    chk->offset = 0;
}


static CSH_chunk CSH_chunk_add (CSH_chunk chk, const void *data, size_t datasize)
{
    if (chk->offset + datasize < chk->capacity) {
        memcpy(chk->chunk + chk->offset, data, datasize);
        chk->offset += datasize;
    } else {
        size_t newcap = ((chk->offset + datasize + CSH_BLOCK_SIZE - 1) / CSH_BLOCK_SIZE) * CSH_BLOCK_SIZE;
        chk = realloc((void *) chk, newcap);
        if (! chk) {
            exit(ENOMEM);
        }
        memset(chk->chunk + chk->capacity, 0, newcap - chk->capacity);
        chk->capacity = newcap;

        memcpy(chk->chunk + chk->offset, data, datasize);
        chk->offset += datasize;
    }

    return chk;
}


static const char * CSH_get_input (const char *message, char *linebuf, size_t bufsize, int *outlen)
{
    size_t i;
    int c;
    const char *start;
    char *end = linebuf;

    if (message) {
        printf("%s", message);
    }

    if (! linebuf) {
        return NULL;
    }

    memset(linebuf, 0, bufsize);

    i = 0;

    while (i < bufsize) {
        c = fgetc(stdin);

        if (c == EOF) {
            break;
        }

        if ((*end++ = c) == '\n') {
            break;
        }

        i++;
    }

    if (i > 0) {
        *--end = '\0';
    } else {
        *linebuf = '\0';
    }

    start = cstr_LRtrim_chr(cstr_LRtrim_chr(linebuf, 32, 0), '\n', 0);

    if (*start == '\0') {
        return NULL;
    }

    if (outlen) {
        *outlen = (int)(end - start);
    }

    return start;
}


static int CSH_parse_answer (const char *answer, int defval)
{
    if (! answer) {
        return defval;
    }

    if (! strcmp(answer, "Y") || ! strcmp(answer, "yes") || ! strcmp(answer, "Yes")) {
        return CSH_ANSWER_YES;
    }

    if (! strcmp(answer, "N") || ! strcmp(answer, "no") || ! strcmp(answer, "n") || ! strcmp(answer, "No")) {
        return CSH_ANSWER_NO;
    }

    return defval;
}


static int CSH_quit_or_exit (const char *msg, int msglen)
{
    if (! cstr_compare_len(msg, msglen, "quit", 4) || ! cstr_compare_len(msg, msglen, "exit", 4) ||
        ! cstr_compare_len(msg, msglen, "QUIT", 4) || ! cstr_compare_len(msg, msglen, "EXIT", 4) ||        
        ! cstr_compare_len(msg, msglen, "Quit", 4) || ! cstr_compare_len(msg, msglen, "Exit", 4)) {
        return CSH_ANSWER_YES;
    }

    return CSH_ANSWER_NO;
}


#if defined(__cplusplus)
}
#endif

#endif /* CSHELL_H_INCLUDED */