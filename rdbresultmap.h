/***********************************************************************
* Copyright (c) 2008-2080 pepstack.com, 350137278@qq.com
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
 * rdbresultmap.h
 *   rdb result map type
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-09-02
 * @update:
 */
#ifndef RDBRESULTMAP_H_INCLUDED
#define RDBRESULTMAP_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#include "rdbcommon.h"


typedef struct _RDBCell_t
{
    /* type of col value */
    RDBCellType type;

    union {
        // RDB_COLTYPE_STRING
        RDBZString zstr;

        // RDB_COLTYPE_BINARY
        RDBBinary  bin;

        // RDB_COLTYPE_REPLY
        redisReply *reply;

        // RDB_COLTYPE_RESULTMAP
        RDBRowset resultmap; /* nested result map */
    };
} RDBCell_t;


typedef struct _RDBRow_t
{
    char *key;
    int keylen;

    /* makes this structure hashable */
    UT_hash_handle hh;

    /* count of cells */
    int count;

    /* cell values of row */
    RDBCell_t cells[0];
} RDBRow_t, *RDBRowNode, *RDBRowsHashMap;


typedef struct _RDBRowIter_t
{
    RDBRowNode head;
    RDBRowNode el;
    RDBRowNode tmp;
} RDBRowIter_t;


/* ResultMap */
typedef struct _RDBRowset_t
{
    /* cached environment variables */
    //RDBCtx ctx;
    //RDBTableFilter filter;
    //RDBSQLStmtType stmt;

    /* rowsmap deletion-safe iteration */
    RDBRowIter_t rowiter;

    /* result rows saved in hashmap */
    RDBRowsHashMap rowsmap;

    /* number of columns and names for column headers */
    int numcolheaders;
    RDBZString colheadernames[0];
} RDBRowset_t;


#if defined(__cplusplus)
}
#endif

#endif /* RDBRESULTMAP_H_INCLUDED */
