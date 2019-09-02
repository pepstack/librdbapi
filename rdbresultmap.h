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
        // RDB_COLTYPE_INTEGER
        sb8        lval;

        // RDB_COLTYPE_DOUBLE
        double     dval;

        // RDB_COLTYPE_STRING
        RDBZString zstr;

        // RDB_COLTYPE_BINARY
        tpl_bin    bval;

        // RDB_COLTYPE_REPLY
        redisReply *reply;

        // RDB_COLTYPE_RESULTMAP
        RDBRowset resultmap; /* nested result map */
    };
} RDBCell_t;


typedef struct _RDBRow_t
{
    RDBZString rowkey;
    redisReply  *replyKey;

    /* number of cells if replyRow is NULL */
    int ncols;

    /* cell values of row */
    RDBCell_t colvals[0];
} RDBRow_t;


/* ResultMap */
typedef struct _RDBRowset_t
{
    /* cached environment variables */
    //RDBCtx ctx;
    //RDBTableFilter filter;
    //RDBSQLStmtType stmt;
    char delimiter;

    /* result rows saved in rbtree as a map */
    red_black_tree_t  rowstree;

    /* number of columns and column headers */
    ub4 numcols;
    ub1 displaywidths[RDBAPI_ARGV_MAXNUM + 4];
    RDBZString names[0];
} RDBRowset_t;


RDBCell RDBCellInit (RDBCell cell, RDBCellType type, void *value);

void RDBCellClean (RDBCell cell);

RDBRow RDBRowNew (ub4 numcols, redisReply *replyKey, char *rowkey, int keylen);

void RDBRowFree (RDBRow row);

const char * RDBRowGetKey (RDBRow row, ub4 *keylen);

ub4 RDBRowGetCells (RDBRow row);

RDBCell RDBRowGetCell (RDBRow row, int colindex);

void RDBCellPrint (RDBRowset resultmap, RDBCell cell, FILE *fout);

RDBAPI_RESULT RDBRowsetCreate (int numcols, const char *names[], RDBRowset *outresultmap);

// extern
void RDBRowsetDestroy (RDBRowset resultmap);

// extern
void RDBRowsetCleanRows (RDBRowset resultmap);

// extern
int RDBRowsetGetCols (RDBRowset resultmap);

// extern
RDBZString RDBRowsetGetColName (RDBRowset resultmap, int colindex);

// extern
void RDBRowsetPrint (RDBRowset resultmap, const RDBSheetStyle_t *styles, FILE *fout);

void RDBRowsetTraverse (RDBRowset resultmap, void (ResultRowCbFunc)(void *, void *), void *arg);


#if defined(__cplusplus)
}
#endif

#endif /* RDBRESULTMAP_H_INCLUDED */
