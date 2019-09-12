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
 * rdbtablefilter.h
 *   rdb table filter
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#ifndef RDBTABLEFILTER_H_INCLUDED
#define RDBTABLEFILTER_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#include "rdbsqlstmt.h"


#define RDBTABLE_FILTER_ACCEPT    1
#define RDBTABLE_FILTER_REJECT    0


#define RDBResultNodeState(resMap, nodeid)  (&(resMap->filter->nodestates[nodeid]))


typedef struct _RDBFilterNode_t
{
    struct _RDBFilterNode_t *next;

    RDBValueType valtype;

    RDBFilterExpr expr;

    // 0: not null; 1: is null
    int null_dest;

    // 0: invalid
    // 1: ub8_dest
    // 2: ub8_dest with hex
    // 3: sb8_dest
    // 4: dbl_dest
    int val_dest;

    union {
        ub8 ub8_dest;       // val_dest=1, 2
        sb8 sb8_dest;       // val_dest=3
        double dbl_dest;    // val_dest=4
    };

    int destlen;
    char dest[0];
} RDBFilterNode_t, *RDBFilterNode;


typedef struct _RDBTableFilter_t
{
    // reference
    RDBSQLStmt sqlstmt;

    // "tablespace.tablename"
    char table[RDB_KEY_NAME_MAXLEN *2 + 1];

    // 1-based rowkey field index refer to fieldes
    //   rowkeyids[0] is field count
    int rowkeyids[RDBAPI_SQL_KEYS_MAX + 1];

    // 1-based rowkey column filters
    RDBFilterNode rowkeyfilters[RDBAPI_SQL_KEYS_MAX + 1];

    // 1-based field index refer to fieldes used in HMGET 
    //   getfieldids[0] is field count
    int getfieldids[RDBAPI_ARGV_MAXNUM + 1];

    // 0-based fields name in HMGET
    const char *getfieldnames[RDBAPI_ARGV_MAXNUM + 1];
    size_t getfieldnameslen[RDBAPI_ARGV_MAXNUM + 1];

    // 1-based field value filters
    RDBFilterNode fieldfilters[RDBAPI_ARGV_MAXNUM + 1];

    // max field id (1-based) for select getfieldids
    int selfieldnum;

    // use $HMGET than SCAN
    int use_hmget;

    // rowkey pattern used in SCAN cursor MATCH $keypattern
    int patternprefixlen;
    int patternlen;
    char keypattern[RDB_ROWKEY_MAX_SIZE];

    // result cursor state
    RDBTableCursor_t nodestates[RDB_CLUSTER_NODES_MAX];
} RDBTableFilter_t;


RDBFilterNode RDBFilterNodeAdd (RDBFilterNode existed, RDBFilterExpr expr, RDBValueType valtype, const char *dest, int destlen);

void RDBFilterNodeFree (RDBFilterNode node);

int RDBFilterNodeExpr (RDBFilterNode node, const char *sour, int sourlen);

RDBTableFilter RDBTableFilterNew (RDBSQLStmt sqlstmt, const char *tablespace, const char *tablename);

void RDBTableFilterFree (RDBTableFilter filter);

int RDBTableFilterNode (RDBFilterNode filternodes[], RDBValueType valtype, const char *colsval[], int valslen[], int numcols);

int RDBTableFilterRowkeyVals(RDBTableFilter filter, int prefixlen, char *rowkeystr, int rowkeylen, const char *rkvals[], int rkvalslen[]);

int RDBTableFilterReplyCols (RDBTableFilter filter, redisReply *replyCols);


#if defined(__cplusplus)
}
#endif

#endif /* RDBTABLEFILTER_H_INCLUDED */
