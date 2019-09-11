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
 * rdbfilter.c
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#include "rdbtablefilter.h"


RDBFilterNode RDBFilterNodeAdd (RDBFilterNode existed, RDBFilterExpr expr, RDBValueType valtype, const char *dest, int destlen)
{
    RDBFilterNode newnode;
    
    if (destlen == -1) {
        destlen = cstr_length(dest, RDB_KEY_VALUE_SIZE - 1);
    }

    newnode = (RDBFilterNode) RDBMemAlloc(sizeof(RDBFilterNode_t) + destlen + 1);

    newnode->next = existed;

    newnode->expr = expr;
    newnode->valtype = valtype;

    newnode->destlen = destlen;
    memcpy(newnode->dest, dest, destlen);

    return newnode;
}


void RDBFilterNodeFree (RDBFilterNode node)
{
    RDBFilterNode tmpnode;

    while (node) {
        tmpnode = node;
        node = node->next;
        RDBMemFree(tmpnode);
    }
}


int RDBFilterNodeExpr (RDBFilterNode node, const char *sour, int sourlen)
{
    while (node) {
        if (! RDBExprValues(node->valtype, sour, sourlen, node->expr, node->dest, node->destlen)) {
            return RDBTABLE_FILTER_REJECT;
        }
  
        node = node->next;
    }

    return RDBTABLE_FILTER_ACCEPT;
}


RDBTableFilter RDBTableFilterNew (RDBSQLStmt sqlstmt, const char *tablespace, const char *tablename)
{
    RDBTableFilter filter = (RDBTableFilter) RDBMemAlloc(sizeof(RDBTableFilter_t));
    if (! filter) {
        fprintf(stderr, "(%s:%d): RDBTableFilterNew failed: no memory.\n", __FILE__, __LINE__);
        exit(EXIT_FAILURE);
    }
    filter->sqlstmt = sqlstmt;
    snprintf_chkd_V1(filter->table, sizeof(filter->table), "%s.%s", tablespace, tablename);
    return filter;
}


void RDBTableFilterFree (RDBTableFilter filter)
{
    int i;
    RDBFilterNode node;

    for (i = 0; i < sizeof(filter->rowkeyfilters)/sizeof(filter->rowkeyfilters[0]); i++) {
        node = filter->rowkeyfilters[i];
        if (node) {
            RDBFilterNodeFree(node);
        }
    }

    for (i = 0; i < sizeof(filter->fieldfilters)/sizeof(filter->fieldfilters[0]); i++) {
        node = filter->fieldfilters[i];
        if (node) {
            RDBFilterNodeFree(node);
        }
    }

    RDBMemFree(filter);
}


int RDBTableFilterNode (RDBFilterNode filternodes[], RDBValueType valtype, const char *colsval[], int valslen[], int numcols)
{
    int col;
    RDBFilterNode node;

    for (col = 0; col < numcols; col++) {
        node = filternodes[col];
        if (node) {
            if (RDBFilterNodeExpr(node, colsval[col], valslen[col]) == RDBTABLE_FILTER_REJECT) {
                return RDBTABLE_FILTER_REJECT;
            }
        }
    }

    return RDBTABLE_FILTER_ACCEPT;    
}


int RDBTableFilterRowkeyVals(RDBTableFilter filter, int prefixlen, char *rowkeystr, int rowkeylen, const char *rkvals[], int rkvalslen[])
{
    int i = 0;

    char *end = NULL;
    char *str = &rowkeystr[prefixlen];

    while (i < RDBAPI_KEYS_MAXNUM && (end=strchr(str, ':')) != NULL) {
        rkvals[i] = str;
        rkvalslen[i] = (int)(end - str);

        if (filter) {
            // scan match key needs filter
            if (RDBFilterNodeExpr(filter->rowkeyfilters[i + 1], rkvals[i], rkvalslen[i]) != RDBTABLE_FILTER_ACCEPT) {
                return (-1);
            }
        }

        i++;
        str = ++end;
    }

    if (end) {
        return (-1);
    }

    rkvals[i] = str;
    rkvalslen[i] = (int) (&rowkeystr[rowkeylen - 1] - str);

    if (filter) {
        if (RDBFilterNodeExpr(filter->rowkeyfilters[i+1], rkvals[i], rkvalslen[i]) != RDBTABLE_FILTER_ACCEPT) {
            return (-1);
        }
    }

    i++;
    return i;
}


int RDBTableFilterReplyCols (RDBTableFilter filter, redisReply *replyCols)
{
    redisReply *replyCol;

    int col = 0;

    if (filter->getfieldids[0]) {
        if (! replyCols || replyCols->elements != filter->getfieldids[0]) {
            fprintf(stderr, "(%s:%d) SHOULD NEVER RUN TO THIS!\n", __FILE__, __LINE__);
            return (-1);
        }

        for (; col < replyCols->elements; col++) {
            replyCol = replyCols->element[col];

            if (! replyCol) {
                fprintf(stderr, "(%s:%d) SHOULD NEVER RUN TO THIS!\n", __FILE__, __LINE__);
                return (-1);
            }

            if (RDBFilterNodeExpr(filter->fieldfilters[col+1], replyCol->str, (int)replyCol->len) != RDBTABLE_FILTER_ACCEPT) {
                return (-1);
            }
        }
    }

    return col;
}
