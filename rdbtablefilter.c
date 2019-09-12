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


/**
 * https://github.com/kokke/tiny-regex-c
 */
#include "common/tiny-regex-c/re.h"


#define RDBEXPR_SGN(a, b)  ((a)>(b)? 1:((a)<(b)? (-1):0))


// src $expr dst ? true(1) : false(0)

//  1: accept
//  0: reject
// -1: reject with error
//
int doNodeExprValue (RDBFilterNode node, const char *src, int slen)
{
    sb8 s8val;
    ub8 u8val;
    double dbval;

    // reject with error
    int result = -1;

    int cmp = 0;

    if (node->expr == RDBFIL_IGNORE) {
        // always accept if expr not given
        return 1;
    }

    if (node->null_dest) {
        if (node->expr == RDBFIL_EQUAL) {
            // is null?
            return src? 0 : 1;
        }

        if (node->expr == RDBFIL_NOT_EQUAL) {
            // not null?
            return src? 1 : 0;
        }
    }

    if (node->valtype == RDBVT_STR) {
        cmp = cstr_compare_len(src, slen, node->dest, node->destlen);

        switch (node->expr) {
        case RDBFIL_EQUAL:
            result = (!cmp? 1 : 0);
            break;

        case RDBFIL_NOT_EQUAL:
            result = (!cmp? 0 : 1);
            break;

        case RDBFIL_GREAT_THAN:
            result = (cmp > 0? 1 : 0);
            break;

        case RDBFIL_LESS_THAN:
            result = (cmp < 0? 1 : 0);
            break;

        case RDBFIL_GREAT_EQUAL:
            result = (cmp >= 0? 1 : 0);
            break;

        case RDBFIL_LESS_EQUAL:
            result = (cmp <= 0? 1 : 0);
            break;

        case RDBFIL_LEFT_LIKE:
            // a like 'left%'
            //   src="aaaaB"
            //   dst="aaaaBBBB"    
            result = cstr_startwith(src, slen, node->dest, node->destlen);
            break;

        case RDBFIL_RIGHT_LIKE:
            // a like '%right'
            //   src="aBBBB"            
            //   dst="aaaaBBBB" 
            result = cstr_endwith(src, slen, node->dest, node->destlen);
            break;

        case RDBFIL_LIKE:
            // a like 'left%' or '%mid%' or '%right'
            result = cstr_containwith(src, slen, node->dest, node->destlen);
            break;

        case RDBFIL_MATCH:
            if (re_match(src, node->dest) == -1) {
                result = 0;
            } else {
                result = 1;
            }
            break;
        }

        return result;
    }

    if (node->val_dest == 3) {
        if (cstr_to_sb8(10, src, slen, &s8val) > 0) {
            cmp = RDBEXPR_SGN(s8val, node->sb8_dest);
        } else {
            return (-1);
        }
    } else if (node->val_dest == 1) {
        if (cstr_to_ub8(10, src, slen, &u8val) > 0) {
            cmp = RDBEXPR_SGN(u8val, node->ub8_dest);
        } else {
            return (-1);
        }
    } else if (node->val_dest == 2) {
        if (cstr_to_ub8(16, src, slen, &u8val) > 0) {
            cmp = RDBEXPR_SGN(u8val, node->ub8_dest);
        } else {
            return (-1);
        }
    } else if (node->val_dest == 4) {
        if (cstr_to_dbl(src, slen, &dbval) > 0) {
            cmp = RDBEXPR_SGN(dbval, node->dbl_dest);
        } else {
            return (-1);
        }
    } else {
        // reject for error
        return (-1);
    }

    switch (node->expr) {
    case RDBFIL_EQUAL:
        result = (!cmp? 1 : 0);
        break;

    case RDBFIL_NOT_EQUAL:
        result = (! cmp? 0 : 1);
        break;

    case RDBFIL_GREAT_THAN:
        result = (cmp > 0? 1 : 0);
        break;

    case RDBFIL_LESS_THAN:
        result = (cmp < 0? 1 : 0);
        break;

    case RDBFIL_GREAT_EQUAL:
        result = (cmp >= 0? 1 : 0);
        break;

    case RDBFIL_LESS_EQUAL:
        result = (cmp <= 0? 1 : 0);
        break;
    }

    return result;
}


RDBFilterNode RDBFilterNodeAdd (RDBFilterNode existed, RDBFilterExpr expr, RDBValueType valtype, const char *dest, int destlen)
{
    RDBFilterNode newnode;

    int null_dest = 0;

    if (destlen == -1) {
        destlen = cstr_length(dest, RDB_KEY_VALUE_SIZE - 1);
    }

    if (! cstr_compare_len(dest, destlen, "(null)", 6)) {
        null_dest = 1;
    }

    newnode = (RDBFilterNode) RDBMemAlloc(sizeof(RDBFilterNode_t) + destlen + 1);

    newnode->next = existed;

    newnode->expr = expr;
    newnode->valtype = valtype;

    newnode->null_dest = null_dest;
    newnode->destlen = destlen;
    memcpy(newnode->dest, dest, destlen);

    // validate dest value
    if (! newnode->null_dest) {
        switch (valtype) {
        case RDBVT_SB8:
        case RDBVT_SB4:
        case RDBVT_SB2:
        case RDBVT_CHAR:
            if (cstr_to_sb8(10, newnode->dest, newnode->destlen, &newnode->sb8_dest) > 0) {
                newnode->val_dest = 3;
            }
            break;

        case RDBVT_UB8:
        case RDBVT_UB4:
        case RDBVT_UB2:
        case RDBVT_BYTE:
            if (cstr_to_ub8(10, newnode->dest, newnode->destlen, &newnode->ub8_dest) > 0) {
                newnode->val_dest = 1;
            }
            break;

        case RDBVT_UB8X:
        case RDBVT_UB4X:
            if (cstr_to_ub8(16, newnode->dest, newnode->destlen, &newnode->ub8_dest) > 0) {
                newnode->val_dest = 2;
            }
            break;

        case RDBVT_FLT64:
            if (cstr_to_dbl(newnode->dest, newnode->destlen, &newnode->dbl_dest) > 0) {
                newnode->val_dest = 4;
            }
            break;
        }
    }

    if (! newnode->valtype) {
        newnode->valtype = RDBVT_STR;
    }

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
    if (node) {
        if (doNodeExprValue(node, sour, sourlen) != 1) {
            return RDBTABLE_FILTER_REJECT;
        }

        return RDBFilterNodeExpr(node->next, sour, sourlen);
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
