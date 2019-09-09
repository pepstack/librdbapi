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
 * rdbtable.c
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#include "rdbsqlstmt.h"


#define RDBTABLE_FILTER_ACCEPT    1
#define RDBTABLE_FILTER_REJECT    0


#define RDBResultNodeState(resMap, nodeid)  (&(resMap->filter->nodestates[nodeid]))


typedef struct _RDBFilterNode_t
{
    struct _RDBFilterNode_t *next;

    RDBValueType valtype;

    RDBFilterExpr expr;

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

    // number of rowkeys in keypattern
    int numrkpattern;

    // rowkey pattern used in SCAN cursor MATCH $keypattern
    int patternprefixlen;
    int patternlen;
    char keypattern[RDB_ROWKEY_MAX_SIZE];

    // result cursor state
    RDBTableCursor_t nodestates[RDB_CLUSTER_NODES_MAX];
} RDBTableFilter_t;


static RDBFilterNode RDBFilterNodeAdd (RDBFilterNode existed, RDBFilterExpr expr, RDBValueType valtype, const char *dest, int destlen)
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


static void RDBFilterNodeFree (RDBFilterNode node)
{
    RDBFilterNode tmpnode;

    while (node) {
        tmpnode = node;
        node = node->next;
        RDBMemFree(tmpnode);
    }
}


static int RDBFilterNodeExpr (RDBFilterNode node, const char *sour, int sourlen)
{
    while (node) {
        if (! RDBExprValues(node->valtype, sour, sourlen, node->expr, node->dest, node->destlen)) {
            return RDBTABLE_FILTER_REJECT;
        }
  
        node = node->next;
    }

    return RDBTABLE_FILTER_ACCEPT;
}


static RDBTableFilter RDBTableFilterNew (RDBSQLStmt sqlstmt, const char *tablespace, const char *tablename)
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


static void RDBTableFilterFree (RDBTableFilter filter)
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


static int RDBTableFilterNode (RDBFilterNode filternodes[], RDBValueType valtype, const char *colsval[], int valslen[], int numcols)
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

/**
 * RDBTableDesFieldIndex()
 * returns:
 *   0-based index for fieldname in tabledes
 *   -1: not found
 */
static int RDBTableDesFieldIndex (const RDBTableDes_t *tabledes, const char *fieldname, int fieldnamelen)
{
    if (fieldnamelen == -1) {
        fieldnamelen = cstr_length(fieldname, RDB_KEY_NAME_MAXLEN + 1);
    }

    if (fieldnamelen > 0 && fieldnamelen <= RDB_KEY_NAME_MAXLEN) {
        int k = 0;
        for (; k < tabledes->nfields; k++) {
            if (!cstr_compare_len(tabledes->fielddes[k].fieldname, tabledes->fielddes[k].namelen, fieldname, fieldnamelen)) {
                return k;
            }
        }
    }

    // fieldname not found
    return (-1);
}


static int RDBTableFilterRowkeyVals(RDBTableFilter filter, int prefixlen, char *rowkeystr, int rowkeylen, const char *rkvals[], int rkvalslen[])
{
    int i = 0;

    char *end = NULL;
    char *str = &rowkeystr[prefixlen];

    while (i < RDBAPI_KEYS_MAXNUM && (end=strchr(str, ':')) != NULL) {
        rkvals[i] = str;
        rkvalslen[i] = (int)(end - str);

        if (filter) {
            if (filter->numrkpattern != filter->rowkeyids[0]) {
                // scan match key needs filter
                if (RDBFilterNodeExpr(filter->rowkeyfilters[i + 1], rkvals[i], rkvalslen[i]) != RDBTABLE_FILTER_ACCEPT) {
                    return (-1);
                }
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
        if (filter->numrkpattern != filter->rowkeyids[0]) {
            if (RDBFilterNodeExpr(filter->rowkeyfilters[i+1], rkvals[i], rkvalslen[i]) != RDBTABLE_FILTER_ACCEPT) {
                return (-1);
            }
        }
    }

    i++;
    return i;
}


static int RDBTableFilterReplyCols (RDBTableFilter filter, redisReply *replyCols)
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


ub8 RDBResultMapGetOffset (RDBResultMap resultmap)
{
    ub8 offset = 0;
    int nodeindex = RDBEnvNumNodes(RDBCtxGetEnv(resultmap->ctx));
    while (nodeindex-- > 0) {
        offset += RDBResultNodeState(resultmap, nodeindex)->offset;
    }
    return offset;
}


RDBAPI_RESULT RDBTableScanFirst (RDBCtx ctx, RDBSQLStmt sqlstmt, RDBResultMap *outresultmap)
{
    int i, j, n, fieldid, rowkeyid;

    RDBValueType valtype;

    size_t offsz;

    RDBResultMap resultmap = NULL;
    RDBTableFilter filter = NULL;

    RDBTableDes_t tabledes = {0};

    int colindex = 0;
    const char *colnames[RDBAPI_ARGV_MAXNUM + RDBAPI_KEYS_MAXNUM + 1] = {0};
    int colnameslen[RDBAPI_ARGV_MAXNUM + RDBAPI_KEYS_MAXNUM + 1] = {0};

    ub4 LmtRows = sqlstmt->select.limit;

    *outresultmap = NULL;

    if (RDBTableDescribe(ctx, sqlstmt->select.tablespace, sqlstmt->select.tablename, &tabledes) != RDBAPI_SUCCESS) {
        return RDBAPI_ERROR;
    }

    filter = RDBTableFilterNew(sqlstmt, sqlstmt->select.tablespace, sqlstmt->select.tablename);

    // init rowkeyids
    for (i = 0; i < tabledes.nfields; i++) {
        rowkeyid = tabledes.fielddes[i].rowkey;
        if (rowkeyid) {
            fieldid = i + 1;

            filter->rowkeyids[rowkeyid] = fieldid;

            // set count for rowkeys
            filter->rowkeyids[0] += 1;
            
            colnames[colindex] = tabledes.fielddes[i].fieldname;
            colnameslen[colindex] = tabledes.fielddes[i].namelen;
            colindex++;
        }
    }

    // add into HMGET fields from SELECT clause first
    if (sqlstmt->select.numselect == -1) {
        for (i = 0; i < tabledes.nfields; i++) {
            if (! tabledes.fielddes[i].rowkey) {
                n = filter->getfieldids[0];

                filter->getfieldids[0] = ++n;
                filter->getfieldids[ n ] = i + 1;
            }
        }
    } else {
        for (i = 0; i < sqlstmt->select.numselect; i++) {
            fieldid = RDBTableDesFieldIndex(&tabledes, sqlstmt->select.selectfields[i], sqlstmt->select.selectfieldslen[i]) + 1;
            if (! fieldid) {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: field in SELECT not found: '%s'", sqlstmt->select.selectfields[i]);
                RDBTableFilterFree(filter);
                return RDBAPI_ERR_BADARG;
            }
            if (! tabledes.fielddes[fieldid - 1].rowkey) {
                n = filter->getfieldids[0];
                for (j = 1; j <= n; j++) {
                    if (filter->getfieldids[j] == fieldid) {
                        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: duplicated field in SELECT: '%s'", sqlstmt->select.selectfields[i]);
                        RDBTableFilterFree(filter);
                        return RDBAPI_ERR_BADARG;
                    }
                }
                filter->getfieldids[0] = ++n;
                filter->getfieldids[ n ] = fieldid;                
            }
        }
    }

    filter->selfieldnum = filter->getfieldids[0];

    // add into HMGET fields from WHERE clause second
    for (i = 0; i < sqlstmt->select.numwhere; i++) {
        fieldid = RDBTableDesFieldIndex(&tabledes, sqlstmt->select.fields[i], sqlstmt->select.fieldslen[i]) + 1;
        if (! fieldid) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: field in SELECT not found: '%s'", sqlstmt->select.fields[i]);
            RDBTableFilterFree(filter);
            return RDBAPI_ERR_BADARG;
        }
        rowkeyid = tabledes.fielddes[fieldid - 1].rowkey;
        valtype = tabledes.fielddes[fieldid - 1].fieldtype;

        if (rowkeyid) {
            if (cstr_find_chrs(sqlstmt->select.fieldvals[i], sqlstmt->select.fieldvalslen[i], ":{ }", 4)) {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: illegal char in rowkey(%s): '%s'", sqlstmt->select.fields[i], sqlstmt->select.fieldvals[i]);
                RDBTableFilterFree(filter);
                return RDBAPI_ERR_BADARG;
            }

            filter->rowkeyfilters[ rowkeyid ] = RDBFilterNodeAdd(filter->rowkeyfilters[ rowkeyid ], sqlstmt->select.fieldexprs[i], valtype, sqlstmt->select.fieldvals[i], sqlstmt->select.fieldvalslen[i]);
        } else {
            n = filter->getfieldids[0];
            for (j = 1; j <= n; j++) {
                if (filter->getfieldids[j] == fieldid) {
                    // found existed field
                    filter->fieldfilters[j] = RDBFilterNodeAdd(filter->fieldfilters[j], sqlstmt->select.fieldexprs[i], valtype, sqlstmt->select.fieldvals[i], sqlstmt->select.fieldvalslen[i]);
                    fieldid = 0;
                    break;
                }
            }
            if (fieldid) {
                // add new field at last
                filter->getfieldids[0] = ++n;
                filter->getfieldids[ n ] = fieldid;
                filter->fieldfilters[ n ] = RDBFilterNodeAdd(filter->fieldfilters[ n ], sqlstmt->select.fieldexprs[i], valtype, sqlstmt->select.fieldvals[i], sqlstmt->select.fieldvalslen[i]);
            }
        }
    }

    // build keypattern
    filter->patternprefixlen = snprintf_chkd_V1(filter->keypattern, sizeof(filter->keypattern), "{%s::%s", sqlstmt->select.tablespace, sqlstmt->select.tablename);
    offsz = filter->patternprefixlen++;

    // set HMGET rather than SCAN default
    filter->numrkpattern = filter->rowkeyids[0];

    for (rowkeyid = 1; rowkeyid <= filter->rowkeyids[0]; rowkeyid++) {
        RDBFilterNode rknode = filter->rowkeyfilters[rowkeyid];

        n = 0;

        if (rknode && ! rknode->next && rknode->expr != RDBFIL_IGNORE) {
            switch (rknode->expr) {
            case RDBFIL_EQUAL:
            case RDBFIL_MATCH:
                n = snprintf_chkd_V1(filter->keypattern + offsz, sizeof(filter->keypattern) - offsz, ":%.*s", rknode->destlen, rknode->dest);
                break;

            case RDBFIL_LEFT_LIKE:
                n = snprintf_chkd_V1(filter->keypattern + offsz, sizeof(filter->keypattern) - offsz, ":%.*s*", rknode->destlen, rknode->dest);
                break;

            case RDBFIL_RIGHT_LIKE:
                n = snprintf_chkd_V1(filter->keypattern + offsz, sizeof(filter->keypattern) - offsz, ":*%.*s", rknode->destlen, rknode->dest);
                break;

            case RDBFIL_LIKE:
                n = snprintf_chkd_V1(filter->keypattern + offsz, sizeof(filter->keypattern) - offsz, ":*%.*s*", rknode->destlen, rknode->dest);
                break;
            }
            if (n) {
                rknode->expr = RDBFIL_IGNORE;
            }
        }

        if (! n) {
            n = snprintf_chkd_V1(filter->keypattern + offsz, sizeof(filter->keypattern) - offsz, ":*");
            filter->numrkpattern--;
        }

        offsz += n;
    }
    offsz += snprintf_chkd_V1(filter->keypattern + offsz, sizeof(filter->keypattern) - offsz, "}");

    if (offsz >= sizeof(filter->keypattern) || filter->keypattern[offsz - 1] != '}') {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: invalid key pattern: '%.*s'", offsz, filter->keypattern);
        RDBTableFilterFree(filter);
        return RDBAPI_ERR_BADARG;
    }
    filter->patternlen = (int) offsz;

    // set hmget fieldnames
    for (j = 1; j <= filter->getfieldids[0]; j++) {
        i = filter->getfieldids[j] - 1;

        filter->getfieldnames[j - 1] = tabledes.fielddes[i].fieldname;
        filter->getfieldnameslen[j - 1] = tabledes.fielddes[i].namelen;

        if (j <= filter->selfieldnum) {
            // only for result display
            colnames[colindex] = tabledes.fielddes[i].fieldname;
            colnameslen[colindex] = tabledes.fielddes[i].namelen;
            colindex++;
        }
    }

    filter->getfieldnames[RDBAPI_ARGV_MAXNUM] = 0;
    filter->getfieldnameslen[RDBAPI_ARGV_MAXNUM] = 0;

    if (sqlstmt->select.limit == (ub4) -1) {
        LmtRows = RDB_TABLE_LIMIT_MAX;
    } else if (sqlstmt->select.limit < RDB_TABLE_LIMIT_MIN) {
        LmtRows = RDB_TABLE_LIMIT_MIN;
    } else if (sqlstmt->select.limit > RDB_TABLE_LIMIT_MAX) {
        LmtRows = RDB_TABLE_LIMIT_MAX;
    }
    sqlstmt->select.limit = LmtRows;

    if (ctx->env->verbose) {
        if (filter->numrkpattern == filter->rowkeyids[0]) {
            printf("$HMGET %.*s", filter->patternlen, filter->keypattern);
            for (j = 0; j < filter->getfieldids[0]; j++) {
                printf(" %.*s", (int) filter->getfieldnameslen[j], filter->getfieldnames[j]);
            }
            printf("\n");
        } else {
            printf("$SCAN %"PRIu64" MATCH %.*s COUNT %"PRIu32"\n", sqlstmt->select.offset, filter->patternlen, filter->keypattern, sqlstmt->select.limit);
        }
    }

    // create result map
    do {
        char maptitle[RDB_KEY_VALUE_SIZE] = {0};

        if (filter->sqlstmt->stmt == RDBSQL_SELECT) {
            snprintf_chkd_V1(maptitle, sizeof(maptitle), "# SELECT results {%s}:", filter->table);
        } else if (filter->sqlstmt->stmt == RDBSQL_DELETE) {
            snprintf_chkd_V1(maptitle, sizeof(maptitle), "# DELETE results {%s}:", filter->table);
        } else {
            snprintf_chkd_V1(maptitle, sizeof(maptitle), "# (%s:%d) RDBSQL_INVALID: SHOULD NEVER RUN TO THIS! {%s}", __FILE__, __LINE__, filter->table);
        }

        if (RDBResultMapCreate(maptitle, colnames, colnameslen, colindex, &resultmap) != RDBAPI_SUCCESS) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) SHOULD NEVER RUN TO THIS!", __FILE__, __LINE__);
            return RDBAPI_ERROR;
        }
    } while(0);

    resultmap->ctx = ctx;
    resultmap->filter = filter;

    for (i = 0; i < RDBEnvNumNodes(ctx->env); i++) {
        RDBResultNodeState(resultmap, i)->nodeindex = i;
    }

    *outresultmap = resultmap;
    return RDBAPI_SUCCESS;
}


// internal api only on single node
//
RDBAPI_RESULT RDBTableScanOnNode (RDBCtxNode ctxnode, RDBTableCursor nodestate, const char *pattern, size_t patternlen, ub4 maxlimit, redisReply **outReply)
{
    RDBAPI_RESULT result;

    size_t argvlen[6];

    char cursor[22];
    char count[12];

    redisReply *reply = NULL;

    *outReply = NULL;

    // construct scan clause
    argvlen[0] = 4; // scan
    argvlen[1] = snprintf_chkd_V1(cursor, sizeof(cursor), "%"PRIu64, nodestate->cursor);
    cursor[21] = 0;

    argvlen[2] = 5; // match
    argvlen[3] = patternlen;

    argvlen[4] = 5; // count
    argvlen[5] = snprintf_chkd_V1(count, sizeof(count), "%u", maxlimit);
    count[11] = 0;

    const char *argv[] = {
        "scan",
        cursor,
        "match",
        pattern,
        "count",
        count,
        0
    };

    result = RedisExecArgvOnNode(ctxnode, 6, argv, argvlen, &reply);

    if (result == RDBAPI_SUCCESS) {
        if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 2 && reply->element[0]->type == REDIS_REPLY_STRING) {
            redisReply * replyTmp;

            // get returned cursor
            nodestate->cursor = strtoull(reply->element[0]->str, 0, 10);

            if (! nodestate->cursor) {
                // if cursor = 0, set node has no more data (finished = 1)
                nodestate->finished = 1;
            }

            if (reply->element[1]->type != REDIS_REPLY_ARRAY || reply->element[1]->elements == 0) {
                // (empty list or set)
                RedisFreeReplyObject(&reply);

                // no data, expect next
                return RDBAPI_CONTINUE;
            }

            nodestate->offset += reply->element[1]->elements;

            // reply success with array elements
            *outReply = reply->element[1];
            reply->element[1] = NULL;

            replyTmp = reply->element[0];
            reply->element[0] = NULL;

            RedisFreeReplyObject(&replyTmp);

            reply->elements = 0;
            RedisFreeReplyObject(&reply);

            // success: return data
            return RDBAPI_SUCCESS;
        } else {
            snprintf_chkd_V1(ctxnode->ctx->errmsg, sizeof(ctxnode->ctx->errmsg), "RDBAPI_ERROR: reply type(%d).", reply->type);
            RedisFreeReplyObject(&reply);
            return RDBAPI_ERROR;
        }
    }

    // no data returned, expect next
    return RDBAPI_CONTINUE;
}


/**
 * RDBTableScanNext
 *
 * returns:
 *   = 0: finished
 *   > 0: numrows
 *
 *
 * | MaxRows  |                        LmtRows
 * O----------+---.------+----------+---V====--+---...-->
 *                |                     |    |
 *             lastOffs              OffRows |
 *                |<-------- ReqRows ------->|
 */
ub8 RDBTableScanNext (RDBResultMap resultmap, ub8 OffRows, ub4 limit)
{
    RDBAPI_RESULT result;
    int  nodeindex, colindex;

    redisReply     *replyRows;
    redisReply     *replyCols;
    redisReply     *replyCol;

    const char *rkvals[RDBAPI_KEYS_MAXNUM + 1] = {0};
    int rkvalslen[RDBAPI_KEYS_MAXNUM + 1] = {0};

    ub8 LastOffs = RDB_ERROR_OFFSET;

    int rowkeynum = resultmap->filter->rowkeyids[0];
    int fieldsnum = resultmap->filter->getfieldids[0];

    RDBCtx ctx = resultmap->ctx;

    if (resultmap->filter->numrkpattern == resultmap->filter->rowkeyids[0]) {
        if (RedisExistsKey(ctx, resultmap->filter->keypattern, resultmap->filter->patternlen) == RDBAPI_TRUE) {
            // use HMGET rather than SCAN
            if (RedisHMGetLen(ctx, resultmap->filter->keypattern, resultmap->filter->patternlen,
                    resultmap->filter->getfieldnames, resultmap->filter->getfieldnameslen, &replyCols) == RDBAPI_SUCCESS) {

                // filter fields by fieldfilters
                if (RDBTableFilterReplyCols(resultmap->filter, replyCols) == fieldsnum) {

                    // split rowkey str into vals without rowkeyfilters
                    if (RDBTableFilterRowkeyVals(NULL, resultmap->filter->patternprefixlen, resultmap->filter->keypattern, resultmap->filter->patternlen, rkvals, rkvalslen) == rowkeynum) {
                        RDBRow row = NULL;

                        if (RDBRowNew(resultmap, resultmap->filter->keypattern, resultmap->filter->patternlen, &row) == RDBAPI_SUCCESS) {
                            if (RDBResultMapInsertRow(resultmap, row) == RDBAPI_SUCCESS) {
                                // set rowkey fields
                                for (colindex = 0; colindex < rowkeynum; colindex++) {
                                    RDBCellSetString(RDBRowCell(row, colindex), rkvals[colindex], rkvalslen[colindex]);
                                }

                                // set attr fields
                                for (colindex = 0; colindex < resultmap->filter->selfieldnum; colindex++) {
                                    replyCol = replyCols->element[colindex];
                                    replyCols->element[colindex] = NULL;
                                    RDBCellSetReply(RDBRowCell(row, rowkeynum + colindex), replyCol);
                                }

                                LastOffs = RDBResultMapGetOffset(resultmap);
                            } else {
                                // insert failed
                                RDBRowFree(row);
                            }
                        }
                    }
                }

                RedisFreeReplyObject(&replyCols);
            }
        }
    } else {
        RDBEnvNode  envnode;
        RDBCtxNode  ctxnode;
        RDBTableCursor  nodestate;

        int numMasters = 0;
        int finMasters = 0;

        // number of rows now to fetch
        sb8 CurRows;

        // number of rows we fetched
        ub8 NumRows = 0;

        // number of rows returned
        ub4 LmtRows = limit;

        // validate limit rows
        if (limit == (ub4) -1) {
            LmtRows = RDB_TABLE_LIMIT_MAX;
        } else if (limit < RDB_TABLE_LIMIT_MIN) {
            LmtRows = RDB_TABLE_LIMIT_MIN;
        } else if (limit > RDB_TABLE_LIMIT_MAX) {
            LmtRows = RDB_TABLE_LIMIT_MAX;
        }

        // get rows from all nodes
        LastOffs = RDBResultMapGetOffset(resultmap);
        if (LastOffs >= OffRows + LmtRows) {
            return LastOffs;
        }

        // get number of finished nodes
        for (nodeindex = 0; nodeindex < RDBEnvNumNodes(ctx->env); nodeindex++) {
            envnode = RDBEnvGetNode(ctx->env, nodeindex);

            if (RDBEnvNodeGetMaster(envnode, NULL) == RDBAPI_TRUE) {
                numMasters++;

                nodestate = RDBResultNodeState(resultmap, nodeindex);
                if (nodestate->finished) {
                    finMasters++;
                }
            }
        }

        if (numMasters == finMasters) {
            return RDB_ERROR_OFFSET;
        }

        nodeindex = 0;
        while (NumRows < LmtRows && nodeindex < RDBEnvNumNodes(ctx->env)) {
            envnode = RDBEnvGetNode(ctx->env, nodeindex);

            // scan only on master node
            if (RDBEnvNodeGetMaster(envnode, NULL) == RDBAPI_TRUE) {
                ctxnode = RDBCtxGetNode(ctx, nodeindex);
                ub8 SaveOffs = LastOffs;

                nodestate = RDBResultNodeState(resultmap, nodeindex);

                if (nodestate->finished) {
                    // goto next node since this current node has finished
                    nodeindex++;
                    continue;
                }

                // calc rows now to fetch
                CurRows = (sb8) (OffRows + LmtRows - SaveOffs - NumRows);

                if (CurRows > RDB_TABLE_LIMIT_MAX) {
                    CurRows = RDB_TABLE_LIMIT_MAX;
                }

                if (NumRows) {
                    CurRows = LmtRows - NumRows;
                }

                if (CurRows > 0) {
                    result = RDBTableScanOnNode(ctxnode, nodestate, resultmap->filter->keypattern, resultmap->filter->patternlen, (ub4) CurRows, &replyRows);
                } else {
                    LastOffs = RDBResultMapGetOffset(resultmap);
                    goto return_offset;
                }

                if (result == RDBAPI_SUCCESS) {
                    LastOffs = RDBResultMapGetOffset(resultmap);

                    if (LastOffs > OffRows) {
                        // here we should add new rows
                        size_t i = 0;

                        if (OffRows > SaveOffs) {
                            i = OffRows - SaveOffs;
                        }

                        for (; i != replyRows->elements; i++) {
                            redisReply *replyRowkey = replyRows->element[i];

                            if (replyRowkey && replyRowkey->type == REDIS_REPLY_STRING && replyRowkey->len) {
                                // split rowkey str into vals with rowkeyfilters
                                if (RDBTableFilterRowkeyVals(resultmap->filter, resultmap->filter->patternprefixlen, replyRowkey->str, (int)replyRowkey->len, rkvals, rkvalslen) != rowkeynum) {
                                    // filter by rowkeyfilters failed
                                    continue;
                                }

                                if (RedisHMGetLen(ctx, replyRowkey->str, replyRowkey->len,
                                        resultmap->filter->getfieldnames, resultmap->filter->getfieldnameslen, &replyCols) == RDBAPI_SUCCESS) {

                                    if (RDBTableFilterReplyCols(resultmap->filter, replyCols) == fieldsnum) {
                                        RDBRow row = NULL;

                                        if (RDBRowNew(resultmap, replyRowkey->str, replyRowkey->len, &row) == RDBAPI_SUCCESS) {
                                            if (RDBResultMapInsertRow(resultmap, row) == RDBAPI_SUCCESS) {
                                                // set rowkey fields
                                                for (colindex = 0; colindex < rowkeynum; colindex++) {
                                                    RDBCellSetString(RDBRowCell(row, colindex), rkvals[colindex], rkvalslen[colindex]);
                                                }

                                                // set attr fields
                                                for (colindex = 0; colindex < resultmap->filter->selfieldnum; colindex++) {
                                                    replyCol = replyCols->element[colindex];

                                                    if (RDBCellSetReply(RDBRowCell(row, rowkeynum + colindex), replyCol)) {
                                                        replyCols->element[colindex] = NULL;
                                                    }
                                                }
                                            } else {
                                                // failed on duplicated rowkey
                                                RDBRowFree(row);
                                            }
                                        }
                                    }

                                    RedisFreeReplyObject(&replyCols);
                                }
                            }
                        }
                    }

                    RedisFreeReplyObject(&replyRows);
                }
            } else {
                nodeindex++;
            }
        }
    }

return_offset:
    return LastOffs;
}


RDBAPI_RESULT RDBTableCreate (RDBCtx ctx, const char *tablespace, const char *tablename, const char *tablecomment, int nfields, RDBFieldDes_t *fieldes)
{
    RDBAPI_RESULT  result;

    int rowkeyid[RDBAPI_SQL_KEYS_MAX + 1] = {0};

    int tablespacelen = cstr_length(tablespace, RDB_KEY_NAME_MAXLEN + 1);
    int tablenamelen = cstr_length(tablename, RDB_KEY_NAME_MAXLEN + 1);
    int commentlen = cstr_length(tablecomment, RDB_KEY_VALUE_SIZE - 1);

    char table_rowkey[RDB_KEY_NAME_MAXLEN * 3];
    char creatdt[30] = {0};
    char numfields[10] = {0};

    tpl_bin outbin = {0};

    const char * fields[] = {"numfields", "fieldes", "creatdt", "comment", 0};
    const char * values[5];

    size_t valueslen[5];

    if (tablespacelen < 2 || tablespacelen > RDB_KEY_NAME_MAXLEN) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: invalid tablespace: %s", tablespace);
        return RDBAPI_ERR_BADARG;
    }
    if (tablenamelen < 4 || tablenamelen > RDB_KEY_NAME_MAXLEN) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: invalid tablename: %s", tablename);
        return RDBAPI_ERR_BADARG;
    }

    if (! RDBFieldDesCheckSet(ctx->env->valtype_chk_table, fieldes, nfields, rowkeyid, ctx->errmsg, sizeof(ctx->errmsg))) {
        return RDBAPI_ERROR;
    }

    // {redisdb::$tablespace:$tablename}
    snprintf_chkd_V1(table_rowkey, sizeof(table_rowkey), "{%s::%.*s:%.*s}", RDB_SYSTEM_TABLE_PREFIX, tablespacelen, tablespace, tablenamelen, tablename);

    if (RDBFieldDesPack(fieldes, nfields, &outbin) != 0) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) RDBAPI_ERROR: RDBFieldDesPack failed.", __FILE__, __LINE__);
        return RDBAPI_ERROR;
    }

    RDBCurrentTime(RDBAPI_TIMESPEC_SEC, creatdt);

    values[0] = numfields;
    values[1] = (char *) outbin.addr;
    values[2] = creatdt;
    values[3] = tablecomment;
    values[4] = 0;

    valueslen[0] = snprintf_chkd_V1(numfields, sizeof(numfields), "%d", nfields);
    valueslen[1] = outbin.sz;
    valueslen[2] = cstr_length(creatdt, sizeof(creatdt));
    valueslen[3] = commentlen;
    valueslen[4] = 0;

    result = RedisHMSet(ctx, table_rowkey, fields, values, valueslen, RDBAPI_KEY_PERSIST);
    free(outbin.addr);
    return result;
}


RDBAPI_RESULT RDBTableDescribe (RDBCtx ctx, const char *tablespace, const char *tablename, RDBTableDes_t *tabledes)
{
    ub8 u8val;
    redisReply *tableReply = NULL;

    const char *fldnames[] = {"numfields", "fieldes", "creatdt", "comment", 0};

    bzero(tabledes, sizeof(*tabledes));

    if (! strcmp(tablespace, RDB_SYSTEM_TABLE_PREFIX)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) RDBAPI_ERROR: system tablespace: %s", __FILE__, __LINE__, tablespace);
        return RDBAPI_ERROR;
    }

    // hmget {redisdb::$tablespace:$tablename} numfields fieldes creatdt comment
    snprintf_chkd_V1(tabledes->table_rowkey, sizeof(tabledes->table_rowkey) - 1, "{%s::%s:%s}", RDB_SYSTEM_TABLE_PREFIX, tablespace, tablename);

    if (RedisHMGet(ctx, tabledes->table_rowkey, fldnames, &tableReply) != RDBAPI_SUCCESS) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERROR: key not found: %s", tabledes->table_rowkey);
        return RDBAPI_ERROR;
    }

    u8val = 0;
    cstr_to_ub8(10, tableReply->element[0]->str, tableReply->element[0]->len, &u8val);
    tabledes->nfields = (int) u8val;

    if (!tableReply->element[1]->str || ! tableReply->element[1]->len || ! tabledes->nfields) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERROR: invalid fieldes: %s", tabledes->table_rowkey);
        RedisFreeReplyObject(&tableReply);
        return RDBAPI_ERROR;
    }

    if (! RDBFieldDesUnpack(tableReply->element[1]->str, tableReply->element[1]->len, tabledes->fielddes, tabledes->nfields)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERROR: RDBFieldDesUnpack: %s", tabledes->table_rowkey);
        RedisFreeReplyObject(&tableReply);
        return RDBAPI_ERROR;
    }

    snprintf_chkd_V1(tabledes->table_datetime, sizeof(tabledes->table_datetime), "%.*s", (int)tableReply->element[2]->len, tableReply->element[2]->str);
    snprintf_chkd_V1(tabledes->table_comment, sizeof(tabledes->table_comment), "%.*s", (int)tableReply->element[3]->len, tableReply->element[3]->str);

    RedisFreeReplyObject(&tableReply);

    if (! RDBFieldDesCheckSet(ctx->env->valtype_chk_table, tabledes->fielddes, tabledes->nfields, tabledes->rowkeyid, ctx->errmsg, sizeof(ctx->errmsg))) {
        return RDBAPI_ERROR;
    }

    return RDBAPI_SUCCESS;
}
