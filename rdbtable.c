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
#include "rdbapi.h"
#include "rdbtypes.h"

#define RDBTABLE_FILTER_ACCEPT    1
#define RDBTABLE_FILTER_REJECT    0
#define RDBTABLE_FILTER_FAILED  (-1)


typedef struct _RDBNameReply_t
{
    char *name;

    redisReply *nameReply;
    redisReply *valueReply;

    // makes this structure hashable
    UT_hash_handle hh;
} RDBNameReply_t, *RDBNameReply, *RDBNameReplyMap;


typedef struct _RDBKeyValue_t
{
    RDBSqlExpr expr;
    RDBValueType vtype;

    int klen;
    char key[RDB_KEY_VALUE_SIZE];

    int vlen;
    char val[0];
} RDBKeyVal_t, *RDBKeyVal, *RDBKeyValMap;


typedef struct _RDBTableCursor_t
{
    ub8 cursor;
    ub8 offset;

    ub4 nodeindex;
    int finished;
} RDBTableCursor_t, * RDBTableCursor;


typedef struct _RDBTableSql_t
{
    // "{$tablespace::$tablename:$key}"
    char tablespace[RDB_KEY_NAME_MAXLEN + 1];
    char tablename[RDB_KEY_NAME_MAXLEN + 1];

    RDBKeyVal keyfilters[RDBAPI_SQL_KEYS_MAX + 1];

    RDBKeyVal fldfilters[RDBAPI_SQL_FIELDS_MAX + 1];

    // result cursor state
    RDBTableCursor_t nodestates[RDB_CLUSTER_NODES_MAX];

    RDBBlob_t patternblob;

    char blob[0];
} RDBTableSql_t;


#define RDBResultNodeState(resMap, nodeid)  (&(resMap->rdbsql->nodestates[nodeid]))


static RDBAPI_RESULT RDBTableSqlCreate (RDBTableSql *rdbsql, ub4 blobsz)
{
    RDBTableSql hsql = (RDBTableSql) RDBMemAlloc(sizeof(RDBTableSql_t) + blobsz);

    if (! hsql) {
        return RDBAPI_ERR_NOMEM;
    }

    hsql->patternblob.str = hsql->blob;
    hsql->patternblob.length = 0;
    hsql->patternblob.maxsz = blobsz;

    hsql->keyfilters[RDBAPI_SQL_KEYS_MAX] = NULL;
    hsql->fldfilters[RDBAPI_SQL_FIELDS_MAX] = NULL;

    *rdbsql = hsql;
    return RDBAPI_SUCCESS;
}


static void RDBTableSqlFree (RDBTableSql rdbsql)
{
    int i;
    RDBKeyVal node;

    i = 0;
    node = rdbsql->keyfilters[i++];
    while (node) {
        RDBMemFree(node);
        node = rdbsql->keyfilters[i++];
    }

    i = 0;
    node = rdbsql->fldfilters[i++];
    while (node) {
        RDBMemFree(node);
        node = rdbsql->fldfilters[i++];
    }

    RDBMemFree(rdbsql);
}


static RDBAPI_RESULT RDBTableSqlInit (RDBTableSql rdbsql,
    const char *tablespace, const char *tablename,
    int numkeys, const char *keys[], const RDBSqlExpr keyexprs[], const char *keyvals[],
    int numfields, const char *fields[], const RDBSqlExpr fieldexprs[], const char *fieldvals[],
    const char *groupby[], const char *orderby[],
    int nfielddes, const RDBFieldDesc *tabledes)
{
    int k, len, offlen = 0;
    size_t sz, valsz;

    char * str = rdbsql->patternblob.str;
    ub4 maxsz = rdbsql->patternblob.maxsz;

    snprintf(rdbsql->tablespace, RDB_KEY_NAME_MAXLEN, "%s", tablespace);
    snprintf(rdbsql->tablename, RDB_KEY_NAME_MAXLEN, "%s", tablename);

    rdbsql->tablespace[RDB_KEY_NAME_MAXLEN] = 0;    
    rdbsql->tablename[RDB_KEY_NAME_MAXLEN] = 0;

    // validate and set keys
    for (k = 0; k < numkeys && k < RDBAPI_SQL_KEYS_MAX; k++) {
        RDBKeyVal keynode;
        RDBSqlExpr expr = RDBSQLEX_IGNORE;

        if (!keys || !keys[k]) {
            return RDBAPI_ERR_BADARG;
        }

        sz = strnlen(keys[k], RDB_KEY_VALUE_SIZE);
        if (sz == 0 || sz == RDB_KEY_VALUE_SIZE) {
            return RDBAPI_ERR_BADARG;
        }

        if (keyexprs) {
            expr = keyexprs[k];
        }

        valsz = 0;
        if (expr) {
            if (! keyvals || ! keyvals[k]) {
                return RDBAPI_ERR_BADARG;
            }

            valsz = strnlen(keyvals[k], RDB_KEY_VALUE_SIZE);
            if (valsz == RDB_KEY_VALUE_SIZE) {
                return RDBAPI_ERR_BADARG;
            }
        }

        keynode = (RDBKeyVal) mem_alloc_zero(1, sizeof(RDBKeyVal_t) + (valsz? (valsz + 1) : 0));

        keynode->expr = expr;

        memcpy(keynode->key, keys[k], sz);
        keynode->klen = (int) sz;

        if (valsz) {
            memcpy(keynode->val, keyvals[k], valsz);
            keynode->vlen = (int) valsz;
        }

        if (nfielddes) {
            int fldindex = RDBTableFindField(tabledes, nfielddes, keynode->key, keynode->klen);
            if (fldindex != -1) {
                keynode->vtype = tabledes[fldindex].fieldtype;
            }
        }

        rdbsql->keyfilters[k] = keynode;
    }

    // validate and set fields
    for (k = 0; k < numfields && k < RDBAPI_SQL_FIELDS_MAX; k++) {
        RDBKeyVal fldnode;
        RDBSqlExpr expr = 0;

        if (!fields || !fields[k]) {
            return RDBAPI_ERR_BADARG;
        }

        sz = strnlen(fields[k], RDB_KEY_VALUE_SIZE);
        if (sz == 0 || sz == RDB_KEY_VALUE_SIZE) {
            return RDBAPI_ERR_BADARG;
        }

        if (fieldexprs) {
            if (! fieldexprs[k]) {
                return RDBAPI_ERR_BADARG;
            }
            expr = fieldexprs[k];
        }

        valsz = 0;
        if (fieldvals) {
            if (! fieldvals[k]) {
                return RDBAPI_ERR_BADARG;
            }
            valsz = strnlen(fieldvals[k], RDB_KEY_VALUE_SIZE);
            if (valsz == RDB_KEY_VALUE_SIZE) {
                return RDBAPI_ERR_BADARG;
            }
        }

        fldnode = (RDBKeyVal) mem_alloc_zero(1, sizeof(RDBKeyVal_t) + (valsz? (valsz + 1) : 0));

        fldnode->expr = expr;

        memcpy(fldnode->key, fields[k], sz);
        fldnode->klen = (int) sz;

        if (valsz) {
            memcpy(fldnode->val, fieldvals[k], valsz);
            fldnode->vlen = (int) valsz;
        }

        if (nfielddes) {
            int fldindex = RDBTableFindField(tabledes, nfielddes, fldnode->key, fldnode->klen);
            if (fldindex != -1) {
                fldnode->vtype = tabledes[fldindex].fieldtype;
            }
        }

        rdbsql->fldfilters[k] = fldnode;
    }

    if (groupby) {
        // TODO:
    }

    if (orderby) {
        // TODO:
    }

    if (numkeys > 0) {
        int expr;

        len = snprintf(str + offlen, maxsz - offlen, "{%s::%s", rdbsql->tablespace, rdbsql->tablename);
        if (len == maxsz - offlen) {
            return RDBAPI_ERR_NOBUF;
        }
        offlen += len;

        for (k = 0; k < numkeys; k++) {
            if (maxsz - offlen < 2) {
                return RDBAPI_ERR_NOBUF;
            }

            if (keyexprs) {
                expr = keyexprs[k];

                if (keyvals) {
                    switch (expr) {
                    case RDBSQLEX_EQUAL:
                    case RDBSQLEX_REG_MATCH:
                        len = snprintf(str + offlen, maxsz - offlen, ":%s", keyvals[k]);
                        break;

                    case RDBSQLEX_LEFT_LIKE:
                        len = snprintf(str + offlen, maxsz - offlen, ":%s*", keyvals[k]);
                        break;

                    case RDBSQLEX_RIGHT_LIKE:
                        len = snprintf(str + offlen, maxsz - offlen, ":*%s", keyvals[k]);
                        break;

                    case RDBSQLEX_LIKE:
                        len = snprintf(str + offlen, maxsz - offlen, ":*%s*", keyvals[k]);
                        break;

                    case RDBSQLEX_IGNORE:
                    case RDBSQLEX_NOT_EQUAL:
                    case RDBSQLEX_GREAT_THAN:
                    case RDBSQLEX_LESS_THAN:
                    case RDBSQLEX_GREAT_EQUAL:
                    case RDBSQLEX_LESS_EQUAL:
                    default:
                        len = snprintf(str + offlen, maxsz - offlen, ":*");
                        break;
                    }
                } else {
                    len = snprintf(str + offlen, maxsz - offlen, ":*");
                }
            } else {
                if (keyvals) {
                    len = snprintf(str + offlen, maxsz - offlen, ":%s", keyvals[k]);
                } else {
                    len = snprintf(str + offlen, maxsz - offlen, ":*");
                }
            }

            if (len == maxsz - offlen) {
                return RDBAPI_ERR_NOBUF;
            }
            offlen += len;
        }

        len = snprintf(str + offlen, maxsz - offlen, "}");
    } else {
        len = snprintf(str + offlen, maxsz - offlen, "{%s::%s:*}", rdbsql->tablespace, rdbsql->tablename);
    }

    if (len == maxsz - offlen) {
        return RDBAPI_ERR_NOBUF;
    }
    offlen += len;

    if (maxsz - offlen < 1) {
        return RDBAPI_ERR_NOBUF;
    }

    // all is ok
    rdbsql->patternblob.length = offlen;
    rdbsql->patternblob.str[offlen] = '\0';

    bzero(rdbsql->nodestates, sizeof(rdbsql->nodestates[0]) * RDB_CLUSTER_NODES_MAX);

    return RDBAPI_SUCCESS;
}


static int RedisReplyNodeCompare (void *newObject, void *nodeObject)
{
    if (newObject == nodeObject) {
        return 0;
    } else {
        redisReply *A = (redisReply *)newObject;
        redisReply *B = (redisReply *)nodeObject;

        return cstr_compare_len(A->str, A->len, B->str, B->len);
    }
}


static void RedisReplyNodeDelete (void *nodeobj, void *arg)
{
    freeReplyObject((redisReply *) nodeobj);
}


// build key format like:
//  {tablespace::tablename:$fieldname1:$fieldname2}
int RDBBuildKeyFormat (const char * tablespace, const char * tablename, const RDBFieldDesc *fielddes, int numfields, int *rowkeyid, char **keyformat)
{
    int offlen = 0;
    char *keyprefix = NULL;
        
    int j, k;
    size_t szlen = 0;

    szlen += strlen(tablespace) + 4;
    szlen += strlen(tablename) + 4;

    for (j = 0; j < numfields; j++) {
        if (fielddes[j].rowkey) {
            rowkeyid[fielddes[j].rowkey] = j;
            rowkeyid[0] = rowkeyid[0] + 1;
        }
    }

    for (k = 1; k <= rowkeyid[0]; k++) {
        j = rowkeyid[k];

        szlen += strlen(fielddes[j].fieldname) + 4;
    }

    keyprefix = (char *) RDBMemAlloc(szlen + 1);

    offlen += snprintf(keyprefix + offlen, szlen - offlen, "{%s::%s", tablespace, tablename);

    for (k = 1; k <= rowkeyid[0]; k++) {
        j = rowkeyid[k];

        offlen += snprintf(keyprefix + offlen, szlen - offlen, ":$%s", fielddes[j].fieldname);
    }

    offlen += snprintf(keyprefix + offlen, szlen - offlen, "}");
    keyprefix[offlen] = 0;

    *keyformat = keyprefix;
    return offlen;
}


RDBAPI_RESULT RDBResultMapNew (RDBTableSql rdbsql, int numfields, const RDBFieldDesc *fielddes, RDBResultMap *phResultMap)
{
    RDBResultMap hMap = (RDBResultMap) RDBMemAlloc(sizeof(RDBResultMap_t) + sizeof(RDBFieldDesc) * numfields);
    if (! hMap) {
        return RDBAPI_ERR_NOMEM;
    }

    hMap->rdbsql = rdbsql;

    RDBResultMapSetDelimiter(hMap, RDB_TABLE_DELIMITER_CHAR);

    rbtree_init(&hMap->rbtree, (fn_comp_func*) RedisReplyNodeCompare);

    if (numfields) {
        hMap->kplen = RDBBuildKeyFormat(rdbsql->tablespace, rdbsql->tablename, fielddes, numfields, hMap->rowkeyid, &hMap->keyprefix);

        memcpy(hMap->fielddes, fielddes, sizeof(RDBFieldDesc) * numfields);
        hMap->numfields = numfields;
    }

    *phResultMap = hMap;
    return RDBAPI_SUCCESS;
}


void RDBResultMapFree (RDBResultMap hResultMap)
{
    RDBMemFree(hResultMap->keyprefix);

    if (hResultMap->rdbsql) {
        RDBTableSqlFree(hResultMap->rdbsql);
    }

    RDBResultMapClean(hResultMap);

    RDBMemFree(hResultMap);
}


void RDBResultMapClean (RDBResultMap hResultMap)
{
    rbtree_traverse(&hResultMap->rbtree, RedisReplyNodeDelete, 0);
    rbtree_clean(&hResultMap->rbtree);
}


ub8 RDBResultMapSize (RDBResultMap hResultMap)
{
    return (ub8) rbtree_size(&hResultMap->rbtree);
}


RDBAPI_RESULT RDBResultMapInsert (RDBResultMap hResultMap, redisReply *reply)
{
    red_black_node_t * node;
    int is_new_node = 0;

    node = rbtree_insert_unique(&hResultMap->rbtree, (void *) reply, &is_new_node);
    if (! node) {
        // out of memory
        return RDBAPI_ERR_NOMEM;
    }

    if (! is_new_node) {
        return RDBAPI_ERR_EXISTED;
    }

    return RDBAPI_SUCCESS;
}


void RDBResultMapDelete (RDBResultMap hResultMap, redisReply *reply)
{
    rbtree_remove(&hResultMap->rbtree, reply);

    freeReplyObject(reply);
}


RDBAPI_BOOL RDBResultMapExist (RDBResultMap hResultMap, redisReply *reply)
{
    red_black_node_t * node;

    node = rbtree_find(&hResultMap->rbtree, (void *) reply);

    return (node? RDBAPI_TRUE : RDBAPI_FALSE);
}


void RDBResultMapTraverse (RDBResultMap hResultMap, void (onReplyNodeTraverseCb)(void *, void *), void *arg)
{
    rbtree_traverse(&hResultMap->rbtree, onReplyNodeTraverseCb, arg);
}


ub8 RDBResultMapGetOffset (RDBResultMap hResultMap)
{
    ub8 total = 0;

    int nodeindex = RDBEnvNumNodes(RDBCtxGetEnv(hResultMap->ctxh));

    while (nodeindex-- > 0) {
        total += RDBResultNodeState(hResultMap, nodeindex)->offset;
    }

    return total;
}


static RDBAPI_RESULT RDBTableScanFirstInternal (int isdesc, RDBCtx ctx,
    const char *tablespace,  // must valid
    const char *tablename,   // must valid
    int numkeys,
    const char *keys[],
    const RDBSqlExpr keyexprs[],
    const char *keyvals[],
    int numfields,
    const char *fields[],
    const RDBSqlExpr fieldexprs[],
    const char *fieldvals[],
    const char *groupby[],    // Not Supported Now!
    const char *orderby[],    // Not Supported Now!
    RDBResultMap *phResultMap)
{
    int nodeindex;
    RDBAPI_RESULT res;
    RDBResultMap resultMap = NULL;
    RDBTableSql rdbsql = NULL;

    int nfields = 0;
    RDBFieldDesc fielddes[RDBAPI_ARGV_MAXNUM];

    if (numkeys > RDBAPI_SQL_KEYS_MAX) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: too many keys");
        return RDBAPI_ERR_BADARG;
    }

    if (numfields > RDBAPI_SQL_FIELDS_MAX) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: too many fields");
        return RDBAPI_ERR_BADARG;
    }

    if (RedisClusterCheck(ctx) != RDBAPI_SUCCESS) {
        return RDBAPI_ERROR;
    }

    res = RDBTableSqlCreate(&rdbsql, RDBAPI_SQL_PATTERN_SIZE);
    if (res != RDBAPI_SUCCESS) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERROR(%d): RDBTableSqlCreate failed", res);
        return RDBAPI_ERROR;
    }

    if (! isdesc) {
        bzero(fielddes, sizeof(fielddes));

        nfields = RDBTableDesc(ctx, tablespace, tablename, fielddes);

        if (nfields <= 0) {
            return RDBAPI_ERROR;
        }
    }

    res = RDBTableSqlInit(rdbsql,
            tablespace, tablename,
            numkeys, keys, keyexprs, keyvals,
            numfields, fields, fieldexprs, fieldvals,
            groupby, orderby,
            nfields, fielddes);

    if (res != RDBAPI_SUCCESS) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERROR(%d): RDBTableSqlInit failed", res);
        return RDBAPI_ERROR;
    }

    // MUST after RDBTableSqlInit
    if (RDBResultMapNew(rdbsql, nfields, fielddes, &resultMap) != RDBAPI_SUCCESS) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_NOMEM: out of memory");
        RDBTableSqlFree(rdbsql);
        return RDBAPI_ERR_NOMEM;
    }

    resultMap->ctxh = ctx;

    for (nodeindex = 0; nodeindex < RDBEnvNumNodes(ctx->env); nodeindex++) {
        RDBResultNodeState(resultMap, nodeindex)->nodeindex = nodeindex;
    }

    *phResultMap = resultMap;
    return RDBAPI_SUCCESS;
}


RDBAPI_RESULT RDBTableScanFirst (RDBCtx ctx,
    const char *tablespace,  // must valid
    const char *tablename,   // must valid
    int numkeys,
    const char *keys[],
    const RDBSqlExpr keyexprs[],
    const char *keyvals[],
    int numfields,
    const char *fields[],
    const RDBSqlExpr fieldexprs[],
    const char *fieldvals[],
    const char *groupby[],    // Not Supported Now!
    const char *orderby[],    // Not Supported Now!
    RDBResultMap *phResultMap)
{
   return RDBTableScanFirstInternal(0, ctx,
            tablespace,
            tablename,
            numkeys,
            keys,
            keyexprs,
            keyvals,
            numfields,
            fields,
            fieldexprs,
            fieldvals,
            groupby,
            orderby,
            phResultMap);
}


// internal api only on single node
//
static RDBAPI_RESULT RDBTableScanOnNode (RDBCtxNode ctxnode, RDBTableCursor nodestate, const RDBBlob_t *patternblob, ub4 maxlimit, redisReply **outReply)
{
    RDBAPI_RESULT result;

    size_t argvlen[6];

    char cursor[22];
    char count[12];

    redisReply *reply = NULL;

    *outReply = NULL;

    // construct scan clause
    argvlen[0] = 4; // scan
    argvlen[1] = snprintf(cursor, 22, "%"PRIu64, nodestate->cursor);
    cursor[21] = 0;

    if (patternblob) {
        argvlen[2] = 5; // match
        argvlen[3] = patternblob->length;

        argvlen[4] = 5; // count
        argvlen[5] = snprintf(count, 12, "%u", maxlimit);
        count[11] = 0;

        const char *argv[] = {
            "scan",
            cursor,
            "match",
            patternblob->str,
            "count",
            count,
            0
        };

        result = RedisExecArgvOnNode(ctxnode, 6, argv, argvlen, &reply);
    } else {
        argvlen[2] = 5; // count
        argvlen[3] = snprintf(count, 12, "%d", maxlimit);
        count[11] = 0;

        const char *argv[] = {
            "scan",
            cursor,
            "count",
            count,
            0
        };

        result = RedisExecArgvOnNode(ctxnode, 4, argv, argvlen, &reply);
    }

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
            snprintf(ctxnode->ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERROR: reply type(%d).", reply->type);
            ctxnode->ctx->errmsg[RDB_ERROR_MSG_LEN] = 0;
            RedisFreeReplyObject(&reply);
            return RDBAPI_ERROR;
        }
    }

    // no data returned, expect next
    return RDBAPI_CONTINUE;
}


static int RDBTableFilterReplyByKey (RDBResultMap resultMap, redisReply * reply)
{
    if (resultMap->rdbsql->keyfilters[0]) {
        // accept row
        return 1;
    } else {
        // TODO:
        return 1;
    }
}


static int RDBTableFilterReplyByField (RDBResultMap resultMap, redisReply * reply)
{
    redisReply * replyRow = NULL;
    RDBCtx ctx = resultMap->ctxh;

    if (! resultMap->rdbsql->fldfilters[0]) {
        // accept reply as key
        return RDBTABLE_FILTER_ACCEPT;
    } else {
        RDBNameReplyMap fields = RDBTableFetchFields(resultMap->ctxh, reply->str);

        if (fields) {
            // filter fields
            int i, ret;
            RDBKeyVal fldfilter;

            i = 0;
            fldfilter = resultMap->rdbsql->fldfilters[i++];
            while (fldfilter) {
                RDBNameReply fieldnode = 0;

                HASH_FIND_STR(fields, fldfilter->key, fieldnode);

                if (! fieldnode) {
                    // reject row if required field not found
                    RDBFieldsMapFree(fields);
                    return RDBTABLE_FILTER_REJECT;
                }

                ret = RDBExprValues(fldfilter->vtype,
                    fieldnode->valueReply->str, fieldnode->valueReply->len,  /* src */
                    fldfilter->expr, fldfilter->val, fldfilter->vlen);       /* dst */

                if (ret != 1) {
                    // reject row on failed expr: required field not found
                    RDBFieldsMapFree(fields);
                    return RDBTABLE_FILTER_REJECT;
                }

                fldfilter = resultMap->rdbsql->fldfilters[i++];
            }

            // accept row: all fileds ok
            RDBFieldsMapFree(fields);
            return RDBTABLE_FILTER_ACCEPT;
        }

        // on error
        return RDBTABLE_FILTER_FAILED;
    }
}


static void RDBTableFilterCallback (void *object, void *arg)
{
    // RDBTableFilterReplyByKey((RDBResultMap) arg, (redisReply *) object);

    // RDBTableFilterReplyByField((RDBResultMap) arg, (redisReply *) object);
}


static RDBAPI_RESULT RDBResultMapFilterInsert (RDBResultMap resultMap, redisReply *reply)
{
    if (RDBTableFilterReplyByKey(resultMap, reply) == RDBTABLE_FILTER_ACCEPT &&
        RDBTableFilterReplyByField(resultMap, reply) == RDBTABLE_FILTER_ACCEPT) {
        return RDBResultMapInsert(resultMap, reply);
    }

    // rejected
    return RDBAPI_ERROR;
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
ub8 RDBTableScanNext (RDBResultMap hResultMap, ub8 OffRows, ub4 limit)
{
    RDBAPI_RESULT   result;
    RDBTableCursor  nodestate;
    redisReply     *replyRows;

    int numMasters = 0;
    int finMasters = 0;

    // number of rows now to fetch
    sb8 CurRows;

    // number of rows we fetched
    ub8 NumRows = 0;

    int nodeindex;

    // max rows returned
    ub4 LmtRows = limit;

    if (limit == (ub4) -1) {
        LmtRows = RDB_TABLE_LIMIT_MAX;
    } else if (limit < RDB_TABLE_LIMIT_MIN) {
        LmtRows = RDB_TABLE_LIMIT_MIN;
    } else if (limit > RDB_TABLE_LIMIT_MAX) {
        LmtRows = RDB_TABLE_LIMIT_MAX;
    }

    // redis context
    RDBCtx ctx = hResultMap->ctxh;

    // get rows from all nodes
    ub8 LastOffs = RDBResultMapGetOffset(hResultMap);
    if (LastOffs >= OffRows + LmtRows) {
        return LastOffs;
    }

    // get number of finished nodes
    for (nodeindex = 0; nodeindex < RDBEnvNumNodes(ctx->env); nodeindex++) {
        RDBEnvNode envnode = RDBEnvGetNode(ctx->env, nodeindex);

        if (RDBEnvNodeGetMaster(envnode, NULL) == RDBAPI_TRUE) {
            numMasters++;

            nodestate = RDBResultNodeState(hResultMap, nodeindex);
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
        RDBEnvNode envnode = RDBEnvGetNode(ctx->env, nodeindex);

        if (RDBEnvNodeGetMaster(envnode, NULL) == RDBAPI_TRUE) {
            RDBCtxNode ctxnode = RDBCtxGetNode(ctx, nodeindex);
            ub8 SaveOffs = LastOffs;

            // scan only on master node
            nodestate = RDBResultNodeState(hResultMap, nodeindex);

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

            result = RDBTableScanOnNode(ctxnode, nodestate, &hResultMap->rdbsql->patternblob, (ub4) CurRows, &replyRows);

            if (result == RDBAPI_SUCCESS) {
                LastOffs = RDBResultMapGetOffset(hResultMap);

                if (LastOffs > OffRows) {
                    // here we should add new rows
                    size_t i = 0;

                    if (OffRows > SaveOffs) {
                        i = OffRows - SaveOffs;
                    }

                    for (; i != replyRows->elements; i++) {
                        redisReply * reply = replyRows->element[i];

                        if (RDBResultMapFilterInsert(hResultMap, reply) == RDBAPI_SUCCESS) {
                            replyRows->element[i] = NULL;
                            NumRows++;
                        }
                    }
                }

                RedisFreeReplyObject(&replyRows);
            }
        } else {
            nodeindex++;
        }
    }

    return LastOffs;
}


RDBAPI_RESULT RDBTableCreate (RDBCtx ctx, const char *tablespace, const char *tablename, const char *tablecomment, int numfields, RDBFieldDesc fielddefs[])
{
    int i;
    int rowkeys[RDBAPI_SQL_KEYS_MAX + 1];

    // validate table and fields
    if (numfields < 1 || numfields > RDBAPI_ARGV_MAXNUM) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: invalid numfields(%d)", numfields);
        return RDBAPI_ERR_BADARG;
    }

    if (! tablespace || strnlen(tablespace, RDB_KEY_NAME_MAXLEN + 1) > RDB_KEY_NAME_MAXLEN) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: invalid tablespace");
        return RDBAPI_ERR_BADARG;
    }

    if (! tablename || strnlen(tablename, RDB_KEY_NAME_MAXLEN + 1) > RDB_KEY_NAME_MAXLEN) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: invalid tablename");
        return RDBAPI_ERR_BADARG;
    }

    if (tablecomment && strnlen(tablecomment, RDB_KEY_VALUE_SIZE) == RDB_KEY_VALUE_SIZE) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: invalid tablecomment");
        return RDBAPI_ERR_BADARG;
    }

    bzero(rowkeys, sizeof(rowkeys));

    for (i = 0; i < numfields; i++) {
        RDBFieldDesc *flddef = &fielddefs[i];

        if (flddef->rowkey < 0) {
            snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: bad rowkey(%d)", flddef->rowkey);
            return RDBAPI_ERR_BADARG;
        }

        if (flddef->rowkey > RDBAPI_SQL_KEYS_MAX) {
            snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: too many keys");
            return RDBAPI_ERR_BADARG;
        }

        if (flddef->rowkey > 0) {
            if (flddef->nullable) {
                snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: rowkey(%d) field is nullable: %s", flddef->rowkey, flddef->fieldname);
                return RDBAPI_ERR_BADARG;
            }

            if (rowkeys[flddef->rowkey]) {
                snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: duplicate rowkeys(%d) for field: %s", flddef->rowkey, flddef->fieldname);
                return RDBAPI_ERR_BADARG;
            }

            rowkeys[flddef->rowkey] = i + 1;
            rowkeys[0] = rowkeys[0] + 1;
        }

        if (flddef->length < 0) {
            snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: invalid length(%d) for field: %s", flddef->length, flddef->fieldname);
            return RDBAPI_ERR_BADARG;
        }

        if (flddef->length > RDB_FIELD_LENGTH_MAX) {
            snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: too big length(%d) for field: %s", flddef->length, flddef->fieldname);
            return RDBAPI_ERR_BADARG;
        }

        if (flddef->fieldtype == RDBVTYPE_FLT64) {
            if (flddef->length > RDB_FIELD_FLT_LENGTH_MAX) {
                snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: too big length(%d) for float field: %s", flddef->length, flddef->fieldname);
                return RDBAPI_ERR_BADARG;
            }

            if (flddef->dscale < RDB_FIELD_FLT_SCALE_MIN) {
                snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: invalid dscale(%d) for float field: %s", flddef->dscale, flddef->fieldname);
                return RDBAPI_ERR_BADARG;
            }
            if (flddef->dscale > RDB_FIELD_FLT_SCALE_MAX) {
                snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: too big dscale(%d) for float field: %s", flddef->dscale, flddef->fieldname);
                return RDBAPI_ERR_BADARG;
            }
        }
    }

    for (i = 1; i <= rowkeys[0]; i++) {
        if (! rowkeys[i]) {
            snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: rowkey(%d) not found", i);
            return RDBAPI_ERR_BADARG;
        }
    }

    do {
        RDBAPI_RESULT  result;

        char key[RDB_KEY_NAME_MAXLEN * 4 + 8];
        char timesec[22];
        char datetimestr[30];
        char emptystr[] = "";

        const char * fields[11];
        const char *values[11];

        // {redisdb::$tablespace:$tablename}
        snprintf(key, sizeof(key), "{%.*s::%s:%s}", RDB_KEY_NAME_MAXLEN, RDB_SYSTEM_TABLE_PREFIX, tablespace, tablename);
        key[sizeof(key) - 1] = 0;

        snprintf(timesec, sizeof(timesec), "%"PRIu64, RDBCurrentTime(RDBAPI_TIMESPEC_SEC, datetimestr));
        timesec[21] = 0;

        fields[0] = "ct";   // create time in seconds
        fields[1] = "dt";   // create date in form: "YYYY-MM-DD hh:mm:ss"
        fields[2] = "comment";
        fields[3] = 0;

        values[0] = timesec;
        values[1] = datetimestr;
        values[2] = (tablecomment ? tablecomment : emptystr);
        values[3] = 0;

        result = RedisHMSet(ctx, key, fields, values, NULL, RDBAPI_KEY_PERSIST);
        if (result == RDBAPI_SUCCESS) {
            char fieldid[4];
            char rowkey[4];
            char length[12];
            char dscale[12];
            char fieldtype[2] = {'\0', '\0'};
            char nullable[2] = {'1', '\0'};

            for (i = 0; i < numfields; i++) {
                RDBFieldDesc *flddef = &fielddefs[i];

                snprintf(fieldid, sizeof(fieldid), "%d", i+1);
                snprintf(rowkey, sizeof(rowkey), "%d", flddef->rowkey);                
                snprintf(length, sizeof(length), "%d", flddef->length);
                snprintf(dscale, sizeof(dscale), "%d", flddef->dscale);

                fieldtype[0] = (char) flddef->fieldtype;

                if (! flddef->nullable) {
                    nullable[0] = '0';
                }

                // {redisdb::$tablespace:$tablename:$fieldname}
                snprintf(key, sizeof(key), "{%.*s::%s:%s:%s}", RDB_KEY_NAME_MAXLEN, RDB_SYSTEM_TABLE_PREFIX, tablespace, tablename, flddef->fieldname);
                key[sizeof(key) - 1] = 0;

                fields[0] = "ct";
                fields[1] = "dt";
                fields[2] = "fieldname";
                fields[3] = "fieldtype";
                fields[4] = "length";
                fields[5] = "dscale";
                fields[6] = "rowkey";
                fields[7] = "nullable";
                fields[8] = "fieldid";      // 1-based
                fields[9] = "comment";
                fields[10] = 0;

                values[0] = timesec;
                values[1] = datetimestr;
                values[2] = flddef->fieldname;
                values[3] = fieldtype;
                values[4] = length;
                values[5] = dscale;
                values[6] = rowkey;
                values[7] = nullable;
                values[8] = fieldid;
                values[9] = flddef->comment;
                values[10] = 0;

                result = RedisHMSet(ctx, key, fields, values, NULL, RDBAPI_KEY_PERSIST);
                if (result != RDBAPI_SUCCESS) {
                    break;
                }
            }
        }

        return result;
    } while(0);
}


typedef struct {
    RDBCtx ctx;
    RDBFieldDesc *tabledes;
} RDBTableDescCallbackArg;


static void RDBTableDescCallback (void * pvReply, void *pvArg)
{
    redisReply *replyRow;

    redisReply *reply = (redisReply *) pvReply;

    RDBTableDescCallbackArg *descarg = (RDBTableDescCallbackArg *) pvArg;

    const char *argv[2];
    size_t argl[2];

    argv[0] = "hgetall";
    argv[1] = reply->str;

    argl[0] = 7;
    argl[1] = reply->len;

    replyRow = RedisExecCommand(descarg->ctx, 2, argv, argl);
    if (replyRow && replyRow->type == REDIS_REPLY_ARRAY && replyRow->elements) {
        size_t i = 0;
        int fieldid = 0;

        RDBFieldDesc flddes = {0};

        while (i < replyRow->elements) {
            redisReply *fieldName = replyRow->element[i++];
            redisReply *fieldValue = replyRow->element[i++];

            if (fieldName && fieldValue && fieldName->type == REDIS_REPLY_STRING && fieldValue->type == REDIS_REPLY_STRING && fieldValue->len) {
                if (! flddes.fieldname[0] &&
                    ! cstr_notequal_len(fieldName->str, fieldName->len, "fieldname", 9) ) {

                    flddes.fieldname[0] = 0;
                    if (fieldValue->len < sizeof(flddes.fieldname)) {
                        strncpy(flddes.fieldname, fieldValue->str, fieldValue->len);
                    } else {
                        strncpy(flddes.fieldname, fieldValue->str, sizeof(flddes.fieldname) - 1);
                    }
                    flddes.fieldname[sizeof(flddes.fieldname) - 1] = 0;
                } else if (! fieldid &&
                           ! cstr_notequal_len(fieldName->str, fieldName->len, "fieldid", 7) ) {

                    fieldid = atoi(fieldValue->str);
                } else if (! flddes.fieldtype &&
                           ! cstr_notequal_len(fieldName->str, fieldName->len, "fieldtype", 9) ) {

                    if (fieldValue->len == 1) {
                        flddes.fieldtype = (RDBValueType) fieldValue->str[0];
                    }
                } else if (! cstr_notequal_len(fieldName->str, fieldName->len, "length", 6) ) {

                    flddes.length = atoi(fieldValue->str);
                } else if (! cstr_notequal_len(fieldName->str, fieldName->len, "dscale", 6) ) {

                    flddes.dscale = atoi(fieldValue->str);
                } else if (! cstr_notequal_len(fieldName->str, fieldName->len, "rowkey", 6)) {

                    flddes.rowkey = atoi(fieldValue->str);
                } else if (! cstr_notequal_len(fieldName->str, fieldName->len, "nullable", 8)) {

                    flddes.nullable = atoi(fieldValue->str);
                } else if (! flddes.comment[0] &&
                           ! cstr_notequal_len(fieldName->str, fieldName->len, "comment", 7)) {

                    flddes.comment[0] = 0;
                    if (fieldValue->len < sizeof(flddes.comment)) {
                        strncpy(flddes.comment, fieldValue->str, fieldValue->len);
                    } else {
                        strncpy(flddes.comment, fieldValue->str, sizeof(flddes.comment) - 1);
                    }
                    flddes.comment[sizeof(flddes.comment) - 1] = 0;
                }
            }
        }

        if (fieldid > 0) {
            memcpy(&descarg->tabledes[fieldid - 1], &flddes, sizeof(flddes));
        }
    }

    RedisFreeReplyObject(&replyRow);
}


int RDBTableDesc (RDBCtx ctx, const char *tablespace, const char *tablename, RDBFieldDesc tabledes[RDBAPI_ARGV_MAXNUM])
{
    RDBAPI_RESULT result;
    RDBResultMap resultMap;

    const char *keys[] = {
        "tablename",
        "fieldname"
    };

    RDBSqlExpr exprs[] = {
        RDBSQLEX_EQUAL,
        RDBSQLEX_IGNORE
    };

    const char * vals[] = {
        tablename,
        0
    };

    result = RDBTableScanFirstInternal(1, ctx, RDB_SYSTEM_TABLE_PREFIX, tablespace, 2, keys, exprs, vals, 0, 0, 0, 0, 0, 0, &resultMap);
    if (result == RDBAPI_SUCCESS) {
        int ifld = 0;

        const RDBValueType vtypes[] = {
            RDBVTYPE_SB2,
            RDBVTYPE_UB2,
            RDBVTYPE_SB4,
            RDBVTYPE_UB4,
            RDBVTYPE_UB4X,
            RDBVTYPE_SB8,
            RDBVTYPE_UB8,
            RDBVTYPE_UB8X,
            RDBVTYPE_CHAR,
            RDBVTYPE_BYTE,
            RDBVTYPE_STR,
            RDBVTYPE_FLT64,
            RDBVTYPE_BIN,
            RDBVTYPE_DEC
        };

        ub8 offset = 0;
        ub8 fields = 0;

        while ((offset = RDBTableScanNext(resultMap, offset, RDBAPI_ARGV_MAXNUM + 1)) != RDB_ERROR_OFFSET) {
            fields += RDBResultMapSize(resultMap);

            if (fields > RDBAPI_ARGV_MAXNUM) {
                snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERROR: too many fields");

                RDBResultMapFree(resultMap);
                return RDBAPI_ERROR;
            }
        }

        if (! fields) {
            snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERROR: table not found: {%s::%s:%s:*}",
                RDB_SYSTEM_TABLE_PREFIX, tablespace, tablename);

            RDBResultMapFree(resultMap);
            return RDBAPI_ERROR;
        }

        RDBTableDescCallbackArg tdescarg = {ctx, tabledes};

        RDBResultMapTraverse(resultMap, RDBTableDescCallback, &tdescarg);

        RDBResultMapFree(resultMap);

        for (; ifld < (int) fields; ifld++) {
            int j = 0;
            RDBFieldDesc *flddes = &tabledes[ifld];

            if (! flddes->fieldname[0]) {
                snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERROR: null fieldname");
                return RDBAPI_ERROR;
            }
           
            for (j = 0; j < sizeof(vtypes)/sizeof(vtypes[0]); j++) {
                if (flddes->fieldtype == vtypes[j]) {
                    break;
                }
            }

            if (j == sizeof(vtypes)/sizeof(vtypes[0])) {
                snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERROR: invalid fieldtype(%d) for field: %s", (int) flddes->fieldtype, flddes->fieldname);
                return RDBAPI_ERROR;
            }
        }

        return (int) fields;
    }

    return result;
}


int RDBTableFindField (const RDBFieldDesc fields[RDBAPI_ARGV_MAXNUM], int numfields, const char *fieldname, int fieldnamelen)
{
    int i = 0;

    for (; i < numfields; i++) {

        if (! cstr_notequal_len(fieldname, fieldnamelen, fields[i].fieldname, (int)strnlen(fields[i].fieldname, RDB_KEY_NAME_MAXLEN))) {
            return i;
        }
    }

    return (-1);
}


RDBFieldsMap RDBTableFetchFields (RDBCtx ctx, const char *rowkey)
{
    redisReply * replyRow = NULL;

    if (RedisHMGet(ctx, rowkey, NULL, &replyRow) == RDBAPI_SUCCESS) {
        size_t i = 0;

        RDBNameReplyMap fieldsmap = NULL;

        while (i < replyRow->elements) {
            RDBNameReply fldnode = (RDBNameReply) mem_alloc_zero(1, sizeof(RDBNameReply_t));

            fldnode->nameReply = replyRow->element[i];
            replyRow->element[i++] = NULL;

            fldnode->valueReply = replyRow->element[i];
            replyRow->element[i++] = NULL;

            fldnode->name = fldnode->nameReply->str;

            HASH_ADD_STR(fieldsmap, name, fldnode);
        }

        RedisFreeReplyObject(&replyRow);

        return fieldsmap;
    }

    // key not found
    return NULL;
}


redisReply * RDBFieldsMapGetField (RDBFieldsMap fields, const char *fieldname)
{
    RDBNameReply fieldnode = 0;

    HASH_FIND_STR(fields, fieldname, fieldnode);

    if (fieldnode) {
        return fieldnode->valueReply;
    }

    return NULL;
}


void RDBFieldsMapFree (RDBFieldsMap fields)
{
    if (fields) {
        RDBNameReply curnode, tmpnode;

        HASH_ITER(hh, fields, curnode, tmpnode) {
            RedisFreeReplyObject(&curnode->nameReply);
            RedisFreeReplyObject(&curnode->valueReply);

            HASH_DEL(fields, curnode);

            mem_free(curnode);
        }
    }
}


static int ResultMapParseRowkeyValues (RDBResultMap resultMap, const char *rowkeystr, int rowkeylen)
{
    // default set it error
    int i = -1;

    if (resultMap->keyprefix) {
        char *p = strstr(resultMap->keyprefix, ":$");
        if (p) {
            if (cstr_startwith(rowkeystr, rowkeylen, resultMap->keyprefix, (int)(p - resultMap->keyprefix + 1))) {
                // skip over prefix ok
                const char *val = &rowkeystr[p - resultMap->keyprefix + 1];
                const char *end = strchr(val, ':');

                i = 0;

                while (val) {
                    resultMap->rowkey_offs[i] = (int) (val - rowkeystr);

                    if (end) {
                        resultMap->rowkey_lens[i] = (int) (end - val);
                    } else {
                        resultMap->rowkey_lens[i] = (int) (rowkeylen - resultMap->rowkey_offs[i] - 1);
                    }

                    i++;

                    val = strchr(val, ':');
                    if (val) {
                        end = strchr(++val, ':');
                    }
                }
            }
        }
    }

    if (i == resultMap->rowkeyid[0]) {
        // parse ok
        return i;
    }

    return (-1);
}


static void RDBResultMapPrintOutCallback (void *result, void *_arg)
{
    int i;
    int num;

    RDBResultMap resultMap = (RDBResultMap)_arg;

    redisReply *reply = (redisReply *) result;

    RDBFieldsMap fieldsamp = RDBTableFetchFields(resultMap->ctxh, reply->str);

    num = ResultMapParseRowkeyValues(resultMap, reply->str, reply->len);
    for (i = 0; i < num; i++) {
        if (i > 0) {
            printf("%c%.*s", resultMap->delimiter, resultMap->rowkey_lens[i], reply->str + resultMap->rowkey_offs[i]);
        } else {
            printf("%.*s", resultMap->rowkey_lens[i], reply->str + resultMap->rowkey_offs[i]);
        }
    }

    num = 0;
    for (i = 0; i < resultMap->numfields; i++) {
        if (! resultMap->fielddes[i].rowkey) {
            // print value for fields
            redisReply * fieldValue = RDBFieldsMapGetField(fieldsamp, resultMap->fielddes[i].fieldname);

            if (num) {
                if (fieldValue && fieldValue->str) {
                    printf("%c%.*s", resultMap->delimiter, fieldValue->len, fieldValue->str);
                } else {
                    printf("%c(null)", resultMap->delimiter);
                }
            } else {
                if (fieldValue && fieldValue->str) {
                    printf("%.*s", fieldValue->len, fieldValue->str);
                } else {
                    printf("(null)");
                }
            }

            num++;
        }
    }

    printf("\n");

    RDBFieldsMapFree(fieldsamp);
}


char RDBResultMapSetDelimiter (RDBResultMap hResultMap, char delimiter)
{
    char oldDelimiter = hResultMap->delimiter;

    hResultMap->delimiter = delimiter;

    if (! hResultMap->delimiter) {
        hResultMap->delimiter = RDB_TABLE_DELIMITER_CHAR;
    }

    return oldDelimiter;
}


void RDBResultMapPrintOut (RDBResultMap resultMap, RDBAPI_BOOL withHead)
{
    int i = 0;

    if (withHead) {
        size_t len = 0;

        printf("\n# Keys Prefix: %.*s", resultMap->kplen, resultMap->keyprefix);

        if (resultMap->rdbsql) {
            printf("\n# Last Offset: %"PRIu64, RDBResultMapGetOffset(resultMap));
        }

        if (resultMap->rbtree.iSize) {
            printf("\n# Num of Rows: %"PRIu64, RDBResultMapSize(resultMap));
        }

        printf("\n");

        for (i = 1; i <= resultMap->rowkeyid[0]; i++) {
            int j = resultMap->rowkeyid[i];

            if (! len) {
                printf("#[ $%s", resultMap->fielddes[j].fieldname);
            } else {
                printf(" %c $%s", resultMap->delimiter, resultMap->fielddes[j].fieldname);
            }

            len += strlen(resultMap->fielddes[j].fieldname) + 4;
        }

        for (i = 0; i < resultMap->numfields; i++) {
            if (! resultMap->fielddes[i].rowkey) {
                if (! len) {
                    printf("#[ %s", resultMap->fielddes[i].fieldname);
                } else {
                    printf(" %c %s", resultMap->delimiter, resultMap->fielddes[i].fieldname);
                }

                len += strlen(resultMap->fielddes[i].fieldname) + 3;
            }
        }
        printf(" ]\n#--");

        i = 1;
        while (i++ < len) {
            printf("-");
        }
        printf("\n");
    }

    RDBResultMapTraverse(resultMap, RDBResultMapPrintOutCallback, resultMap);
}