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
#include "rdbresultmap.h"

#define RDBTABLE_FILTER_ACCEPT    1
#define RDBTABLE_FILTER_REJECT    0


typedef struct _RDBKeyValue_t
{
    RDBFilterExpr expr;
    RDBValueType vtype;

    int klen;
    char key[RDB_KEY_VALUE_SIZE];

    int vlen;
    char val[0];
} RDBKeyVal_t, *RDBKeyVal, *RDBKeyValMap;


typedef struct _RDBTableFilter_t
{
    // "{$tablespace::$tablename:$key}"
    char tablespace[RDB_KEY_NAME_MAXLEN + 1];
    char tablename[RDB_KEY_NAME_MAXLEN + 1];

    // number of rowkey in keyfilters
    int patternkeys;
    RDBKeyVal keyfilters[RDBAPI_SQL_KEYS_MAX + 1];

    RDBKeyVal fldfilters[RDBAPI_SQL_FIELDS_MAX + 1];

    // result cursor state
    RDBTableCursor_t nodestates[RDB_CLUSTER_NODES_MAX];

    // rowkey pattern
    RDBBlob_t patternblob;
    char blob[0];
} RDBTableFilter_t;


#define RDBResultNodeState(resMap, nodeid)  (&(resMap->filter->nodestates[nodeid]))


static RDBAPI_RESULT RDBTableFilterCreate (RDBTableFilter *filter, ub4 blobsz)
{
    RDBTableFilter hsql = (RDBTableFilter) RDBMemAlloc(sizeof(RDBTableFilter_t) + blobsz);

    if (! hsql) {
        return RDBAPI_ERR_NOMEM;
    }

    hsql->patternblob.str = hsql->blob;
    hsql->patternblob.length = 0;
    hsql->patternblob.maxsz = blobsz;

    hsql->keyfilters[RDBAPI_SQL_KEYS_MAX] = NULL;
    hsql->fldfilters[RDBAPI_SQL_FIELDS_MAX] = NULL;

    *filter = hsql;
    return RDBAPI_SUCCESS;
}


static void RDBTableFilterFree (RDBTableFilter filter)
{
    int i;
    RDBKeyVal node;

    i = 0;
    node = filter->keyfilters[i++];
    while (node) {
        RDBMemFree(node);
        node = filter->keyfilters[i++];
    }

    i = 0;
    node = filter->fldfilters[i++];
    while (node) {
        RDBMemFree(node);
        node = filter->fldfilters[i++];
    }

    RDBMemFree(filter);
}


static int RDBValidateRowkeyValue (const char *value, int vlen, int maxsize)
{
    return 1;
}


static int RDBFieldIsRowid (RDBResultMap hMap, int fieldindex)
{
    int k;

    for (k = 1; k <= hMap->rowkeyid[0]; k++) {
        if (fieldindex == hMap->rowkeyid[k]) {
            // in rowkey
            return k;
        }
    }

    // not in rowkey
    return 0;
}


static int RDBFieldGetRowid (int rowkeyid[], int fieldindex)
{
    int k;

    for (k = 1; k <= rowkeyid[0]; k++) {
        if (fieldindex == rowkeyid[k]) {
            // in rowkey
            return k;
        }
    }

    // not in rowkey
    return 0;
}


static int RDBFieldIsRowkey (const char * fieldname, const RDBFieldDes_t *fielddes, int nfields)
{
    // check if fieldname is a rowkey
    int k;
    for (k = 0; k < nfields; k++) {
        if (! strcmp(fieldname, fielddes[k].fieldname) && fielddes[k].rowkey) {
            return (k + 1);
        }
    }

    return 0;
}


static RDBAPI_RESULT RDBTableFilterInit (RDBTableFilter filter,
    const char *tablespace, const char *tablename, const char *desctable,
    int numkeys, const char *keys[], const RDBFilterExpr keyexprs[], const char *keyvals[],
    int numfields, const char *fields[], const RDBFilterExpr fieldexprs[], const char *fieldvals[],
    const char *groupby[], const char *orderby[],
    int nfielddes, const RDBFieldDes_t *fielddes)
{
    int i, j, k, len, offlen = 0;
    size_t sz, valsz;

    snprintf_chkd_V1(filter->tablespace, sizeof(filter->tablespace), "%s", tablespace);
    snprintf_chkd_V1(filter->tablename, sizeof(filter->tablename), "%s", tablename);

    // validate and set keys
    filter->patternkeys = 0;
    for (k = 0; k < numkeys && k < RDBAPI_SQL_KEYS_MAX; k++) {
        RDBFilterExpr expr = keyexprs[k];

        if (expr) {
            RDBKeyVal keynode;

            sz = strnlen(keys[k], RDB_KEY_VALUE_SIZE);
            if (sz == 0 || sz == RDB_KEY_VALUE_SIZE) {
                return RDBAPI_ERR_BADARG;
            }

            valsz = strnlen(keyvals[k], RDB_KEY_VALUE_SIZE);
            if (valsz == RDB_KEY_VALUE_SIZE) {
                return RDBAPI_ERR_BADARG;
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
                int fldindex = RDBTableFindField(fielddes, nfielddes, keynode->key, keynode->klen);
                if (fldindex != -1) {
                    keynode->vtype = fielddes[fldindex].fieldtype;
                }
            }

            filter->keyfilters[filter->patternkeys++] = keynode;
        }
    }

    // validate and set fields
    i = 0, j = filter->patternkeys;
    for (k = 0; k < numfields && k < RDBAPI_SQL_FIELDS_MAX; k++) {
        RDBFilterExpr expr = fieldexprs[k];

        if (expr) {
            RDBKeyVal fldnode;
           
            sz = cstr_length(fields[k], RDB_KEY_VALUE_SIZE);
            if (sz == 0 || sz == RDB_KEY_VALUE_SIZE) {
                return RDBAPI_ERR_BADARG;
            }

            valsz = cstr_length(fieldvals[k], RDB_KEY_VALUE_SIZE);
            if (valsz == RDB_KEY_VALUE_SIZE) {
                return RDBAPI_ERR_BADARG;
            }

            fldnode = (RDBKeyVal) RDBMemAlloc(sizeof(RDBKeyVal_t) + (valsz? (valsz + 1) : 0));

            fldnode->expr = expr;

            memcpy(fldnode->key, fields[k], sz);
            fldnode->klen = (int) sz;

            if (valsz) {
                memcpy(fldnode->val, fieldvals[k], valsz);
                fldnode->vlen = (int) valsz;
            }

            if (nfielddes) {
                int fldindex = RDBTableFindField(fielddes, nfielddes, fldnode->key, fldnode->klen);
                if (fldindex != -1) {
                    fldnode->vtype = fielddes[fldindex].fieldtype;
                }
            }

            if (RDBFieldIsRowkey(fields[k], fielddes, numfields)) {
                filter->keyfilters[j++] = fldnode;
            } else {
                filter->fldfilters[i++] = fldnode;
            }
        }
    }

    if (groupby) {
        // TODO:
    }

    if (orderby) {
        // TODO:
    }

    if (nfielddes > 0) {
        // make rowkey format
        char *keypattern = NULL;
        int rowkeyid[RDBAPI_KEYS_MAXNUM + 1] = {0};

        RDBKeyVal kvnode;
        char pattern[RDB_KEY_NAME_MAXLEN + 2] = {0};
        
        len = RDBBuildRowkeyPattern(tablespace, tablename, fielddes, nfielddes, rowkeyid, &keypattern);

        i = 0;
        while ((kvnode = filter->keyfilters[i++]) != NULL) {
            if (kvnode->expr >= RDBFIL_EQUAL && kvnode->expr < RDBFIL_NOT_EQUAL) {
                if (RDBValidateRowkeyValue(kvnode->val, kvnode->vlen, RDB_KEY_VALUE_SIZE)) {
                    char repvalue[RDB_KEY_VALUE_SIZE + 4] = {0};

                    switch (kvnode->expr) {
                    case RDBFIL_EQUAL:
                    case RDBFIL_MATCH:
                        snprintf_chkd_V1(repvalue, RDB_KEY_VALUE_SIZE, "%.*s", kvnode->vlen, kvnode->val);
                        break;

                    case RDBFIL_LEFT_LIKE:
                        snprintf_chkd_V1(repvalue, RDB_KEY_VALUE_SIZE + 1, "%.*s*", kvnode->vlen, kvnode->val);
                        break;

                    case RDBFIL_RIGHT_LIKE:
                        snprintf_chkd_V1(repvalue, RDB_KEY_VALUE_SIZE + 1, "*%.*s", kvnode->vlen, kvnode->val);
                        break;

                    case RDBFIL_LIKE:
                        snprintf_chkd_V1(repvalue, RDB_KEY_VALUE_SIZE + 2, "*%.*s*", kvnode->vlen, kvnode->val);
                        break;
                    }

                    if (snprintf_chkd_V1(pattern, sizeof(pattern), "$%.*s", kvnode->klen, kvnode->key) < sizeof(pattern)) {
                        char * newstr;
                        int newlen = cstr_replace_new(keypattern, pattern, repvalue, &newstr);
                        if (newlen) {
                            RDBMemFree(keypattern);
                            keypattern = newstr;
                        }
                        kvnode->expr = RDBFIL_IGNORE;
                    }
                }
            }
        }

        len = RDBFinishRowkeyPattern(fielddes, nfielddes, rowkeyid, &keypattern);
        if (len >= (int)filter->patternblob.maxsz) {
            RDBMemFree(keypattern);
            return RDBAPI_ERR_NOBUF;
        }

        memcpy(filter->patternblob.str, keypattern, len + 1);
        filter->patternblob.length = len;

        RDBMemFree(keypattern);
    } else {
        if (desctable) {
            // {redisdb::xsdb:logentry:*}
            len = snprintf_chkd_V1(filter->patternblob.str, filter->patternblob.maxsz, "{%s::%s:%s:*}", filter->tablespace, filter->tablename, desctable);
        } else {
            len = snprintf_chkd_V1(filter->patternblob.str, filter->patternblob.maxsz, "{%s::%s:*}", filter->tablespace, filter->tablename);
        }

        if (len >= (int)filter->patternblob.maxsz) {
            return RDBAPI_ERR_NOBUF;
        }
        filter->patternblob.length = len;
    }

    // patternblob is ok

    bzero(filter->nodestates, sizeof(filter->nodestates[0]) * RDB_CLUSTER_NODES_MAX);

    return RDBAPI_SUCCESS;
}


static int RDBResultRowNodeCompare (void *newObject, void *nodeObject)
{
    if (newObject == nodeObject) {
        return 0;
    } else {
        RDBResultRow A = (RDBResultRow) newObject;
        RDBResultRow B = (RDBResultRow) nodeObject;

        return cstr_compare_len(A->replykey->str, A->replykey->len, B->replykey->str, B->replykey->len);
    }
}


void RDBResultRowFree (RDBResultRow rowdata)
{
//DEL    RedisFreeReplyObject(&rowdata->replykey);
//DEL    RDBFieldsMapFree(rowdata->fieldmap);

//DEL    RDBMemFree(rowdata);
}


static void RDBResultRowNodeDelete (void *rownode, void *arg)
{
     RDBResultRowFree((RDBResultRow) rownode);
}


static int RDBRowkeyPatternParse (const char *keypattern, const char *rkVal, int rkValLen, int rowkeys, int rkValOffs[RDBAPI_SQL_KEYS_MAX], int rkValLens[RDBAPI_SQL_KEYS_MAX])
{
    // default set it error
    int i = -1;

    char *p = strstr(keypattern, ":$");
    if (p) {
        if (cstr_startwith(rkVal, rkValLen, keypattern, (int)(p - keypattern + 1))) {
            // skip over prefix ok
            const char *val = & rkVal[p - keypattern + 1];
            const char *end = strchr(val, ':');

            i = 0;

            while (val) {
                rkValOffs[i] = (int) (val - rkVal);

                if (end) {
                    rkValLens[i] = (int) (end - val);
                } else {
                    rkValLens[i] = (int) (rkValLen - rkValOffs[i] - 1);
                }

                i++;

                val = strchr(val, ':');
                if (val) {
                    end = strchr(++val, ':');
                }
            }
        }
    }

    if (i == rowkeys) {
        // parse ok
        return i;
    }

    return (-1);
}


// build key pattern like:
//  {tablespace::tablename:$fieldname1:$fieldname2}
int RDBBuildRowkeyPattern (const char * tablespace, const char * tablename,
    const RDBFieldDes_t *fielddes, int numfields,
    int rowkeyid[RDBAPI_KEYS_MAXNUM + 1],
    char **outRowkeyPattern)
{
    int offlen = 0;
    char *rowkey = NULL;

    int j, k;
    size_t szlen = 0;

    szlen += strlen(tablespace) + 4;
    szlen += strlen(tablename) + 4;

    bzero(rowkeyid, sizeof(int) * (RDBAPI_KEYS_MAXNUM + 1));

    for (j = 0; j < numfields; j++) {
        k = fielddes[j].rowkey;

        if (k > 0 && k <= RDBAPI_KEYS_MAXNUM) {
            if (! rowkeyid[ k ]) {
                rowkeyid[ k ] = j;
                rowkeyid[0] += 1;
            }
        }
    }

    for (k = 1; k <= rowkeyid[0]; k++) {
        j = rowkeyid[k];

        szlen += strlen(fielddes[j].fieldname) + 4;
    }

    rowkey = (char *) RDBMemAlloc(szlen + 1);

    offlen += snprintf_chkd_V1(rowkey + offlen, szlen - offlen, "{%s::%s", tablespace, tablename);

    for (k = 1; k <= rowkeyid[0]; k++) {
        j = rowkeyid[k];

        offlen += snprintf_chkd_V1(rowkey + offlen, szlen - offlen, ":$%s", fielddes[j].fieldname);
    }

    offlen += snprintf_chkd_V1(rowkey + offlen, szlen - offlen, "}");
    rowkey[offlen] = 0;

    *outRowkeyPattern = rowkey;
    return offlen;
}


int RDBFinishRowkeyPattern (const RDBFieldDes_t *fielddes, int nfielddes, const int rowkeyid[RDBAPI_KEYS_MAXNUM + 1], char **rowkeypattern)
{
    int i, j, l, len = 0;
    char *result;
    char pattern[RDB_KEY_NAME_MAXLEN + 2] = {0};

    char *rowkey = *rowkeypattern;

    for (i = 1; i <= rowkeyid[0]; i++) {
        j = rowkeyid[i];

        snprintf_chkd_V1(pattern, sizeof(pattern), "$%.*s", fielddes[j].namelen, fielddes[j].fieldname);

        l = cstr_replace_new(rowkey, pattern, "*", &result);
        if (l) {
            RDBMemFree(rowkey);
            rowkey = result;
            len = l;
        }
    }

    *rowkeypattern = rowkey;
    return cstr_length(rowkey, RDB_ROWKEY_MAX_SIZE);
}


RDBResultFilter RDBResultFilterNew (RDBCtx ctx, RDBTableFilter filter, RDBSQLStmtType stmt, int numfields, const RDBFieldDes_t *fielddes, ub1 resultfields[])
{
    int i, j, k;

    RDBResultFilter res = (RDBResultFilter) RDBMemAlloc(sizeof(RDBResultFilter_t) + sizeof(RDBFieldDes_t) * numfields);

    if (! res) {
        exit(EXIT_FAILURE);
    }

    res->ctx = ctx;
    res->filter = filter;
    res->sqlstmt = stmt;

    res->kplen = RDBBuildRowkeyPattern(filter->tablespace, filter->tablename, fielddes, numfields, res->rowkeyid, &res->keypattern);

    memcpy(res->fielddes, fielddes, sizeof(RDBFieldDes_t) * numfields);
    res->numfields = numfields;

    if (resultfields) {
        // set all of result fields
        k = 0;
        for (i = 0; i < resultfields[0]; i++) {
            j = resultfields[i + 1] - 1;

            if (! RDBFieldGetRowid(res->rowkeyid, j)) {
                res->fetchfields[k] = res->fielddes[j].fieldname;
                res->fieldnamelens[k] = cstr_length(res->fielddes[j].fieldname, RDB_KEY_NAME_MAXLEN);

                k++;
            }
        }

        res->resultfields = k;
    }

    RDBKeyVal fldfilter;

    // set all of filter fields
    i = 0;
    while ((fldfilter = res->filter->fldfilters[i++]) != NULL) {
        int found = 0;

        for (j = 0; res->fetchfields[j] != NULL; j++) {
            if (! strcmp(fldfilter->key, res->fetchfields[j])) {
                // skip existed
                found = 1;
                break;
            }
        }

        if (! found) {
            res->fetchfields[j] = fldfilter->key;
            res->fieldnamelens[j] = fldfilter->klen;
        }
    }

    res->fetchfields[RDBAPI_ARGV_MAXNUM] = NULL;
    res->fieldnamelens[RDBAPI_ARGV_MAXNUM] = 0;

    return res;
}


void RDBResultFilterFree (RDBResultFilter resfilter)
{
    if (resfilter) {
        RDBMemFree(resfilter->keypattern);
        RDBTableFilterFree(resfilter->filter);
        RDBMemFree(resfilter);
    }
}


void RDBResultMapNew (RDBCtx ctx, RDBTableFilter filter, RDBSQLStmtType sqlstmt, const char *tablespace, const char *tablename, int numfields, const RDBFieldDes_t *fielddes, ub1 *resultfields, RDBResultMap *phResultMap)
{
    RDBResultMap hMap = (RDBResultMap) RDBMemAlloc(sizeof(RDBResultMap_t));
    if (! hMap) {
        fprintf(stderr, "(%s:%d) out of memory.\n", __FILE__, __LINE__);
        exit(EXIT_FAILURE);
    }

    hMap->ctxh = ctx;
    hMap->filter = filter;
    hMap->sqlstmt = sqlstmt;

    rbtree_init(&hMap->rbtree, (fn_comp_func*) RDBResultRowNodeCompare);

    if (numfields) {
        int i, j, k;

        hMap->kplen = RDBBuildRowkeyPattern(tablespace, tablename, fielddes, numfields, hMap->rowkeyid, &hMap->keypattern);

        memcpy(hMap->fielddes, fielddes, sizeof(RDBFieldDes_t) * numfields);
        hMap->numfields = numfields;

        if (resultfields) {
            // set all of result fields
            k = 0;
            for (i = 0; i < resultfields[0]; i++) {
                j = resultfields[i + 1] - 1;

                if (! RDBFieldIsRowid(hMap, j)) {
                    hMap->fetchfields[k] = hMap->fielddes[j].fieldname;
                    hMap->fieldnamelens[k] = (int) strlen(hMap->fielddes[j].fieldname);
                    k++;
                }
            }
            hMap->resultfields = k;
        }

        if (filter) {
            RDBKeyVal fldfilter;

            // set all of filter fields
            i = 0;
            while ((fldfilter = hMap->filter->fldfilters[i++]) != NULL) {
                int found = 0;

                for (j = 0; hMap->fetchfields[j] != NULL; j++) {
                    if (! strcmp(fldfilter->key, hMap->fetchfields[j])) {
                        // skip existed
                        found = 1;
                        break;
                    }
                }

                if (! found) {
                    hMap->fetchfields[j] = fldfilter->key;
                    hMap->fieldnamelens[j] = fldfilter->klen;
                }
            }
        }

        hMap->fetchfields[RDBAPI_ARGV_MAXNUM] = NULL;
        hMap->fieldnamelens[RDBAPI_ARGV_MAXNUM] = 0;
    }

    *phResultMap = hMap;
}


RDBAPI_RESULT RDBResultMapInsert (RDBResultMap resultMap, redisReply *reply)
{
    int i, j, k, ret;

    red_black_node_t *node;
    RDBResultRow rowdata;

    RDBKeyVal kvfilter;

    const int rkcount = resultMap->rowkeyid[0];

    RDBNameReplyMap fieldmap = NULL;

    if (resultMap->keypattern) {
        // filter by keys
        int rkValOffs[RDBAPI_SQL_KEYS_MAX] = {0};
        int rkValLens[RDBAPI_SQL_KEYS_MAX] = {0};

        ret = RDBRowkeyPatternParse(resultMap->keypattern, reply->str, reply->len, rkcount, rkValOffs, rkValLens);
        if (ret == -1) {
            return RDBAPI_ERROR;
        }

        for (k = 1; k <= rkcount; k++) {
            RDBNameReply rknode;

            j = resultMap->rowkeyid[k];

            rknode = (RDBNameReply) RDBMemAlloc(sizeof(RDBNameReply_t));

            rknode->name = (char *) resultMap->fielddes[j].fieldname;
            rknode->namelen = resultMap->fielddes[j].namelen;

            rknode->subval = reply->str + rkValOffs[k-1];
            rknode->sublen = rkValLens[k-1];

            HASH_ADD_STR_LEN(fieldmap, name, rknode->namelen, rknode);
        }

        i = resultMap->filter->patternkeys;
        while ((kvfilter = resultMap->filter->keyfilters[i++]) != NULL) {
            if (kvfilter->expr) {
                RDBNameReply kvnode = NULL;
                HASH_FIND_STR_LEN(fieldmap, kvfilter->key, kvfilter->klen, kvnode);

                if (! kvnode) {
                    // reject row if required field not found
                    RDBFieldsMapFree(fieldmap);
                    return RDBAPI_ERROR;
                }

                ret = RDBExprValues(kvfilter->vtype,
                    kvnode->subval, kvnode->sublen,                       /* src */
                    kvfilter->expr, kvfilter->val, kvfilter->vlen);       /* dst */

                if (ret != 1) {
                    // reject row on failed expr
                    RDBFieldsMapFree(fieldmap);
                    return RDBAPI_ERROR;
                }
            }
        }
    }

    // filter by fields
    if (resultMap->sqlstmt == RDBSQL_SELECT ||
        resultMap->sqlstmt == RDBSQL_DELETE ||
        resultMap->sqlstmt == RDBSQL_UPSERT // TODO
    ) {
        fieldmap = RDBTableFetchFields(resultMap->ctxh, fieldmap, (const char **) resultMap->fetchfields, resultMap->fieldnamelens, reply->str);
    }

    if (fieldmap) {
        i = 0;
        while ((kvfilter = resultMap->filter->fldfilters[i++]) != NULL) {
            if (kvfilter->expr) {
                RDBNameReply kvnode = NULL;
                HASH_FIND_STR_LEN(fieldmap, kvfilter->key, kvfilter->klen, kvnode);

                if (! kvnode) {
                    // reject row if required field not found
                    RDBFieldsMapFree(fieldmap);
                    return RDBAPI_ERROR;
                }

                ret = RDBExprValues(kvfilter->vtype,
                    kvnode->value->str, kvnode->value->len,               /* src */
                    kvfilter->expr, kvfilter->val, kvfilter->vlen);       /* dst */

                if (ret != 1) {
                    // reject row on failed expr
                    RDBFieldsMapFree(fieldmap);
                    return RDBAPI_ERROR;
                }
            }
        }
    }

    if (resultMap->sqlstmt == RDBSQL_SELECT ||
        resultMap->sqlstmt == RDBSQL_DELETE ||
        resultMap->sqlstmt == RDBSQL_SHOW_DATABASES ||
        resultMap->sqlstmt == RDBSQL_SHOW_TABLES
    ) {
        // pass all filters ok
        rowdata = (RDBResultRow) RDBMemAlloc(sizeof(RDBResultRow_t ));

        rowdata->fieldmap = fieldmap;
        rowdata->replykey = reply;

        node = rbtree_insert_unique(&resultMap->rbtree, (void *) rowdata, &ret);

        if (! node) {
            // out of memory
            RDBResultRowFree(rowdata);
            return RDBAPI_ERR_NOMEM;
        }

        if (! ret) {
            RDBResultRowFree(rowdata);
            return RDBAPI_ERR_EXISTED;
        }

        return RDBAPI_SUCCESS;
    }

    // others:
    RedisFreeReplyObject(&reply);
    RDBFieldsMapFree(fieldmap);
    return RDBAPI_SUCCESS;
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


RDBSQLStmtType RDBResultMapGetStmt (RDBResultMap resultMap)
{
    return resultMap->sqlstmt;
}


RDBAPI_RESULT RDBTableScanFirst (RDBCtx ctx,
    RDBSQLStmtType stmt,
    const char *tablespace,   // must valid
    const char *tablename,    // must valid
    int numkeys,
    const char *keys[],
    RDBFilterExpr keyexprs[],
    const char *keyvals[],
    int numfields,
    const char *fields[],
    RDBFilterExpr fieldexprs[],
    const char *fieldvals[],
    const char *groupby[],    // Not Supported Now!
    const char *orderby[],    // Not Supported Now!
    int filedcount,
    const char *fieldnames[],
    RDBResultMap *phResultMap)
{
    int nodeindex, j, k;

    RDBAPI_RESULT res;

    RDBResultMap results = NULL;
    RDBResultMap resultMap = NULL;
    RDBTableFilter filter = NULL;

    RDBTableDes_t tabledes = {0};

    // 1-based index of result fields refer to tabledes.fielddes
    ub1 resultfields[RDBAPI_ARGV_MAXNUM + 1] = {0};

    if (numkeys > RDBAPI_SQL_KEYS_MAX) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: too many keys");
        return RDBAPI_ERR_BADARG;
    }

    if (numfields > RDBAPI_SQL_FIELDS_MAX) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: too many fields");
        return RDBAPI_ERR_BADARG;
    }

    if (RDBTableDescribe(ctx, tablespace, tablename, &tabledes) != RDBAPI_SUCCESS) {
        return RDBAPI_ERROR;
    }

    res = RDBTableFilterCreate(&filter, RDB_ROWKEY_MAX_SIZE);
    if (res != RDBAPI_SUCCESS) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERROR(%d): RDBTableFilterCreate failed", res);
        return RDBAPI_ERROR;
    }

    // get result fieldnames
    if (filedcount == -1) {
        // SELECT * FROM ...
        for (j = 0; j < tabledes.nfields; j++) {
            resultfields[j + 1] = (ub1) (j + 1);
        }
        resultfields[0] = tabledes.nfields;
    } else {
        for (j = 0; j < filedcount; j++) {
            for (k = 0; k < tabledes.nfields; k++) {
                if (! strcmp(tabledes.fielddes[k].fieldname, fieldnames[j])) {
                    resultfields[j + 1] = (ub1)(k + 1);
                    break;
                }
            }
        }
        resultfields[0] = filedcount;            
    }

    for (j = 1; j <= resultfields[0]; j++) {
        if (! resultfields[j]) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "field not found: %s", fieldnames[j - 1]);
            RDBTableFilterFree(filter);
            return RDBAPI_ERROR;
        }
    }

    if (tabledes.nfields) {
        int           CK_numkeys = 0;
        const char   *CK_keys[RDBAPI_SQL_KEYS_MAX] = {0};
        RDBFilterExpr CK_keyexprs[RDBAPI_SQL_KEYS_MAX] = {0};
        const char   *CK_keyvals[RDBAPI_SQL_KEYS_MAX] = {0};

        // check if fields[j] is rowkey, if it is, move it into keys
        for (j = 0; j < numfields; j++) {
            int rkid = 0;

            if (fieldexprs[j] >= RDBFIL_EQUAL && fieldexprs[j] < RDBFIL_NOT_EQUAL) {
                // check if fields[j] is a rowkey
                for (k = 0; k < tabledes.nfields; k++) {
                    if (! strcmp(fields[j], tabledes.fielddes[k].fieldname) && tabledes.fielddes[k].rowkey) {
                        rkid = k + 1;
                        break;
                    }
                }
            }

            if (rkid) {
                // move from fields[j] to keys:
                CK_keys[CK_numkeys] = fields[j];
                CK_keyexprs[CK_numkeys] = fieldexprs[j];
                CK_keyvals[CK_numkeys] = fieldvals[j];

                // ignore this field at all
                fieldexprs[j] = RDBFIL_IGNORE;

                CK_numkeys++;
            }
        }

        // merge keys with CK_keys
        for (k = 0; k < numkeys; k++) {
            if (CK_numkeys < RDBAPI_SQL_KEYS_MAX) {
                CK_keys[CK_numkeys] = keys[k];
                CK_keyexprs[CK_numkeys] = keyexprs[k];
                CK_keyvals[CK_numkeys] = keyvals[k];

                CK_numkeys++;
            }
        }

        res = RDBTableFilterInit(filter, tablespace, tablename, NULL,
                CK_numkeys, CK_keys, CK_keyexprs, CK_keyvals,
                numfields, fields, fieldexprs, fieldvals,
                groupby, orderby,
                tabledes.nfields, tabledes.fielddes);
    }

    if (res != RDBAPI_SUCCESS) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERROR(%d): RDBTableFilterInit failed", res);
        RDBTableFilterFree(filter);
        return RDBAPI_ERROR;
    }

    // create result map
    /*
    RDBResultFilter resfilter = RDBResultFilterNew(ctx, filter, stmt, tabledes.nfields, tabledes.fielddes, resultfields);
    RDBResultMapCreate(resfilter->resultfields, resfilter->fetchfields, &results);
    for (nodeindex = 0; nodeindex < RDBEnvNumNodes(ctx->env); nodeindex++) {
        RDBResultNodeState(resfilter, nodeindex)->nodeindex = nodeindex;
    }
    results->resfilter = resfilter;
    */

    //@@@ DEL???
    RDBResultMapNew(ctx, filter, stmt, filter->tablespace, filter->tablename, tabledes.nfields, tabledes.fielddes, resultfields, &resultMap);

    for (nodeindex = 0; nodeindex < RDBEnvNumNodes(ctx->env); nodeindex++) {
        RDBResultNodeState(resultMap, nodeindex)->nodeindex = nodeindex;
    }

    *phResultMap = resultMap;
    return RDBAPI_SUCCESS;
}


// internal api only on single node
//
RDBAPI_RESULT RDBTableScanOnNode (RDBCtxNode ctxnode, RDBTableCursor nodestate, const RDBBlob_t *patternblob, ub4 maxlimit, redisReply **outReply)
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

    if (patternblob) {
        argvlen[2] = 5; // match
        argvlen[3] = patternblob->length;

        argvlen[4] = 5; // count
        argvlen[5] = snprintf_chkd_V1(count, sizeof(count), "%u", maxlimit);
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
        argvlen[3] = snprintf_chkd_V1(count, sizeof(count), "%d", maxlimit);
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
ub8 RDBTableScanNext (RDBResultMap hResultMap, ub8 OffRows, ub4 limit)
{
    RDBAPI_RESULT   result;
    RDBTableCursor  nodestate;
    redisReply     *replyRows;

    RDBCtx ctx;

    int numMasters = 0;
    int finMasters = 0;

    // number of rows now to fetch
    sb8 CurRows;

    // number of rows we fetched
    ub8 NumRows = 0;

    ub8 LastOffs;

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
    ctx = hResultMap->ctxh;

    // get rows from all nodes
    LastOffs = RDBResultMapGetOffset(hResultMap);
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

        // scan only on master node
        if (RDBEnvNodeGetMaster(envnode, NULL) == RDBAPI_TRUE) {
            RDBCtxNode ctxnode = RDBCtxGetNode(ctx, nodeindex);
            ub8 SaveOffs = LastOffs;

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

            if (CurRows > 0) {
                result = RDBTableScanOnNode(ctxnode, nodestate, &hResultMap->filter->patternblob, (ub4) CurRows, &replyRows);
            } else {
                LastOffs = RDBResultMapGetOffset(hResultMap);
                goto return_offset;
            }

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

                        if (RDBResultMapInsert(hResultMap, reply) == RDBAPI_SUCCESS) {
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


typedef struct {
    RDBCtx ctx;
    int numfields;
    RDBFieldDes_t *fieldes;
} GetFieldDesCallbackArg;


static void onGetFieldDesCallback (void * pvRow, void *pvArg)
{
    redisReply *replyRow;

    redisReply *reply = ((RDBResultRow) pvRow)->replykey;

    GetFieldDesCallbackArg *descarg = (GetFieldDesCallbackArg *) pvArg;

    const char *argv[2];
    size_t argl[2];

    argv[0] = "hgetall";
    argv[1] = reply->str;

    argl[0] = 7;
    argl[1] = reply->len;

    replyRow = RedisExecCommand(descarg->ctx, 2, argv, argl);
    if (replyRow && replyRow->type == REDIS_REPLY_ARRAY && replyRow->elements) {
        size_t i = 0;

        RDBFieldDes_t flddes = {0};

        while (i < replyRow->elements) {
            redisReply *fieldName = replyRow->element[i++];
            redisReply *fieldValue = replyRow->element[i++];

            if (fieldName && fieldValue && fieldName->type == REDIS_REPLY_STRING && fieldValue->type == REDIS_REPLY_STRING && fieldValue->len) {
                if (! flddes.fieldname[0] && ! cstr_notequal_len(fieldName->str, fieldName->len, "fieldname", 9) ) {

                    flddes.namelen = snprintf_chkd_V1(flddes.fieldname, sizeof(flddes.fieldname), "%.*s", (int) fieldValue->len, fieldValue->str);
                } else if (! flddes.fieldtype && ! cstr_notequal_len(fieldName->str, fieldName->len, "fieldtype", 9) ) {

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
                } else if (! flddes.comment[0] && ! cstr_notequal_len(fieldName->str, fieldName->len, "comment", 7)) {

                    snprintf_chkd_V1(flddes.comment, sizeof(flddes.comment), "%.*s", (int)fieldValue->len, fieldValue->str);
                }
            }
        }

        memcpy(&descarg->fieldes[descarg->numfields++], &flddes, sizeof(flddes));
    }

    RedisFreeReplyObject(&replyRow);
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


int RDBTableFindField (const RDBFieldDes_t fields[RDBAPI_ARGV_MAXNUM], int numfields, const char *fieldname, int fieldnamelen)
{
    int i = 0;

    for (; i < numfields; i++) {

        if (! cstr_notequal_len(fieldname, fieldnamelen, fields[i].fieldname, (int)strnlen(fields[i].fieldname, RDB_KEY_NAME_MAXLEN))) {
            return i;
        }
    }

    return (-1);
}


RDBFieldsMap RDBTableFetchFields (RDBCtx ctx, RDBFieldsMap fieldsmap, const char *fieldnames[], int fieldnamelens[], const char *rowkey)
{
    redisReply * replyRow = NULL;

    if (RedisHMGet(ctx, rowkey, fieldnames, &replyRow) == RDBAPI_SUCCESS) {
        size_t i = 0;

        if (fieldnames && fieldnames[0]) {
            while (i < replyRow->elements && fieldnames[i]) {
                RDBNameReply replynode = (RDBNameReply) RDBMemAlloc(sizeof(RDBNameReply_t));

                replynode->name = (char *) fieldnames[i];
                replynode->namelen = fieldnamelens[i];

                replynode->value = replyRow->element[i];
                replyRow->element[i] = NULL;

                HASH_ADD_STR(fieldsmap, name, replynode);

                i += 1;
            }
        } else {
            while (i < replyRow->elements) {
                redisReply *nameRep = replyRow->element[i];

                RDBNameReply replynode = (RDBNameReply) RDBMemAlloc(sizeof(RDBNameReply_t) + nameRep->len + 1);

                replynode->namelen = nameRep->len;
                memcpy(replynode->namebuf, nameRep->str, nameRep->len);
                replynode->name = replynode->namebuf;

                replynode->value = replyRow->element[i+1];
                replyRow->element[i+1] = NULL;

                HASH_ADD_STR(fieldsmap, name, replynode);

                i += 2;
            }
        }

        RedisFreeReplyObject(&replyRow);
    }

    return fieldsmap;
}


redisReply * RDBFieldsMapGetField (RDBFieldsMap fieldsmap, const char *fieldname, int fieldnamelen)
{
    RDBNameReply fieldnode = NULL;

    if (fieldnamelen == -1) {
        HASH_FIND_STR(fieldsmap, fieldname, fieldnode);
    } else {
        HASH_FIND_STR_LEN(fieldsmap, fieldname, fieldnamelen, fieldnode);
    }

    if (fieldnode) {
        return fieldnode->value;
    }

    return NULL;
}


void RDBFieldsMapFree (RDBFieldsMap fields)
{
    if (fields) {
        RDBNameReply curnode, tmpnode;

        HASH_ITER(hh, fields, curnode, tmpnode) {
            RedisFreeReplyObject(&curnode->value);

            HASH_DEL(fields, curnode);

            RDBMemFree(curnode);
        }
    }
}
