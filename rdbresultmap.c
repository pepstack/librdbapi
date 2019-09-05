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
 * rdbresultmap.c
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-08-31
 * @update:
 */
#include "rdbresultmap.h"


/**************************************
 *
 * RDBResultMap
 *
 **************************************/

RDBAPI_RESULT RDBResultMapCreate (const char *title, int colheads, const char *colheadnames[], RDBResultMap *outresultmap)
{
    if (colheads < 1 || colheads > RDBAPI_ARGV_MAXNUM) {
        return RDBAPI_ERR_BADARG;
    } else {
        RDBResultMap resultmap = (RDBResultMap) RDBMemAlloc(sizeof(RDBResultMap_t) + sizeof(RDBZString) * colheads);
        if (resultmap) {
            int col = 0;
            for (; col < colheads; col++) {
                resultmap->colheadnames[col] = RDBZStringNew(colheadnames[col], cstr_length(colheadnames[col], RDB_KEY_NAME_MAXLEN));
            }

            snprintf_chkd_V1(resultmap->title, sizeof(resultmap->title), "%.*s", cstr_length(title, RDB_KEY_NAME_MAXLEN), title);
            resultmap->colheads = col;

            *outresultmap = resultmap;
            return RDBAPI_SUCCESS;
        } else {
            return RDBAPI_ERR_NOMEM;
        }
    }
}


void RDBResultMapDestroy (RDBResultMap resultmap)
{
    if (resultmap) {
        while (resultmap->colheads-- > 0) {
            RDBZStringFree(resultmap->colheadnames[resultmap->colheads]);
        }

        RDBResultMapCleanRows(resultmap);

        RDBResultFilterFree(resultmap->resfilter);

        RDBMemFree(resultmap);
    }
}


RDBAPI_RESULT RDBResultMapInsertRow (RDBResultMap resultmap, RDBRow row)
{
    if (RDBResultMapRows(resultmap) == (ub4)(UB4MAXVAL - 1)) {
        return RDBAPI_ERROR;
    } else {
        RDBRowNode rownode;
        HASH_FIND_STR_LEN(resultmap->rowsmap, row->key, row->keylen, rownode);
        if (rownode) {
            return RDBAPI_ERR_EXISTED;
        }
        HASH_ADD_STR_LEN(resultmap->rowsmap, key, row->keylen, row);
        return RDBAPI_SUCCESS;
    }
}


RDBRow RDBResultMapFindRow (RDBResultMap resultmap, const char *rowkey, int keylen)
{
    RDBRowNode node;

    if (keylen == -1) {
        HASH_FIND_STR(resultmap->rowsmap, rowkey, node);
    } else {
        HASH_FIND_STR_LEN(resultmap->rowsmap, rowkey, keylen, node);
    }

    return node;
}


void RDBResultMapDeleteRow (RDBResultMap resultmap, RDBRow row)
{
    HASH_DEL(resultmap->rowsmap, row);
    RDBRowFree(row);
}


void RDBResultMapCleanRows (RDBResultMap resultmap)
{
    RDBRowNode curnode, tmpnode;
    HASH_ITER(hh, resultmap->rowsmap, curnode, tmpnode) {
        HASH_DEL(resultmap->rowsmap, curnode);
        RDBRowFree(curnode);
    }
}


const char * RDBResultMapTitle (RDBResultMap resultmap)
{
    return (resultmap->title[0] == '\0')? NULL : resultmap->title;
}


int RDBResultMapColHeads (RDBResultMap resultmap)
{
    return resultmap->colheads;
}


RDBZString RDBResultMapColHeadName (RDBResultMap resultmap, int colindex)
{
    return resultmap->colheadnames[colindex];
}


RDBRowIter RDBResultMapFirstRow (RDBResultMap resultmap)
{
    RDBRowIter iter = &resultmap->rowiter;

    iter->head = resultmap->rowsmap;
    iter->el = iter->head;

#ifdef NO_DECLTYPE
    (*(char**)(&iter->tmp)) = (char*) (iter->head != NULL? iter->head->hh.next : NULL);
#else
    iter->tmp = DECLTYPE(iter->el)(iter->head != NULL? iter->head->hh.next : NULL);
#endif

    return (iter->el != NULL ? iter : NULL);
}


RDBRowIter RDBResultMapNextRow (RDBRowIter iter)
{
    if (! iter) {
        return NULL;
    }

    iter->el = iter->tmp;

#ifdef NO_DECLTYPE
    (*(char**)(&iter->tmp)) = (char*)(iter->tmp != NULL? iter->tmp->hh.next : NULL);
#else
    iter->tmp = DECLTYPE(el)(iter->tmp != NULL? iter->tmp->hh.next : NULL);
#endif

    return (iter->el != NULL ? iter : NULL);
}


RDBRow RDBRowIterGetRow (RDBRowIter iter)
{
    return (iter? iter->el : NULL);
}


ub4 RDBResultMapRows (RDBResultMap resultmap)
{
    if (resultmap && resultmap->rowsmap) {
        return (ub4) HASH_COUNT(resultmap->rowsmap);
    }

    return 0;
}

/*
-------------+-----------+----------+-----------+---------+---------
[ tablespace | tablename | datetime | timestamp | comment | fields ]
-------------+-----------+----------+-----------+---------+---------
xsdb | test22 | 2019-09-04 19:30:40 | 0 | test table | (@RESULTMAP@fields:1)

(RESULTMAP@fields:1)
*/


void RDBResultMapPrint (RDBCtx ctx, RDBResultMap resultmap, FILE *fout)
{
    int col, chlen;

    RDBRow row;
    RDBCell cell;
    RDBRowIter rowiter;

    int verbose = ctx->env->verbose;
    char delimt = ctx->env->delimiter;

    fprintf(fout, "%s:\n", resultmap->title);

    // print table head
    int numcols = RDBResultMapColHeads(resultmap);

    for (col = 0; col < numcols; col++) {
        chlen = (int) RDBZStringLen(RDBResultMapColHeadName(resultmap, col));

        if (col == 0) {
            fprintf(fout, "--");            
            while (chlen-- > 0) fprintf(fout, "-");
        } else {
            fprintf(fout, "-+-");
            while (chlen-- > 0) fprintf(fout, "-");
        }
    }
    fprintf(fout, "--\n");

    for (col = 0; col < numcols; col++) {
        RDBZString zstr = RDBResultMapColHeadName(resultmap, col);
        if (col == 0) {
            fprintf(fout, "[ %*s", (int) RDBZStringLen(zstr), RDBZStringAddr(zstr));
            chlen += (int) RDBZStringLen(zstr) + 2;
        } else {
            fprintf(fout, " | %*s", (int) RDBZStringLen(zstr), RDBZStringAddr(zstr));
            chlen += (int) RDBZStringLen(zstr) + 3;
        }
    }
    fprintf(fout, " ]\n");

    for (col = 0; col < numcols; col++) {
        chlen = (int) RDBZStringLen(RDBResultMapColHeadName(resultmap, col));

        if (col == 0) {
            fprintf(fout, "--");            
            while (chlen-- > 0) fprintf(fout, "-");
        } else {
            fprintf(fout, "-+-");
            while (chlen-- > 0) fprintf(fout, "-");
        }
    }
    fprintf(fout, "--\n");
    fflush(fout);

    // print body rows
    rowiter = RDBResultMapFirstRow(resultmap);
    while (rowiter) {
        row = RDBRowIterGetRow(rowiter);

        for (col = 0; col < numcols; col++) {
            cell = RDBRowCell(row, col);

            if (col > 0) {
                fprintf(fout, "%c", delimt);
            }

            RDBCellPrint(cell, fout);
        }

        fprintf(fout, "\n");
        fflush(fout);

        for (col = 0; col < numcols; col++) {
            cell = RDBRowCell(row, col);

            if (RDBCellGetValue(cell, NULL)== RDB_CELLTYPE_RESULTMAP) {
                RDBResultMapPrint(ctx, RDBCellGetResult(cell), fout);
            }
        }

        rowiter = RDBResultMapNextRow(rowiter);
    }

    fprintf(fout, "\n");
    fflush(fout);
}


/*
void RDBResultMapPrint (RDBResultMap resultMap, const char *header, ...)
{

    int i;

    if (header) {
        va_list args;
        va_start(args, header);
        vprintf(header, args);
        va_end(args);
    }

    if (resultMap->sqlstmt == RDBSQL_SELECT || resultMap->sqlstmt == RDBSQL_DELETE) {
        size_t len = 0;

        const char **vtnames = (const char **) resultMap->ctxh->env->valtypenames;

        fprintf(stdout, "$(tablename): %.*s}\n", (int) (strchr(resultMap->keypattern, '$') - resultMap->keypattern - 1), resultMap->keypattern);
        fprintf(stdout, "$(rkpattern): %.*s\n", resultMap->kplen, resultMap->keypattern);

        if (resultMap->filter) {
            fprintf(stdout, "$(last offset): %"PRIu64"\n", RDBResultMapGetOffset(resultMap));
        }
        if (resultMap->rbtree.iSize) {
            fprintf(stdout, "$(result rows): %"PRIu64"\n", RDBResultMapRows(resultMap));
        }

        fprintf(stdout, "\n");

        for (i = 1; i <= resultMap->rowkeyid[0]; i++) {
            int j = resultMap->rowkeyid[i];

            if (! len) {
                fprintf(stdout, "[ $%s", resultMap->fielddes[j].fieldname);
            } else {
                fprintf(stdout, " %c $%s", resultMap->delimiter, resultMap->fielddes[j].fieldname);
            }

            len += strlen(resultMap->fielddes[j].fieldname) + 4;
        }

        // print names for fields
        for (i = 0; i < resultMap->resultfields; i++) {
            if (! len) {
                // fieldname
                fprintf(stdout, "[ %.*s", resultMap->fieldnamelens[i], resultMap->fetchfields[i]);
            } else {
                // | fieldname
                fprintf(stdout, " %c %.*s", resultMap->delimiter, resultMap->fieldnamelens[i], resultMap->fetchfields[i]);
            }

            len += resultMap->fieldnamelens[i] + 3;
        }
        fprintf(stdout, " ]\n--");

        i = 1;
        while (i++ < len) {
            fprintf(stdout, "-");
        }
        fprintf(stdout, "\n");

    } else if (resultMap->sqlstmt == RDBSQL_DESC_TABLE) {
        const char **vtnames = (const char **) resultMap->ctxh->env->valtypenames;

        fprintf(stdout, "$(tablename): %.*s}\n", (int) (strchr(resultMap->keypattern, '$') - resultMap->keypattern - 1), resultMap->keypattern);
        fprintf(stdout, "$(rkpattern): %.*s\n", resultMap->kplen, resultMap->keypattern);
        fprintf(stdout, "$(timestamp): %"PRIu64"\n", resultMap->table_timestamp);
        fprintf(stdout, "$(creatdate): %s\n", resultMap->table_datetime);
        fprintf(stdout, "$(tbcomment): %s\n", resultMap->table_comment);
        fprintf(stdout, "---------------------+-----------+--------+---------+--------+----------+----------\n");
        fprintf(stdout, "[      fieldname     | fieldtype | length |  scale  | rowkey | nullable | comment ]\n");
        fprintf(stdout, "---------------------+-----------+--------+---------+--------+----------+----------\n");

        for (i = 0; i < resultMap->numfields; i++) {
            RDBFieldDes_t *fdes = &resultMap->fielddes[i];

            fprintf(stdout, " %-20s| %8s  | %-6d |%8d |  %4d  |   %4d   | %s\n",
                fdes->fieldname,
                vtnames[fdes->fieldtype],
                fdes->length,
                fdes->dscale,
                fdes->rowkey,
                fdes->nullable,
                fdes->comment);
        }
        fprintf(stdout, "---------------------+-----------+--------+---------+--------+----------+----------\n");
    } else if (resultMap->sqlstmt == RDBSQL_SHOW_DATABASES) {
        fprintf(stdout, "------------------------------\n");
        fprintf(stdout, "[          database          ]\n");
        printf("------------------------------\n");
    } else if (resultMap->sqlstmt == RDBSQL_SHOW_TABLES) {
        fprintf(stdout, "------------------------------\n");
        fprintf(stdout, "[       database.table       ]\n");
        fprintf(stdout, "------------------------------\n");
    } else if (resultMap->sqlstmt == RDBSQL_INFO_SECTION) {

    }

    // print rowset
    RDBResultMapTraverse(resultMap, RDBResultRowPrintCallback, resultMap);

    if (resultMap->sqlstmt == RDBSQL_SHOW_DATABASES || resultMap->sqlstmt == RDBSQL_SHOW_TABLES) {
        if (RDBResultMapRows(resultMap) > 0) {
            fprintf(stdout, "------------------------------\n");
        }
        fprintf(stdout, "$(totalrows): %"PRIu64"\n", RDBResultMapRows(resultMap));
    } else if (resultMap->sqlstmt == RDBSQL_INFO_SECTION) {
        fprintf(stdout, "$(clusternodes): %d\n", RDBEnvNumNodes(resultMap->ctxh->env));
    }

    fflush(stdout);
}
*/


/*
static void RDBResultRowPrintCallback (void * _row, void * _map)
{
    int i, cols = 0;

    RDBResultMap resultMap = (RDBResultMap)_map;
    RDBResultRow resultRow = (RDBResultRow)_row;
    RDBFieldsMap propsmap = resultRow->fieldmap;

    if (resultMap->sqlstmt == RDBSQL_SELECT ||
        resultMap->sqlstmt == RDBSQL_DELETE
    ) {
        // print rowkey values
        for (i = 1; i <= resultMap->rowkeyid[0]; i++) {
            int j = resultMap->rowkeyid[i];

            RDBNameReply kvnode = NULL;

            HASH_FIND_STR_LEN(propsmap, resultMap->fielddes[j].fieldname, resultMap->fielddes[j].namelen, kvnode);

            if (cols) {
                fprintf(stdout, "%c%.*s", resultMap->delimiter, kvnode->sublen, kvnode->subval);
            } else {
                fprintf(stdout, "%.*s", kvnode->sublen, kvnode->subval);
            }

            cols++;
        }

        // print field values
        for (i = 0; i < resultMap->resultfields; i++) {
            RDBNameReply kvnode = NULL;

            HASH_FIND_STR_LEN(propsmap, resultMap->fetchfields[i], resultMap->fieldnamelens[i], kvnode);

            if (cols) {
                if (kvnode->value) {
                    fprintf(stdout, "%c%.*s", resultMap->delimiter, (int)kvnode->value->len, kvnode->value->str);
                } else {
                    fprintf(stdout, "%c(null)", resultMap->delimiter);
                }
            } else {
                if (kvnode->value) {
                    fprintf(stdout, "%.*s", (int)kvnode->value->len, kvnode->value->str);
                } else {
                    fprintf(stdout, "(null)");
                }
            }

            cols++;
        }
        fprintf(stdout, "\n");
    } else if (resultMap->sqlstmt == RDBSQL_INFO_SECTION) {
        RDBNameReply propnode;

        // $(redisdb): {clusternode(9)::127.0.0.1:7009}
        fprintf(stdout, "$(%s): %.*s\n", RDB_SYSTEM_TABLE_PREFIX, (int) resultRow->replykey->len, resultRow->replykey->str);
        fprintf(stdout, "-------------+------------------------------------+--------------------------------\n");
        fprintf(stdout, "[   section  |             name                   |              value            ]\n");
        fprintf(stdout, "-------------+------------------------------------+--------------------------------\n");

        for (propnode = propsmap; propnode != NULL; propnode = propnode->hh.next) {
            char * pname = strstr(propnode->name, "::");
            int seclen = (int) (pname - propnode->name);

            fprintf(stdout, " %-12.*s| %-34.*s | %.*s\n",
                seclen, propnode->name,
                propnode->namelen - seclen - 2, pname + 2,
                propnode->sublen, propnode->subval);
        }

        fprintf(stdout, "-------------+------------------------------------+--------------------------------\n");
        fprintf(stdout, "\n");
    } else if (resultMap->sqlstmt == RDBSQL_SHOW_DATABASES || resultMap->sqlstmt == RDBSQL_SHOW_TABLES) {
        fprintf(stdout, " %-30.*s\n", resultRow->replykey->len, resultRow->replykey->str);
    }
}
*/

/**************************************
 *
 * RDBRow
 *
 **************************************/

RDBAPI_RESULT RDBRowNew (RDBResultMap resultmap, const char *key, int keylen, RDBRow *outrow)
{
    if (! key) {
        char kbuf[22];

        ub4 rowid = RDBResultMapRows(resultmap);

        int klen = snprintf_chkd_V1(kbuf, sizeof(kbuf), "%"PRIu64, (ub8)(rowid + 1));

        RDBRow row = (RDBRow) RDBMemAlloc(sizeof(RDBRow_t) + sizeof(RDBCell_t) * RDBResultMapColHeads(resultmap) + klen + 1);
        if (row) {
            row->count = RDBResultMapColHeads(resultmap);

            row->key = (char *) &row->cells[row->count];
            row->keylen = klen;
    
            memcpy(row->key, kbuf, klen);

            *outrow = row;
            return RDBAPI_SUCCESS;
        } else {
            return RDBAPI_ERR_NOMEM;
        }
    } else {
        if (keylen == (-1)) {
            keylen = cstr_length(key, RDB_KEY_VALUE_SIZE);
        }

        if (keylen > 0 && keylen < RDB_KEY_VALUE_SIZE) {
            RDBRow row = (RDBRow) RDBMemAlloc(sizeof(RDBRow_t) + sizeof(RDBCell_t) * RDBResultMapColHeads(resultmap) + keylen + 1);
            if (row) {
                row->count = RDBResultMapColHeads(resultmap);

                row->key = (char *) &row->cells[row->count];
                row->keylen = keylen;
    
                memcpy(row->key, key, keylen);

                *outrow = row;
                return RDBAPI_SUCCESS;
            } else {
                return RDBAPI_ERR_NOMEM;
            }
        } else {
            return RDBAPI_ERR_BADARG;
        }
    }
}


void RDBRowFree (RDBRow row)
{
    while (row->count-- > 0) {
        RDBCellClean(RDBRowCell(row, row->count));
    }

    RDBMemFree(row);
}


const char * RDBRowGetKey (RDBRow row, int *keylen)
{
    if (keylen) {
        *keylen = row->keylen;
    }
    return row->key;
}


int RDBRowCells (RDBRow row)
{
    return row->count;
}


RDBCell RDBRowCell (RDBRow row, int colindex)
{
    return &row->cells[colindex];
}


/**************************************
 *
 * RDBCell API
 *
 *************************************/

RDBCell RDBCellSetValue (RDBCell cell, RDBCellType type, void *value)
{
    if (cell->type != RDB_CELLTYPE_INVALID) {
        RDBCellClean(cell);
    }

    switch (type) {
    case RDB_CELLTYPE_ZSTRING:
        cell->zstr = (RDBZString) value;
        break;

    case RDB_CELLTYPE_BINARY:
        cell->bin = (RDBBinary) value;
        break;

    case RDB_CELLTYPE_REPLY:
        cell->reply = (redisReply *) value;
        break;

    case RDB_CELLTYPE_RESULTMAP:
        cell->resultmap = (RDBResultMap) value;
        break;

    default:
        cell->type = RDB_CELLTYPE_INVALID;
        return cell;
    }

    cell->type = type;
    return cell;
}


RDBCellType RDBCellGetValue (RDBCell cell, void **outvalue)
{
    if (outvalue) {
        switch (cell->type) {
        case RDB_CELLTYPE_ZSTRING:
            *outvalue = (void*) cell->zstr;
            break;

        case RDB_CELLTYPE_BINARY:
            *outvalue = (void*) cell->bin;
            break;

        case RDB_CELLTYPE_REPLY:
            *outvalue = (void*)cell->reply;
            break;

        case RDB_CELLTYPE_RESULTMAP:
            *outvalue = (void*)cell->resultmap;
            break;

        default:
            *outvalue = NULL;
            break;
        }
    }
    return cell->type;
}


RDBCell RDBCellSetString (RDBCell cell, const char *str, int len)
{
    return RDBCellSetValue(cell, RDB_CELLTYPE_ZSTRING, (void*) RDBZStringNew(str, len));
}


RDBCell RDBCellSetBinary (RDBCell cell, const void *addr, ub4 sz)
{
    return RDBCellSetValue(cell, RDB_CELLTYPE_RESULTMAP, (void*) RDBBinaryNew(addr, sz));
}


RDBCell RDBCellSetReply (RDBCell cell, redisReply *replyString)
{
    return RDBCellSetValue(cell, RDB_CELLTYPE_RESULTMAP, (void*) replyString);
}


RDBCell RDBCellSetResult (RDBCell cell, RDBResultMap resultmap)
{
    return RDBCellSetValue(cell, RDB_CELLTYPE_RESULTMAP, (void*) resultmap);
}


RDBZString RDBCellGetString (RDBCell cell)
{
    return cell->zstr;
}


RDBBinary RDBCellGetBinary (RDBCell cell)
{
    return cell->bin;
}


redisReply* RDBCellGetReply (RDBCell cell)
{
    return cell->reply;
}


RDBResultMap RDBCellGetResult (RDBCell cell)
{
    return cell->resultmap;
}


void RDBCellClean (RDBCell cell)
{
    switch (cell->type) {
    case RDB_CELLTYPE_ZSTRING:
        RDBZStringFree(cell->zstr);
        break;

    case RDB_CELLTYPE_BINARY:
        RDBBinaryFree(cell->bin);
        break;

    case RDB_CELLTYPE_REPLY:
        freeReplyObject(cell->reply);
        break;

    case RDB_CELLTYPE_RESULTMAP:
        RDBResultMapDestroy(cell->resultmap);
        break;
    }

    bzero(cell, sizeof(RDBCell_t));
}


extern void RDBCellPrint (RDBCell cell, FILE *fout)
{
    switch (cell->type) {
    case RDB_CELLTYPE_ZSTRING:
        fprintf(fout, " %.*s ", (int) cell->zstr->len, cell->zstr->str);
        break;
    case RDB_CELLTYPE_REPLY:
        fprintf(fout, " %.*s ", (int) cell->reply->len, cell->reply->str);
        break;
    case RDB_CELLTYPE_BINARY:
        fprintf(fout, " (@BINARY:%"PRIu32") ", cell->bin->sz);
        break;
    case RDB_CELLTYPE_RESULTMAP:
        if (RDBResultMapTitle(cell->resultmap)) {
            fprintf(fout, " (@RESULTMAP:%s) ", RDBResultMapTitle(cell->resultmap));
        } else {
            fprintf(fout, " (@RESULTMAP:%p) ", cell->resultmap);
        }
        break;
    default:
        fprintf(fout, " (@INVALID@) ");
        break;
    }
}
