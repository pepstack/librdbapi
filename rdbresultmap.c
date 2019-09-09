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

RDBAPI_RESULT RDBResultMapCreate (const char *title, const char *colheadnames[], const int *nameslen, size_t numcolheads, RDBResultMap *outresultmap)
{
    if (numcolheads == -1) {
        int num = 0;

        while (colheadnames && colheadnames[num]) {
            if (num++ > RDBAPI_ARGV_MAXNUM) {
                return RDBAPI_ERR_BADARG;
            }
        }

        numcolheads = num;
    }

    if (numcolheads > RDBAPI_ARGV_MAXNUM) {
        return RDBAPI_ERR_BADARG;
    } else {
        RDBResultMap resultmap = (RDBResultMap) RDBMemAlloc(sizeof(RDBResultMap_t) + sizeof(RDBZString) * numcolheads);
        if (resultmap) {
            int len = 0;
            size_t col = 0;

            len = cstr_length(title, RDB_KEY_VALUE_SIZE);
            if (len) {
                resultmap->title = RDBZStringNew(title, len);
            } else {
                char str[20] = {0};
                len = snprintf_chkd_V1(str, sizeof(str), "%p", resultmap);
                resultmap->title = RDBZStringNew(str, len);
            }

            for (; col < numcolheads; col++) {
                if (nameslen) {
                    resultmap->colheadnames[col] = RDBZStringNew(colheadnames[col], nameslen[col]);
                } else {
                    resultmap->colheadnames[col] = RDBZStringNew(colheadnames[col], cstr_length(colheadnames[col], RDB_KEY_NAME_MAXLEN));
                }
            }

            resultmap->colheads = (int) col;

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

        RDBZStringFree(resultmap->title);

        RDBResultMapDeleteAll(resultmap);

        RDBMemFree(resultmap->filter);

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


void RDBResultMapDeleteOne (RDBResultMap resultmap, RDBRow row)
{
    HASH_DEL(resultmap->rowsmap, row);
    RDBRowFree(row);
}


void RDBResultMapDeleteAll (RDBResultMap resultmap)
{
    RDBRowNode curnode, tmpnode;
    HASH_ITER(hh, resultmap->rowsmap, curnode, tmpnode) {
        HASH_DEL(resultmap->rowsmap, curnode);
        RDBRowFree(curnode);
    }
}


void RDBResultMapDeleteAllOnCluster (RDBResultMap resultmap)
{
    RDBRowNode curnode, tmpnode;
    HASH_ITER(hh, resultmap->rowsmap, curnode, tmpnode) {
        if (RedisDeleteKey(resultmap->ctx, curnode->key, curnode->keylen, NULL, 0) != RDBAPI_KEY_DELETED) {
            HASH_DEL(resultmap->rowsmap, curnode);
            RDBRowFree(curnode);
        }
    }
}


RDBZString RDBResultMapTitle (RDBResultMap resultmap)
{
    return resultmap->title;
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
    iter->tmp = DECLTYPE(iter->el)(iter->tmp != NULL? iter->tmp->hh.next : NULL);
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

    fprintf(fout, "%.*s\n", (int) resultmap->title->len, resultmap->title->str);

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
    if (numcols > 0) {
        fprintf(fout, "--\n");
    }

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
    if (numcols > 0) {
        fprintf(fout, " ]\n");
    }

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
    if (numcols > 0) {
        fprintf(fout, "--\n");
    }
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
    return RDBCellSetValue(cell, RDB_CELLTYPE_BINARY, (void*) RDBBinaryNew(addr, sz));
}


RDBCell RDBCellSetReply (RDBCell cell, redisReply *replyString)
{
    return RDBCellSetValue(cell, RDB_CELLTYPE_REPLY, (void*) replyString);
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
        fprintf(fout, " (@RESULTMAP:%.*s) ",
            (int) RDBZStringLen(RDBResultMapTitle(cell->resultmap)), RDBZStringAddr(RDBResultMapTitle(cell->resultmap)));
        break;
    default:
        fprintf(fout, " (@INVALID) ");
        break;
    }
}