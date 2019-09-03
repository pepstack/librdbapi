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
 * RDBRowset
 *
 **************************************/

RDBAPI_RESULT RDBRowsetCreate (int numcolheaders, const char *colheadernames[], RDBRowset *outresultmap)
{
    if (numcolheaders < 1 || numcolheaders > RDBAPI_ARGV_MAXNUM) {
        return RDBAPI_ERR_BADARG;
    } else {
        RDBRowset resultmap = (RDBRowset) RDBMemAlloc(sizeof(RDBRowset_t) + sizeof(RDBZString) * numcolheaders);
        if (resultmap) {
            int col = 0;
            for (; col < numcolheaders; col++) {
                resultmap->colheadernames[col] = RDBZStringNew(colheadernames[col], cstr_length(colheadernames[col], RDB_KEY_NAME_MAXLEN));
            }

            resultmap->numcolheaders = col;

            *outresultmap = resultmap;
            return RDBAPI_SUCCESS;
        } else {
            return RDBAPI_ERR_NOMEM;
        }
    }
}


void RDBRowsetDestroy (RDBRowset resultmap)
{
    if (resultmap) {
        while (resultmap->numcolheaders-- > 0) {
            RDBZStringFree(resultmap->colheadernames[resultmap->numcolheaders]);
        }

        RDBRowsetCleanRows(resultmap);

        RDBResultFilterFree(resultmap->resfilter);

        RDBMemFree(resultmap);
    }
}


RDBAPI_RESULT RDBRowsetInsertRow (RDBRowset resultmap, RDBRow row)
{
    if (RDBRowsetSize(resultmap) == (ub4)(UB4MAXVAL - 1)) {
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


RDBRow RDBRowsetFindRow (RDBRowset resultmap, const char *rowkey, int keylen)
{
    RDBRowNode node;

    if (keylen == -1) {
        HASH_FIND_STR(resultmap->rowsmap, rowkey, node);
    } else {
        HASH_FIND_STR_LEN(resultmap->rowsmap, rowkey, keylen, node);
    }

    return node;
}


void RDBRowsetDeleteRow (RDBRowset resultmap, RDBRow row)
{
    HASH_DEL(resultmap->rowsmap, row);
    RDBRowFree(row);
}


void RDBRowsetCleanRows (RDBRowset resultmap)
{
    RDBRowNode curnode, tmpnode;
    HASH_ITER(hh, resultmap->rowsmap, curnode, tmpnode) {
        HASH_DEL(resultmap->rowsmap, curnode);
        RDBRowFree(curnode);
    }
}


int RDBRowsetColHeaders (RDBRowset resultmap)
{
    return resultmap->numcolheaders;
}


RDBZString RDBRowsetColHeaderName (RDBRowset resultmap, int colindex)
{
    return resultmap->colheadernames[colindex];
}


RDBRowIter RDBRowsetFirstRow (RDBRowset resultmap)
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


RDBRowIter RDBRowsetNextRow (RDBRowIter iter)
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


ub4 RDBRowsetSize (RDBRowset resultmap)
{
    if (resultmap && resultmap->rowsmap) {
        return (ub4) HASH_COUNT(resultmap->rowsmap);
    }

    return 0;
}


void RDBRowsetPrint (RDBRowset resultmap, FILE *fout)
{
    // print cols header
    int col;

    for (col = 0; col < RDBRowsetColHeaders(resultmap); col++) {
        RDBZString zstr = RDBRowsetColHeaderName(resultmap, col);
        if (col == 0) {
            fprintf(fout, " %-20.*s", (int) RDBZStringLen(zstr), RDBZStringAddr(zstr));
        } else {
            fprintf(fout, "| %-20.*s", (int) RDBZStringLen(zstr), RDBZStringAddr(zstr));
        }
    }

    fprintf(fout, "\n");

    // print rows
}



/**************************************
 *
 * RDBRow
 *
 **************************************/

RDBAPI_RESULT RDBRowNew (RDBRowset resultmap, const char *key, int keylen, RDBRow *outrow)
{
    if (! key) {
        char kbuf[22];

        ub4 rowid = RDBRowsetSize(resultmap);

        int klen = snprintf_chkd_V1(kbuf, sizeof(kbuf), "%"PRIu64, (ub8)(rowid + 1));

        RDBRow row = (RDBRow) RDBMemAlloc(sizeof(RDBRow_t) + sizeof(RDBCell_t) * RDBRowsetColHeaders(resultmap) + klen + 1);
        if (row) {
            row->count = RDBRowsetColHeaders(resultmap);

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
            RDBRow row = (RDBRow) RDBMemAlloc(sizeof(RDBRow_t) + sizeof(RDBCell_t) * RDBRowsetColHeaders(resultmap) + keylen + 1);
            if (row) {
                row->count = RDBRowsetColHeaders(resultmap);

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
        cell->resultmap = (RDBRowset) value;
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


RDBCell RDBCellSetResult (RDBCell cell, RDBRowset resultmap)
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


RDBRowset RDBCellGetResult (RDBCell cell)
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
        RDBRowsetDestroy(cell->resultmap);
        break;
    }

    bzero(cell, sizeof(RDBCell_t));
}
