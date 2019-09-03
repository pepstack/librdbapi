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
        RDBMemFree(resultmap);
    }
}


RDBAPI_RESULT RDBRowsetInsertRow (RDBRowset resultmap, RDBRow row)
{
    RDBRowNode rownode;

    HASH_FIND_STR_LEN(resultmap->rowsmap, row->key, row->keylen, rownode);

    if (rownode) {
        return RDBAPI_ERR_EXISTED;
    }

    HASH_ADD_STR_LEN(resultmap->rowsmap, key, row->keylen, row);

    return RDBAPI_SUCCESS;
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

RDBAPI_RESULT RDBRowNew (int numcells, const char *key, int keylen, RDBRow *outrow)
{
    if (keylen == (-1)) {
        keylen = cstr_length(key, RDB_KEY_VALUE_SIZE);
    }

    if (keylen > 0 && keylen < RDB_KEY_VALUE_SIZE) {
        RDBRow row = (RDBRow) RDBMemAlloc(sizeof(RDBRow_t) + sizeof(RDBCell_t) * numcells + keylen+1);
        if (row) {
            row->count = numcells;

            row->key = (char *) &row->cells[numcells];
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


void RDBRowFree (RDBRow row)
{
    while (row->count-- > 0) {
        RDBCellClean(RDBRowGetCell(row, row->count));
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


int RDBRowGetCells (RDBRow row)
{
    return row->count;
}


RDBCell RDBRowGetCell (RDBRow row, int colindex)
{
    return &row->cells[colindex];
}


/**************************************
 *
 * RDBCell
 *
 **************************************/

RDBCell RDBCellInit (RDBCell cell, RDBCellType type, void *value)
{
    switch (type) {
    case RDB_CELLTYPE_STRING:
        cell->zstr = (RDBZString) value;
        break;

    case RDB_CELLTYPE_INTEGER:
        memcpy(&cell->lval, (sb8 *)value, sizeof(sb8));
        break;

    case RDB_CELLTYPE_DOUBLE:
        memcpy(&cell->dval, (double *)value, sizeof(double));
        break;

    case RDB_CELLTYPE_BINARY:
        cell->bval.addr = ((tpl_bin *) value)->addr;
        cell->bval.sz = ((tpl_bin *) value)->sz;
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


void RDBCellClean (RDBCell cell)
{
    switch (cell->type) {
    case RDB_CELLTYPE_STRING:
        RDBZStringFree(cell->zstr);
        break;

    case RDB_CELLTYPE_BINARY:
        RDBMemFree(cell->bval.addr);
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


void RDBCellPrint (RDBRowset resultmap, RDBCell cell, FILE *fout)
{
    switch (cell->type) {
    case RDB_CELLTYPE_STRING:
        fprintf(fout, "%.*s", (int)cell->zstr->len, cell->zstr->str);
        break;

    case RDB_CELLTYPE_INTEGER:
        fprintf(fout, "%"PRId64, cell->lval);
        break;

    case RDB_CELLTYPE_DOUBLE:
        fprintf(fout, "%lf", cell->dval);
        break;

    case RDB_CELLTYPE_BINARY:
        // TODO
        fprintf(fout, "(binary:%d)", (int)cell->bval.sz);
        break;

    case RDB_CELLTYPE_REPLY:
        // TODO:
        break;

    case RDB_CELLTYPE_RESULTMAP:
        RDBRowsetPrint(cell->resultmap, fout);
        break;
    }
}