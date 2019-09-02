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


typedef struct {
    FILE *fout;

    RDBRowset resultmap;
} RDBPrintCbArg_t, *RDBPrintCbArg;


static int RDBResultRowCompareCb (void *newObject, void *nodeObject)
{
    if (newObject == nodeObject) {
        return 0;
    } else {
        RDBRow A = (RDBRow) newObject;
        RDBRow B = (RDBRow) nodeObject;

        return cstr_compare_len(A->rowkey->str, A->rowkey->len, B->rowkey->str, B->rowkey->len);
    }
}


static void RDBResultRowDeleteCb (void *rowobject, void *rowsmap)
{
     RDBRowFree((RDBRow) rowobject);
}


static void RDBResultRowPrintCb (void *rowobject, void *cbarg)
{
    int col;
    RDBCell cell;

    RDBRow row = (RDBRow) rowobject;

    RDBPrintCbArg pcba = (RDBPrintCbArg) cbarg;

    // RDBRowGetKey(row, );

    int cells = RDBRowGetCells(row);

    for (col = 0; col < cells; col++) {
        cell = RDBRowGetCell(row, col);

        RDBCellPrint(pcba->resultmap, cell, (FILE *) pcba->fout);
    }
}


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
        //TODO: RDBResultMapDestroy(cell->resultmap);
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
        RDBRowsetPrint(cell->resultmap, NULL, fout);
        break;
    }
}


RDBRow RDBRowNew (ub4 numcols, redisReply *replyKey, char *rowkey, int keylen)
{
    if (replyKey && replyKey->type == REDIS_REPLY_STRING) {
        RDBRow row = (RDBRow) RDBMemAlloc(sizeof(RDBRow_t) + sizeof(RDBCell_t) * numcols);
        row->ncols = numcols;
        row->replyKey = replyKey;
        return row;
    } else if (rowkey) {
        RDBRow row = (RDBRow) RDBMemAlloc(sizeof(RDBRow_t) + sizeof(RDBCell_t) * numcols);
        row->ncols = numcols;
        row->rowkey = RDBZStringNew(rowkey, (ub4) keylen);
        return row;
    } else {
        fprintf(stderr, "(%s:%d) application error.\n", __FILE__, __LINE__);
        return NULL;
    }
}


void RDBRowFree (RDBRow row)
{
    while (row->ncols-- > 0) {
        RDBCellClean(RDBRowGetCell(row, row->ncols));
    }

    if (row->replyKey) {
        freeReplyObject(row->replyKey);
    } else {
        RDBZStringFree(row->rowkey);
    }

    RDBMemFree(row);
}


const char * RDBRowGetKey (RDBRow row, ub4 *keylen)
{
    if (row->replyKey) {
        if (keylen) {
            *keylen = (ub4) row->replyKey->len;
        }
        return row->replyKey->str;
    } else {
        if (keylen) {
            *keylen = row->rowkey->len;
        }
        return row->rowkey->str;
    }
}


ub4 RDBRowGetCells (RDBRow row)
{
    return row->ncols;
}


RDBCell RDBRowGetCell (RDBRow row, int colindex)
{
    return &row->colvals[colindex];
}


RDBAPI_RESULT RDBRowsetCreate (int numcols, const char *names[], RDBRowset *outresultmap)
{
    if (numcols > RDBAPI_ARGV_MAXNUM) {
        return RDBAPI_ERROR;
    } else {
        int col;
        RDBRowset resultmap = (RDBRowset) RDBMemAlloc(sizeof(RDBRowset_t) + sizeof(RDBZString) * numcols);
        if (! resultmap) {
            fprintf(stderr, "(%s:%d) out of memory.\n", __FILE__, __LINE__);
            exit(EXIT_FAILURE);
        }

        resultmap->delimiter = RDB_TABLE_DELIMITER_CHAR;

        for (col = 0; col < numcols; col++) {
            resultmap->names[col] = RDBZStringNew(names[col], cstr_length(names[col], RDB_KEY_NAME_MAXLEN));
        }

        resultmap->numcols = col;
        rbtree_init(&resultmap->rowstree, (fn_comp_func*) RDBResultRowCompareCb);

        *outresultmap = resultmap;
        return RDBAPI_SUCCESS;
    }
}


void RDBRowsetDestroy (RDBRowset resultmap)
{
    if (resultmap) {
        while (resultmap->numcols-- > 0) {
            RDBZStringFree(resultmap->names[resultmap->numcols]);
        }

        RDBRowsetCleanRows(resultmap);
        RDBMemFree(resultmap);
    }
}


int RDBRowsetGetCols (RDBRowset resultmap)
{
    return resultmap->numcols;
}


RDBZString RDBRowsetGetColName (RDBRowset resultmap, int colindex)
{
    return resultmap->names[colindex];
}


void RDBRowsetCleanRows (RDBRowset resultmap)
{
    if (resultmap) {
        rbtree_traverse(&resultmap->rowstree, RDBResultRowDeleteCb, (void *) resultmap);

        rbtree_clean(&resultmap->rowstree);
    }
}


void RDBRowsetTraverse (RDBRowset resultmap, void (ResultRowCbFunc)(void *, void *), void *arg)
{
    rbtree_traverse(&resultmap->rowstree, ResultRowCbFunc, arg);
}


void RDBRowsetPrint (RDBRowset resultmap, const RDBSheetStyle_t *styles, FILE *fout)
{
    int col;
    RDBPrintCbArg_t pcba = {0};

    pcba.fout = fout;
    pcba.resultmap = resultmap;

    // print cols header
    for (col = 0; col < RDBRowsetGetCols(resultmap); col++) {
        RDBZString zstr = RDBRowsetGetColName(resultmap, col);
        if (col == 0) {
            fprintf(fout, " %-20.*s", (int) RDBZStringLen(zstr), RDBZStringAddr(zstr));
        } else {
            fprintf(fout, "| %-20.*s", (int) RDBZStringLen(zstr), RDBZStringAddr(zstr));
        }
    }

    fprintf(fout, "\n");

    // print rows

    RDBRowsetTraverse(resultmap, RDBResultRowPrintCb, (void *)&pcba);
}