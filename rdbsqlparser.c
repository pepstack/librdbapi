/***********************************************************************
* Copyright (c) 2008-2080 syna-tech.com, pepstack.com, 350137278@qq.com
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
 * rdbsqlparser.c
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#include "rdbapi.h"
#include "rdbtypes.h"

#include "common/cstrutil.h"
#include "common/uthash/uthash.h"


typedef struct _RDBSQLParser_t
{
    RDBSQLStmt stmt;

    union {
        struct SELECT {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];

            // fields filter
            int numfields;
            char      *fields[RDBAPI_ARGV_MAXNUM];
            RDBSqlExpr fieldexprs[RDBAPI_ARGV_MAXNUM];
            char      *fieldvals[RDBAPI_ARGV_MAXNUM];

            // keys filter
            int numkeys;
            char *keys[RDBAPI_ARGV_MAXNUM];
            RDBSqlExpr keyexprs[RDBAPI_ARGV_MAXNUM];
            char *keyvals[RDBAPI_ARGV_MAXNUM];

            // result fields
            int count;
            char *resultfields[RDBAPI_ARGV_MAXNUM];

            ub8 offset;
            ub4 limit;
        } select;

        struct DELETE_FROM {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];

            // fields filter
            int numfields;
            char      *fields[RDBAPI_ARGV_MAXNUM];
            RDBSqlExpr fieldexprs[RDBAPI_ARGV_MAXNUM];
            char      *fieldvals[RDBAPI_ARGV_MAXNUM];

            // keys filter
            int numkeys;
            char *keys[RDBAPI_ARGV_MAXNUM];
            RDBSqlExpr keyexprs[RDBAPI_ARGV_MAXNUM];
            char *keyvals[RDBAPI_ARGV_MAXNUM];
        } delfrom;
    };

    int sqlen;
    char sql[0];
} RDBSQLParser_t;

RDBAPI_BOOL onParseSelect (RDBSQLParser_t * parser, RDBCtx ctx, char *selsql, int len);
RDBAPI_BOOL onParseDelete (RDBSQLParser_t * parser, RDBCtx ctx, char *delsql, int len);
RDBAPI_BOOL onParseUpdate (RDBSQLParser_t * parser, RDBCtx ctx, char *updsql, int len);
RDBAPI_BOOL onParseCreate (RDBSQLParser_t * parser, RDBCtx ctx, char *cresql, int len);


RDBAPI_RESULT RDBSQLParserNew (RDBCtx ctx, const char *sql, size_t sqlen, RDBSQLParser *outParser)
{
    RDBSQLParser parser;
    int parseOk = 0;
    int len = 0;

    if (sqlen == (size_t) -1) {
        len = (int) strlen(sql);
    } else {
        len = (int) sqlen;
    }

    if (len < 16) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_RDBSQL: sql is too short");
        RDBCTX_ERRMSG_DONE(ctx);
        return RDBAPI_ERR_RDBSQL;
    }

    if (len > 4095) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_RDBSQL: sql is too long");
        RDBCTX_ERRMSG_DONE(ctx);
        return RDBAPI_ERR_RDBSQL;
    }

    parser = (RDBSQLParser) RDBMemAlloc(sizeof(RDBSQLParser_t) + len + 1);

    char * str0 = strdup(sql);
    char * str = cstr_LRtrim_chr(cstr_LRtrim_chr(str0, 32), ';');

    parser->sqlen = (int) strlen(str);
    memcpy(parser->sql, str, parser->sqlen);

    free(str0);

    if (cstr_startwith(parser->sql, parser->sqlen, "SELECT ", (int)strlen("SELECT "))) {

        parseOk = onParseSelect(parser, ctx, parser->sql + strlen("SELECT "), parser->sqlen - (int)strlen("SELECT "));
        parser->stmt = parseOk? RDBSQL_SELECT : RDBSQL_INVALID;

    } else if (cstr_startwith(parser->sql, parser->sqlen, "DELETE FROM ", (int)strlen("DELETE FROM "))) {

        parseOk = onParseDelete(parser, ctx, parser->sql + strlen("DELETE FROM "), parser->sqlen - (int)strlen("DELETE FROM "));
        parser->stmt = parseOk? RDBSQL_DELETE : RDBSQL_INVALID;

    } else if (cstr_startwith(parser->sql, parser->sqlen, "UPDATE ", (int)strlen("UPDATE "))) {

        parseOk = onParseUpdate(parser, ctx, parser->sql + strlen("UPDATE "), parser->sqlen - (int)strlen("UPDATE "));
        parser->stmt = parseOk? RDBSQL_UPDATE : RDBSQL_INVALID;

    } else if (cstr_startwith(parser->sql, parser->sqlen, "CREATE TABLE ", (int)strlen("CREATE TABLE "))) {

        parseOk = onParseCreate(parser, ctx, parser->sql + strlen("CREATE TABLE "), parser->sqlen - (int)strlen("CREATE TABLE "));
        parser->stmt = parseOk? RDBSQL_CREATE : RDBSQL_INVALID;

    } else {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_RDBSQL: invalid sql");
        RDBCTX_ERRMSG_DONE(ctx);

        parser->stmt = RDBSQL_INVALID;
    }

    if (parser->stmt == RDBSQL_INVALID) {
        RDBSQLParserFree(parser);
        RDBCTX_ERRMSG_DONE(ctx);
        return RDBAPI_ERR_RDBSQL;
    }

    RDBCTX_ERRMSG_ZERO(ctx);

    *outParser = parser;
    return RDBAPI_SUCCESS;
}


void RDBSQLParserFree (RDBSQLParser parser)
{
    if (parser->stmt == RDBSQL_SELECT) {
        int i = 0;
        while (i < RDBAPI_ARGV_MAXNUM) {
            char *psz;

            psz = parser->select.resultfields[i];
            RDBMemFree(psz);

            psz = parser->select.keys[i];
            RDBMemFree(psz);

            psz = parser->select.keyvals[i];
            RDBMemFree(psz);

            psz = parser->select.fields[i];
            RDBMemFree(psz);

            psz = parser->select.fieldvals[i];
            RDBMemFree(psz);

            i++;
        }
    };

    RDBMemFree(parser);
}


ub8 RDBSQLExecute(RDBCtx ctx, RDBSQLParser parser, RDBResultMap *outResultMap)
{
    *outResultMap = NULL;

    if (parser->stmt == RDBSQL_SELECT) {
        RDBResultMap resultMap;

        RDBAPI_RESULT res = RDBTableScanFirst(ctx, parser->select.tablespace, parser->select.tablename,
                parser->select.numkeys,
                parser->select.keys,
                parser->select.keyexprs,
                parser->select.keyvals,
                parser->select.numfields,
                parser->select.fields,
                parser->select.fieldexprs,
                parser->select.fieldvals,
                NULL,
                NULL,
                &resultMap);

        if (res == RDBAPI_SUCCESS) {
            ub8 offset = RDBTableScanNext(resultMap, parser->select.offset, parser->select.limit);

            if (offset != RDB_ERROR_OFFSET) {
                *outResultMap = resultMap;
                return offset;
            }

            RDBResultMapFree(resultMap);
            return 0;
        }
    }

    return (ub8) RDBAPI_ERROR;
}


static int RDBSQLNameValidate (const char *name, int len, int maxlen)
{
    const char *pch;

    if (maxlen == -1) {
        maxlen = RDB_KEY_NAME_MAXLEN;
    }

    if (!name || len < 1 || len > maxlen) {
        // non name
        return 0;
    }

    pch = name;

    while (pch - name < len) {
        if (! islower(*pch) && *pch != '_') {
            return 0;
        }

        pch++;
    }

    // success
    return 1;
}


static int RDBSQLNameValidateMaxLen (const char *name, int maxlen)
{
    if (!name) {
        return 0;
    } else {
        return RDBSQLNameValidate(name, (int)strnlen(name, maxlen+1), maxlen);
    }
}


static int parse_table (char *table, char tablespace[RDB_KEY_NAME_MAXLEN + 1], char tablename[RDB_KEY_NAME_MAXLEN + 1])
{
    int len = (int) strlen(table);
    char *p = strchr(table, '.');

    if (p && p == strrchr(table, '.')) {
        snprintf(tablespace, RDB_KEY_NAME_MAXLEN, "%.*s", (int)(p - table), table);
        *p++ = 0;
        snprintf(tablename, RDB_KEY_NAME_MAXLEN, "%.*s", (int)(len - (p - table)), p);

        if (! RDBSQLNameValidateMaxLen(tablespace, RDB_KEY_NAME_MAXLEN)) {
            return -1;
        }

        if (! RDBSQLNameValidateMaxLen(tablename, RDB_KEY_NAME_MAXLEN)) {
            return -1;
        }

        // ok
        return 1;
    }

    // not found
    return 0;
}


static int parse_fields (char *fields, char *fieldnames[RDBAPI_ARGV_MAXNUM])
{
    char namebuf[RDB_KEY_NAME_MAXLEN * 2 + 1];

    char *p;
    int i = 0;

    if (fields[0] == '*') {
        // all fields
        return -1;
    }

    p = strtok(fields, ",");
    while (p) {
        char *name;

        snprintf(namebuf, RDB_KEY_NAME_MAXLEN * 2, "%s", p);

        name = cstr_LRtrim_chr(namebuf, 32);

        if (! RDBSQLNameValidateMaxLen(name, RDB_KEY_NAME_MAXLEN)) {
            return 0;
        }

        fieldnames[i++] = strdup(name);

        p = strtok(0, ",");
    }

    return i;
}


static char * CheckNewFieldName (const char *name)
{
    char *namebuf;
    char tmpbuf[RDB_KEY_NAME_MAXLEN + 32];
    snprintf(tmpbuf, RDB_KEY_NAME_MAXLEN + 30, "%s", name);
    namebuf = cstr_LRtrim_chr(tmpbuf, 32);
    if (RDBSQLNameValidateMaxLen(namebuf, RDB_KEY_NAME_MAXLEN)) {
        return strdup(namebuf);
    }
    return NULL;
}


static char * CheckNewFieldValue (const char *value)
{
    char *valbuf;
    char tmpbuf[RDB_KEY_VALUE_SIZE + 32];
    snprintf(tmpbuf, RDB_KEY_VALUE_SIZE+30, "%s", value);
    valbuf = cstr_LRtrim_chr(cstr_LRtrim_chr(tmpbuf, 32), 39);
    return strlen(valbuf) < RDB_KEY_VALUE_SIZE? strdup(valbuf) : NULL;
}


// WHERE cretime > 123456 AND cretime < 999999
static int parse_where (char *wheres, char *fieldnames[RDBAPI_ARGV_MAXNUM],
    RDBSqlExpr fieldexprs[RDBAPI_ARGV_MAXNUM],
    char *fieldvalues[RDBAPI_ARGV_MAXNUM])
{
    int i, k = 0;
    char *substrs[RDBAPI_ARGV_MAXNUM] = {0};

    int num = cstr_split_substr(wheres, " AND ", 5, substrs, RDBAPI_ARGV_MAXNUM);

    for (i = 0; i < num; i++) {
        char *pair[2] = {0};

        // a <> b
        if (cstr_split_substr(substrs[i], "<>", 2, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBSQLEX_NOT_EQUAL;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a != b
        if (cstr_split_substr(substrs[i], "!=", 2, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBSQLEX_NOT_EQUAL;
            fieldvalues[k] = strdup(pair[1]);
            k++;
            continue;
        }

        // a >= b
        if (cstr_split_substr(substrs[i], ">=", 2, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBSQLEX_GREAT_EQUAL;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a <= b
        if (cstr_split_substr(substrs[i], "<=", 2, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBSQLEX_LESS_EQUAL;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a = b
        if (cstr_split_substr(substrs[i], "=", 1, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBSQLEX_EQUAL;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a > b
        if (cstr_split_substr(substrs[i], ">", 1, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBSQLEX_GREAT_THAN;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a < b
        if (cstr_split_substr(substrs[i], "<", 1, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBSQLEX_LESS_THAN;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a LLIKE b
        if (cstr_split_substr(substrs[i], " LLIKE ", 7, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBSQLEX_LEFT_LIKE;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a RLIKE b
        if (cstr_split_substr(substrs[i], " RLIKE ", 7, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBSQLEX_RIGHT_LIKE;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a LIKE b
        if (cstr_split_substr(substrs[i], " LIKE ", 6, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBSQLEX_LIKE;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a MATCH b
        if (cstr_split_substr(substrs[i], " MATCH ", 7, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBSQLEX_REG_MATCH;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // error
        return (-1);
    }

    return k;
}


// SELECT $fields FROM $tablespace.$tablename WHERE $condition OFFSET $position LIMIT $count
//
RDBAPI_BOOL onParseSelect (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
    char *psz;

    char table[RDB_KEY_NAME_MAXLEN * 2 + 10 + 1] = {0};
    char fields[1024] = {0};    
    char wheres[1024] = {0};

    char offset[32] = {0};
    char limit[32] = {0};

    ub8 limitTmp = 0;

    char * _from = strstr(sql, " FROM ");
    if (! _from) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "(SELECT) error - FROM not found");
        return RDBAPI_FALSE;
    }

    snprintf(fields, sizeof(fields) - 1, "%.*s", (int)(_from - sql), sql);

    snprintf(table, sizeof(table) - 1, "%.*s", RDB_KEY_NAME_MAXLEN * 2 + 10, _from + strlen(" FROM "));

    psz = strstr(table, " WHERE ");
    if (psz) {
        *psz = 0;

        psz = strstr(sql, " WHERE ");
        snprintf(wheres, sizeof(wheres) - 1, "%.*s", len - (int)(psz - sql), psz + strlen(" WHERE "));

        psz = strstr(wheres, " OFFSET ");
        if (psz) {
            *psz = 0;
        }
        psz = strstr(wheres, " LIMIT ");
        if (psz) {
            *psz = 0;
        }
    }

    psz = strstr(sql, " OFFSET ");
    if (psz) {
        snprintf(offset, sizeof(offset) - 1, "%.*s", len - (int)(psz - sql), psz + strlen(" OFFSET "));
        psz = strstr(offset, " LIMIT ");
        if (psz) {
            *psz = 0;
        }
    }

    psz = strstr(sql, " LIMIT ");
    if (psz) {
        snprintf(limit, sizeof(limit) - 1, "%.*s", len - (int)(psz - sql), psz + strlen(" LIMIT "));
        psz = strstr(limit, " OFFSET ");
        if (psz) {
            *psz = 0;
        }
    }

    if (! parse_table(cstr_LRtrim_chr(table, 32), parser->select.tablespace, parser->select.tablename)) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "parse_table failed");
        return RDBAPI_FALSE;
    }

    parser->select.count = parse_fields(cstr_LRtrim_chr(fields, 32), parser->select.resultfields);
    if (! parser->select.count) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "parse_fields failed");
        return RDBAPI_FALSE;
    }
  
    parser->select.numfields = parse_where(cstr_LRtrim_chr(wheres, 32), parser->select.fields, parser->select.fieldexprs, parser->select.fieldvals);
    if (parser->select.numfields < 0) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "parse_where failed");
        return RDBAPI_FALSE;
    }

    if (cstr_to_ub8(10, offset, (int) strlen(offset), &parser->select.offset) < 0) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "error OFFSET");
        return RDBAPI_FALSE;
    }

    if (cstr_to_ub8(10, limit, (int) strlen(limit), &limitTmp) < 0) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "error LIMIT");
        return RDBAPI_FALSE;
    }
    parser->select.limit = (ub4)limitTmp;

    return RDBAPI_TRUE;
}


RDBAPI_BOOL onParseDelete (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
    char *psz;

    char table[RDB_KEY_NAME_MAXLEN * 2 + 10 + 1] = {0};
    char wheres[1024] = {0};

    snprintf(table, sizeof(table) - 1, "%.*s", RDB_KEY_NAME_MAXLEN * 2 + 10, sql);

    psz = strstr(table, " WHERE ");
    if (psz) {
        *psz = 0;

        psz = strstr(sql, " WHERE ");
        snprintf(wheres, sizeof(wheres) - 1, "%.*s", len - (int)(psz - sql), psz + strlen(" WHERE "));
    }

    if (! parse_table(cstr_LRtrim_chr(table, 32), parser->delfrom.tablespace, parser->delfrom.tablename)) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "parse_table failed");
        return RDBAPI_FALSE;
    }
  
    if (parse_where(cstr_LRtrim_chr(wheres, 32), parser->delfrom.fields, parser->delfrom.fieldexprs, parser->delfrom.fieldvals) < 0) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "parse_where failed");
        return RDBAPI_FALSE;
    }

    return RDBAPI_FALSE;
}


RDBAPI_BOOL onParseUpdate (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
    return RDBAPI_FALSE;
}


RDBAPI_BOOL onParseCreate (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{

    return RDBAPI_FALSE;
}