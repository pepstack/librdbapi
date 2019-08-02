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

    char tableName[1];
    char whereClause[1];

    char *fields[RDBAPI_ARGV_MAXNUM];

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

    parser = (RDBSQLParser) RDBMemAlloc(sizeof(RDBSQLParser_t) + len + 1);

    memcpy(parser->sql, sql, len);
    parser->sqlen = len;

    if (cstr_startwith(parser->sql, parser->sqlen, "SELECT ", (int)strlen("SELECT "))) {

        parseOk = onParseSelect(parser, ctx, parser->sql + strlen("SELECT "), parser->sqlen - (int)strlen("SELECT "));
    } else if (cstr_startwith(parser->sql, parser->sqlen, "DELETE FROM ", (int)strlen("DELETE FROM "))) {

        parseOk = onParseDelete(parser, ctx, parser->sql + strlen("DELETE FROM "), parser->sqlen - (int)strlen("DELETE FROM "));
    } else if (cstr_startwith(parser->sql, parser->sqlen, "UPDATE ", (int)strlen("UPDATE "))) {

        parseOk = onParseUpdate(parser, ctx, parser->sql + strlen("UPDATE "), parser->sqlen - (int)strlen("UPDATE "));
    } else if (cstr_startwith(parser->sql, parser->sqlen, "CREATE TABLE ", (int)strlen("CREATE TABLE "))) {

        parseOk = onParseCreate(parser, ctx, parser->sql + strlen("CREATE TABLE "), parser->sqlen - (int)strlen("CREATE TABLE "));
    } else {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_RDBSQL: invalid sql");
        RDBCTX_ERRMSG_DONE(ctx);
        parseOk = 0;
    }

    if (! parseOk) {
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
    RDBMemFree(parser);
}


RDBAPI_RESULT RDBSQLExecute (RDBCtx ctx, RDBSQLParser sqlParser)
{
    return RDBAPI_SUCCESS;
}


// SELECT $fields FROM $tablespace.$tablename WHERE $condition OFFSET $position LIMIT $count
//
RDBAPI_BOOL onParseSelect (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
    char fields[4096];

    char * _from = strstr(sql, " FROM ");
    if (! _from) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "(SELECT) error - FROM not found");
        return RDBAPI_FALSE;
    }

    //snprintf(fields, sizeof(fields) - 1, "%.*s", _from + strlen(" FROM "));

    return RDBAPI_FALSE;
}

RDBAPI_BOOL onParseDelete (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
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