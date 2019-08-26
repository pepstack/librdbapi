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
        // DELETE also use select
        struct SELECT {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];

            // fields filter
            int numfields;
            char *fields[RDBAPI_ARGV_MAXNUM];
            RDBFilterExpr fieldexprs[RDBAPI_ARGV_MAXNUM];
            char *fieldvals[RDBAPI_ARGV_MAXNUM];

            // keys filter
            int numkeys;
            char *keys[RDBAPI_ARGV_MAXNUM];
            RDBFilterExpr keyexprs[RDBAPI_ARGV_MAXNUM];
            char *keyvals[RDBAPI_ARGV_MAXNUM];

            // result fields
            int count;
            char *resultfields[RDBAPI_ARGV_MAXNUM];

            ub8 offset;
            ub4 limit;
        } select;

        struct UPSERT_INTO {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];

            int numfields;
            char *fields[RDBAPI_ARGV_MAXNUM];
            RDBFilterExpr fieldexprs[RDBAPI_ARGV_MAXNUM];
            char *fieldvals[RDBAPI_ARGV_MAXNUM];
        } upsert;

        struct CREATE_TABLE {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];

            int numfields;
            RDBFieldDes_t fielddefs[RDBAPI_ARGV_MAXNUM];

            char tablecomment[RDB_KEY_VALUE_SIZE];

            int fail_on_exists;
        } create;

        struct DESC_TABLE {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];
        } desctable;

        struct SHOW_TABLES {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
        } showtables;

        struct DROP_TABLE {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];
        } droptable;

        struct INFO_SECTION {
            RDBNodeInfoSection section;

            // 1-based. 0 for all
            int whichnode;
        } info;
    };

    int sqlen;
    char sql[0];
} RDBSQLParser_t;


RDBAPI_BOOL onParseSelect (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len);
RDBAPI_BOOL onParseDelete (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len);
RDBAPI_BOOL onParseUpsert (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len);
RDBAPI_BOOL onParseCreate (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len);
RDBAPI_BOOL onParseDesc (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len);
RDBAPI_BOOL onParseDrop (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len);
RDBAPI_BOOL onParseInfo (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len);
RDBSQLStmt  onParseShow (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len);


RDBAPI_RESULT RDBSQLParserNew (RDBCtx ctx, const char *sql, size_t sqlen, RDBSQLParser *outParser)
{
    RDBSQLParser parser;
    int parseOk = 0;
    int len = 0;

    *ctx->errmsg = 0;
    
    if (sqlen == (size_t) -1) {
        len = (int) strnlen(sql, RDB_SQL_TOTAL_LEN_MAX + 1);
    } else {
        len = (int) sqlen;
    }

    if (len < 4) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_RDBSQL: sql is too short");
        return RDBAPI_ERR_RDBSQL;
    }

    if (len > RDB_SQL_TOTAL_LEN_MAX) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_RDBSQL: sql is too long");
        return RDBAPI_ERR_RDBSQL;
    }

    parser = (RDBSQLParser) RDBMemAlloc(sizeof(RDBSQLParser_t) + len + 1);

    char * str0 = RDBMemAlloc(len + 1);
    memcpy(str0, sql, len);

    char * str = cstr_trim_chr_mul(str0, "\r\n`", (int) strlen("\r\n`"));

    str = cstr_replace_chr(str, '\t', 32);
    str = cstr_LRtrim_chr(cstr_LRtrim_chr(str, 32), ';');

    parser->sqlen = (int) strlen(str);
    memcpy(parser->sql, str, parser->sqlen);

    RDBMemFree(str0);

    if (cstr_startwith(parser->sql, parser->sqlen, "SELECT ", (int)strlen("SELECT "))) {

        parseOk = onParseSelect(parser, ctx, parser->sql + strlen("SELECT "), parser->sqlen - (int)strlen("SELECT "));
        parser->stmt = parseOk? RDBSQL_SELECT : RDBSQL_INVALID;

    } else if (cstr_startwith(parser->sql, parser->sqlen, "DELETE FROM ", (int)strlen("DELETE FROM "))) {

        parseOk = onParseDelete(parser, ctx, parser->sql + strlen("DELETE FROM "), parser->sqlen - (int)strlen("DELETE FROM "));
        parser->stmt = parseOk? RDBSQL_DELETE : RDBSQL_INVALID;

    } else if (cstr_startwith(parser->sql, parser->sqlen, "UPSERT ", (int)strlen("UPSERT "))) {

        parseOk = onParseUpsert(parser, ctx, parser->sql + strlen("UPSERT "), parser->sqlen - (int)strlen("UPSERT "));
        parser->stmt = parseOk? RDBSQL_UPSERT : RDBSQL_INVALID;

    } else if (cstr_startwith(parser->sql, parser->sqlen, "CREATE TABLE ", (int)strlen("CREATE TABLE "))) {

        parseOk = onParseCreate(parser, ctx, parser->sql + strlen("CREATE TABLE "), parser->sqlen - (int)strlen("CREATE TABLE "));
        parser->stmt = parseOk? RDBSQL_CREATE : RDBSQL_INVALID;

    } else if (cstr_startwith(parser->sql, parser->sqlen, "DESC ", (int)strlen("DESC "))) {

        parseOk = onParseDesc(parser, ctx, parser->sql + strlen("DESC "), parser->sqlen - (int)strlen("DESC "));
        parser->stmt = parseOk? RDBSQL_DESC_TABLE : RDBSQL_INVALID;

    } else if (cstr_startwith(parser->sql, parser->sqlen, "INFO", (int)strlen("INFO"))) {

        parseOk = onParseInfo(parser, ctx, parser->sql + strlen("INFO"), parser->sqlen - (int)strlen("INFO"));
        parser->stmt = parseOk? RDBSQL_INFO_SECTION : RDBSQL_INVALID;

    } else if (cstr_startwith(parser->sql, parser->sqlen, "DROP ", (int)strlen("DROP "))) {

        parseOk = onParseDrop(parser, ctx, parser->sql + strlen("DROP "), parser->sqlen - (int)strlen("DROP "));
        parser->stmt = parseOk? RDBSQL_DROP_TABLE : RDBSQL_INVALID;

    } else if (cstr_startwith(parser->sql, parser->sqlen, "SHOW ", (int)strlen("SHOW "))) {
        parser->stmt = onParseShow(parser, ctx, parser->sql + strlen("SHOW "), parser->sqlen - (int)strlen("SHOW "));

    } else {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_RDBSQL: invalid sql");
        parser->stmt = RDBSQL_INVALID;
    }

    if (parser->stmt == RDBSQL_INVALID) {
        RDBSQLParserFree(parser);
        return RDBAPI_ERR_RDBSQL;
    }

    *outParser = parser;
    return RDBAPI_SUCCESS;
}


void RDBSQLParserFree (RDBSQLParser parser)
{
    if (parser) {
        if (parser->stmt == RDBSQL_SELECT || parser->stmt == RDBSQL_DELETE) {
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
        } else if (parser->stmt == RDBSQL_UPSERT) {
            int i = 0;
            while (i < RDBAPI_ARGV_MAXNUM) {
                char *psz;

                psz = parser->upsert.fields[i];
                RDBMemFree(psz);

                psz = parser->upsert.fieldvals[i];
                RDBMemFree(psz);

                i++;
            }
        }

        RDBMemFree(parser);
    };
}


static int RDBSQLNameValidate (const char *name, int len, int maxlen)
{
    const char *pch;

    if (maxlen == -1) {
        maxlen = RDB_KEY_NAME_MAXLEN;
    }

    if (!name || len < 1) {
        printf("(%s:%d) empty name.\n", __FILE__, __LINE__);
        return 0;
    }

    if (len > maxlen) {
        printf("(%s:%d) name is too long: %.*s\n", __FILE__, __LINE__, len, name);
        return 0;
    }

    if (isdigit(*name)) {
        printf("(%s:%d) start with number ('%c'): %.*s\n", __FILE__, __LINE__, name[0], len, name);
        return 0;
    }

    pch = name;
    while (pch - name < len) {
        if (! isalnum(*pch) && *pch != '_') {
            printf("(%s:%d) illegal char ('%c') in: %.*s\n", __FILE__, __LINE__, *pch, len, name);
            return 0;
        }

        if ( isalpha(*pch) && ! islower(*pch) ) {
            printf("(%s:%d) not a lower char ('%c') in: %.*s\n", __FILE__, __LINE__, *pch, len, name);
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


RDBSQLStmt RDBSQLParserGetStmt (RDBSQLParser sqlParser, char **parsedClause, int pretty)
{
    return sqlParser->stmt;
}


static int parse_table (char *table, char tablespace[RDB_KEY_NAME_MAXLEN + 1], char tablename[RDB_KEY_NAME_MAXLEN + 1])
{
    int len = (int) strlen(table);
    char *p = strchr(table, '.');

    if (p && p == strrchr(table, '.')) {
        snprintf_chkd_V1(tablespace, RDB_KEY_NAME_MAXLEN + 1, "%.*s", (int)(p - table), table);
        *p++ = 0;
        snprintf_chkd_V1(tablename, RDB_KEY_NAME_MAXLEN + 1, "%.*s", (int)(len - (p - table)), p);

        if (strchr(tablename, 32)) {
            *strchr(tablename, 32) = 0;
        }

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

        snprintf_chkd_V1(namebuf, RDB_KEY_NAME_MAXLEN * 2, "%s", p);

        name = cstr_LRtrim_chr(namebuf, 32);

        if (! RDBSQLNameValidateMaxLen(name, RDB_KEY_NAME_MAXLEN)) {
            return 0;
        }

        fieldnames[i++] = strdup(name);

        p = strtok(0, ",");
    }

    return i;
}


static int parse_field_values (char *fields, char *fieldvalues[RDBAPI_ARGV_MAXNUM])
{
    char valbuf[RDB_SQL_TOTAL_LEN_MAX + 1];

    char *p;
    int i = 0;

    if (fields[0] == '*') {
        // all fields
        return -1;
    }

    p = strtok(fields, ",");
    while (p) {
        char *value;

        snprintf_chkd_V1(valbuf, sizeof(valbuf), "%s", p);

        value = cstr_LRtrim_chr(cstr_LRtrim_chr(valbuf, 32), '\'');

        fieldvalues[i++] = strdup(value);

        p = strtok(0, ",");
    }

    return i;
}


static char * CheckNewFieldName (const char *name)
{
    char *namebuf;
    char tmpbuf[RDB_KEY_NAME_MAXLEN + 32];

    snprintf_chkd_V1(tmpbuf, sizeof(tmpbuf), "%s", name);

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

    snprintf_chkd_V1(tmpbuf, sizeof(tmpbuf), "%s", value);

    valbuf = cstr_LRtrim_chr(cstr_LRtrim_chr(tmpbuf, 32), 39);

    return strlen(valbuf) < RDB_KEY_VALUE_SIZE? strdup(valbuf) : NULL;
}


// WHERE cretime > 123456 AND cretime < 999999
static int parse_where (char *wheres, char *fieldnames[RDBAPI_ARGV_MAXNUM],
    RDBFilterExpr fieldexprs[RDBAPI_ARGV_MAXNUM],
    char *fieldvalues[RDBAPI_ARGV_MAXNUM])
{
    int i, num, k = 0;
    char *substrs[RDBAPI_ARGV_MAXNUM] = {0};

    if (!wheres || !*wheres) {
        return 0;
    }

    num = cstr_split_substr(wheres, " AND ", 5, substrs, RDBAPI_ARGV_MAXNUM);

    for (i = 0; i < num; i++) {
        char *pair[2] = {0};

        // a <> b
        if (cstr_split_substr(substrs[i], "<>", 2, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBFIL_NOT_EQUAL;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a != b
        if (cstr_split_substr(substrs[i], "!=", 2, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBFIL_NOT_EQUAL;
            fieldvalues[k] = strdup(pair[1]);
            k++;
            continue;
        }

        // a >= b
        if (cstr_split_substr(substrs[i], ">=", 2, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBFIL_GREAT_EQUAL;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a <= b
        if (cstr_split_substr(substrs[i], "<=", 2, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBFIL_LESS_EQUAL;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a = b
        if (cstr_split_substr(substrs[i], "=", 1, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBFIL_EQUAL;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a > b
        if (cstr_split_substr(substrs[i], ">", 1, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBFIL_GREAT_THAN;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a < b
        if (cstr_split_substr(substrs[i], "<", 1, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBFIL_LESS_THAN;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a LLIKE b
        if (cstr_split_substr(substrs[i], " LLIKE ", 7, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBFIL_LEFT_LIKE;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a RLIKE b
        if (cstr_split_substr(substrs[i], " RLIKE ", 7, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBFIL_RIGHT_LIKE;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a LIKE b
        if (cstr_split_substr(substrs[i], " LIKE ", 6, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBFIL_LIKE;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a MATCH b
        if (cstr_split_substr(substrs[i], " MATCH ", 7, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = RDBFIL_MATCH;
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
// "SELECT   sid FROM   xsdb.logentry      WHERE cretime >= 1564543181 AND cretime <'9999999999' OFFSET 0 LIMIT 20;"
RDBAPI_BOOL onParseSelect (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
    char *psz;

    char table[RDB_KEY_NAME_MAXLEN * 2 + 10 + 1] = {0};
    char fields[RDB_SQL_TOTAL_LEN_MAX + 2] = {0};
    char wheres[RDB_SQL_WHERE_LEN_MAX + 2] = {0};

    char offset[32] = {0};
    char limit[32] = {0};

    ub8 limitTmp = 0;

    char * _from = strstr(sql, " FROM ");
    if (! _from) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(SELECT) error - FROM not found");
        return RDBAPI_FALSE;
    }

    snprintf_chkd_V1(fields, sizeof(fields), "%.*s", (int)(_from - sql), sql);
    snprintf_chkd_V1(table, sizeof(table), "%s", _from + strlen(" FROM "));

    psz = strstr(table, " WHERE ");
    if (psz) {
        *psz = 0;

        psz = strstr(sql, " WHERE ");
        snprintf_chkd_V1(wheres, sizeof(wheres), "%.*s", len - (int)(psz - sql), psz + strlen(" WHERE "));

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
        snprintf_chkd_V1(offset, sizeof(offset), "%.*s", len - (int)(psz - sql), psz + strlen(" OFFSET "));
        psz = strstr(offset, " LIMIT ");
        if (psz) {
            *psz = 0;
        }
    }

    psz = strstr(sql, " LIMIT ");
    if (psz) {
        snprintf_chkd_V1(limit, sizeof(limit), "%.*s", len - (int)(psz - sql), psz + strlen(" LIMIT "));
        psz = strstr(limit, " OFFSET ");
        if (psz) {
            *psz = 0;
        }
    }

    if (! parse_table(cstr_LRtrim_chr(table, 32), parser->select.tablespace, parser->select.tablename)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "parse_table failed");
        return RDBAPI_FALSE;
    }

    parser->select.count = parse_fields(cstr_LRtrim_chr(fields, 32), parser->select.resultfields);
    if (! parser->select.count) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) parse_fields failed", __FILE__, __LINE__);
        return RDBAPI_FALSE;
    }
  
    parser->select.numfields = parse_where(cstr_LRtrim_chr(wheres, 32), parser->select.fields, parser->select.fieldexprs, parser->select.fieldvals);
    if (parser->select.numfields < 0) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "parse_where failed");
        return RDBAPI_FALSE;
    }

    if (cstr_to_ub8(10, offset, (int) strlen(offset), &parser->select.offset) < 0) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "error OFFSET");
        return RDBAPI_FALSE;
    }

    if (cstr_to_ub8(10, limit, (int) strlen(limit), &limitTmp) < 0) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "error LIMIT");
        return RDBAPI_FALSE;
    }
    parser->select.limit = (ub4)limitTmp;

    return RDBAPI_TRUE;
}


RDBAPI_BOOL onParseDelete (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
    char *psz;

    char table[RDB_KEY_NAME_MAXLEN * 2 + 10 + 1] = {0};
    char wheres[RDB_SQL_WHERE_LEN_MAX + 2] = {0};

    char offset[32] = {0};
    char limit[32] = {0};

    ub8 limitTmp = 0;

    snprintf_chkd_V1(table, sizeof(table), "%.*s", len, sql);

    psz = strstr(table, " WHERE ");
    if (psz) {
        *psz = 0;

        psz = strstr(sql, " WHERE ");
        snprintf_chkd_V1(wheres, sizeof(wheres), "%.*s", len - (int)(psz - sql), psz + strlen(" WHERE "));

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
        snprintf_chkd_V1(offset, sizeof(offset), "%.*s", len - (int)(psz - sql), psz + strlen(" OFFSET "));
        psz = strstr(offset, " LIMIT ");
        if (psz) {
            *psz = 0;
        }
    }

    psz = strstr(sql, " LIMIT ");
    if (psz) {
        snprintf_chkd_V1(limit, sizeof(limit), "%.*s", len - (int)(psz - sql), psz + strlen(" LIMIT "));
        psz = strstr(limit, " OFFSET ");
        if (psz) {
            *psz = 0;
        }
    }

    if (! parse_table(cstr_LRtrim_chr(table, 32), parser->select.tablespace, parser->select.tablename)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "parse_table failed");
        return RDBAPI_FALSE;
    }

    parser->select.numfields = parse_where(cstr_LRtrim_chr(wheres, 32), parser->select.fields, parser->select.fieldexprs, parser->select.fieldvals);
    if (parser->select.numfields < 0) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "parse_where failed");
        return RDBAPI_FALSE;
    }

    if (cstr_to_ub8(10, offset, (int) strlen(offset), &parser->select.offset) < 0) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "error OFFSET");
        return RDBAPI_FALSE;
    }

    if (cstr_to_ub8(10, limit, (int) strlen(limit), &limitTmp) < 0) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "error LIMIT");
        return RDBAPI_FALSE;
    }
    parser->select.limit = (ub4)limitTmp;

    return RDBAPI_TRUE;
}


RDBAPI_BOOL onParseUpsert (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int sqlen)
{
    char table[RDB_KEY_NAME_MAXLEN * 2 + 10 + 1] = {0};
    char fields[RDB_SQL_TOTAL_LEN_MAX + 2] = {0};
    char wheres[RDB_SQL_WHERE_LEN_MAX + 2] = {0};
    char *p, *q;

    char *s = cstr_Ltrim_chr(sql, 32);
    int len = cstr_length(s, sqlen);

    if (! cstr_startwith(s, len, "INTO ", 5)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "invalid sql clause for UPSERT: INTO not found");
        return RDBAPI_FALSE; 
    }

    s += 5;
    len -= 5;

    s = cstr_Ltrim_chr(s, 32);
    len = cstr_length(s, sqlen);

    p = cstr_Lfind_chr(s, len, '(');
    if (!p) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "invalid sql clause for UPSERT");
        return RDBAPI_FALSE;
    }

    snprintf_chkd_V1(table, sizeof(table), "%.*s", (int)(p - s), s);

    if (! parse_table(table, parser->upsert.tablespace, parser->upsert.tablename)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "parse_table failed");
        return RDBAPI_FALSE;
    }

    q = cstr_Lfind_chr(p, len, ')');
    if (!q) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "invalid sql clause for UPSERT: fields not found");
        return RDBAPI_FALSE;
    }

    *p++ = 0;
    *q++ = 0;

    parser->upsert.numfields = parse_fields(p, parser->upsert.fields);
    if (! parser->upsert.numfields) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) parse_fields failed", __FILE__, __LINE__);
        return RDBAPI_FALSE;
    }

    s = cstr_Ltrim_chr(q, 32);
    len = cstr_length(s, sqlen);

    if (! cstr_startwith(s, len, "VALUES", 6)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) invalid sql clause: VALUES not found", __FILE__, __LINE__);
        return RDBAPI_FALSE;
    }

    s += 6;
    len -= 6;

    p = cstr_Ltrim_chr(s, 32);
    len = cstr_length(p, sqlen);

    if (*p != '(') {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) invalid sql clause: '(' not found", __FILE__, __LINE__);
        return RDBAPI_FALSE;
    }

    q = cstr_Lfind_chr(p, len, ')');
    if (! q) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) invalid sql clause: ')' not found", __FILE__, __LINE__);
        return RDBAPI_FALSE;
    }

    *p++ = 0;
    *q++ = 0;

    if (parse_field_values(p, parser->upsert.fieldvals) != parser->upsert.numfields) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) parse_field_values failed", __FILE__, __LINE__);
        return RDBAPI_FALSE;
    }

    return RDBAPI_TRUE;
}


RDBAPI_BOOL onParseCreate (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
    /**
     * redplus.exe -S"CREATE TABLE IF NOT EXISTS xsdb.connect (sid UB4 NOT NULL COMMENT 'server id', connfd UB4 NOT NULL COMMENT 'connect socket fd', sessionid UB8X COMMENT 'session id', port UB4 COMMENT 'client port', host STR(30) COMMENT 'client host ip', hwaddr STR(30) COMMENT 'mac addr', agent STR(30) COMMENT 'client agent', ROWKEY(sid | connfd) ) COMMENT 'log file entry';"
     *
     *   CREATE TABLE IF NOT EXISTS xsdb.connect (
     *      sid UB4 NOT NULL COMMENT 'server id',
     *      connfd UB4 NOT NULL COMMENT 'connect socket fd',
     *      sessionid UB8X COMMENT 'session id',
     *      port UB4 COMMENT 'client port', host STR(30) COMMENT 'client host ip',
     *      hwaddr STR(30) COMMENT 'mac addr',
     *      agent STR(30) COMMENT 'client agent',
     *      ROWKEY(sid | connfd)
     *   ) COMMENT 'log file entry';"
     */
    char table[RDB_KEY_NAME_MAXLEN * 2 + 10 + 1] = {0};

    char * ifnex = NULL;

    char *leftp  = strchr(sql, '(');
    char *rightp = strrchr(sql, ')');

    const char **vtnames = (const char **) ctx->env->valtypenames;

    if (!leftp || !rightp || (rightp - leftp) < 16) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "error CREATE TABLE grammar");
        return RDBAPI_FALSE;      
    }

    ifnex = strstr(sql, "IF NOT EXISTS ");
    if (ifnex) {
        if (leftp - ifnex > 14) {
            snprintf_chkd_V1(table, sizeof(table), "%.*s", (int)(leftp - ifnex - 14), ifnex + 14);
        }
        parser->create.fail_on_exists = 0;
    } else {
        snprintf_chkd_V1(table, sizeof(table), "%.*s", (int)(leftp - sql), sql);
        parser->create.fail_on_exists = 1;
    }

    if (! parse_table(cstr_LRtrim_chr(table, 32), parser->create.tablespace, parser->create.tablename)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "parse_table failed");
        return RDBAPI_FALSE;
    }

    do {
        RDBFieldDes_t fielddefs[RDBAPI_ARGV_MAXNUM] = {0};
        int numfields = 0;

        int n, nflds;
        char *flds[RDBAPI_ARGV_MAXNUM];

        char *rowkeys[RDBAPI_SQL_KEYS_MAX] = {0};
        int k, rk = 0;

        n = cstr_slpit_chr(leftp + 1, (int)(rightp - leftp - 1), ',', 0, 0);
        if (! n) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "field not found");
            return RDBAPI_FALSE;
        }
        if (n > RDBAPI_ARGV_MAXNUM) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "too many fields");
            return RDBAPI_FALSE;
        }

        nflds = cstr_slpit_chr(leftp + 1, (int)(rightp - leftp - 1), ',', flds, n);

        for (n = 0; n < nflds; n++) {
            char *pfn = flds[n];
            char *fld = cstr_LRtrim_chr(pfn, 32);

            flds[n] = strdup(fld);
            fld = flds[n];

            free(pfn);

            if (cstr_startwith(fld, (int) strlen(fld), "ROWKEY", 6)) {
                char *lp = strchr(fld, '(');
                char *rp = strrchr(fld, ')');

                if (lp && rp && rp > lp) {
                    k = cstr_slpit_chr(lp + 1, (int)(rp - lp - 1), '|', 0, 0);

                    if (! k) {
                        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "ROWKEY not found");
                        return RDBAPI_FALSE;
                    }

                    if (k > RDBAPI_SQL_KEYS_MAX) {
                        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "ROWKEY too many");
                        return RDBAPI_FALSE;
                    }

                    rk = cstr_slpit_chr(lp + 1, (int)(rp - lp - 1), '|', rowkeys, k);
                }

                break;
            }
        }

        for (n = 0; n < nflds; n++) {
            char *fld = flds[n];

            if (! cstr_startwith(fld, (int) strlen(fld), "ROWKEY", 6)) {
                char *comment = NULL;

                char *p = strchr(fld, 32);
                if (! p) {
                    // ERROR
                }

                comment = strstr(fld, " COMMENT ");
                if (comment) {
                    *comment++ = 0;
                }

                *p++ = 0;

                fld = cstr_LRtrim_chr(fld, 32);
                snprintf_chkd_V1(fielddefs[numfields].fieldname, sizeof(fielddefs[numfields].fieldname), "%s", fld);

                fld = cstr_Ltrim_chr(p, 32);
                int j = cstr_startwith_mul(fld, (int) strlen(fld), vtnames, NULL, 256);
                if (j == -1) {
                    // TODO: ERROR: unknown type
                    
                }

                fielddefs[numfields].fieldtype = (RDBValueType) j;

                if (fielddefs[numfields].fieldtype == RDBVT_STR || fielddefs[numfields].fieldtype == RDBVT_BLOB) {
                    // STR(length)
                    char *a = strchr(fld, '(');
                    char *b = strchr(fld, ')');
                    char valbuf[22];

                    if (!a || !b || b-a < 2) {
                        // error
                    }

                    if (b - a > 20) {
                        // error
                    }

                    snprintf_chkd_V1(valbuf, sizeof(valbuf), "%.*s", (int)(b - a - 1), a + 1);

                    fielddefs[numfields].length = atoi(cstr_LRtrim_chr(valbuf, 32));
                    if (fielddefs[numfields].length <= 0) {
                        // error

                    }
                } else if (fielddefs[numfields].fieldtype == RDBVT_DEC) {
                    // DEC(length|dscale)
                    // error: not support now
                }

                fielddefs[numfields].rowkey = 1 + cstr_findstr_in(fielddefs[numfields].fieldname, (int)strlen(fielddefs[numfields].fieldname), (const char **)rowkeys, rk);

                if (strstr(fld, " NOT NULL")) {
                    fielddefs[numfields].nullable = 0;
                } else {
                    fielddefs[numfields].nullable = 1;
                }

                if (comment) {
                    char *c1 = strchr(comment, '\'');
                    char *c2 = strrchr(comment, '\'');

                    if (!c1 || !c2 || c2 - c1 < 1) {
                        // error
                    }

                    snprintf_chkd_V1(fielddefs[numfields].comment, sizeof(fielddefs[numfields].comment), "%.*s", (int)(c2 - c1 - 1), c1 + 1);
                }

                numfields++;
            }

            free(flds[n]);
        }

        for (k = 0; k < rk; k++) {
            free(rowkeys[k]);
        }

        memcpy(parser->create.fielddefs, fielddefs, numfields * sizeof(fielddefs[0]));

        parser->create.numfields = numfields;
    } while (0);

    if (strstr(rightp, "COMMENT ")) {
        char *t1 = strchr(strstr(rightp, "COMMENT "), '\'');
        char *t2 = strrchr(strstr(rightp, "COMMENT "), '\'');

        if (!t1 || !t2 || t2 - t1 < 2) {
            // error
        }

        snprintf_chkd_V1(parser->create.tablecomment, sizeof(parser->create.tablecomment), "%.*s", (int)(t2 - t1 - 1), t1 + 1);
    }

    return RDBAPI_TRUE;
}


RDBAPI_BOOL onParseDesc (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
    char table[RDB_KEY_NAME_MAXLEN * 2 + 10 + 1] = {0};

    snprintf_chkd_V1(table, sizeof(table), "%s", sql);

    if (! parse_table(cstr_LRtrim_chr(table, 32), parser->desctable.tablespace, parser->desctable.tablename)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) parse_table failed", __FILE__, __LINE__);
        return RDBAPI_FALSE;
    }

    return RDBAPI_TRUE;
}


RDBAPI_BOOL onParseDrop (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
    char table[RDB_KEY_NAME_MAXLEN * 2 + 10 + 1] = {0};

    if (! strstr(sql, "TABLE ")) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) TABLE not found", __FILE__, __LINE__);
        return RDBAPI_FALSE;
    }

    snprintf_chkd_V1(table, sizeof(table), "%s", strstr(sql, "TABLE ") + 6);

    if (! parse_table(cstr_LRtrim_chr(table, 32), parser->droptable.tablespace, parser->droptable.tablename)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) parse_table failed", __FILE__, __LINE__);
        return RDBAPI_FALSE;
    }

    return RDBAPI_TRUE;
}


RDBAPI_BOOL onParseInfo (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
    int chlen;

    // 0 is for all
    int nodeid_1_based = 0;
    char * sec = cstr_Rtrim_chr(sql, 32);

    // nodeid is 1-based
    char * nodeid = strrchr(sec, 32);
    if (nodeid) {
        *nodeid++ = 0;

        nodeid_1_based = atoi(nodeid);
        if (nodeid_1_based < 0 || nodeid_1_based > RDBEnvNumNodes(ctx->env)) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) invalid node id(%s). nodeid=0,1,...,%d", __FILE__, __LINE__, nodeid, RDBEnvNumNodes(ctx->env));
            return RDBAPI_FALSE;
        }
    }

    parser->info.whichnode = nodeid_1_based;

    sec = cstr_LRtrim_chr(sec, 32);

    chlen = cstr_length(sec, 12);
    if (! chlen) {
        parser->info.section = MAX_NODEINFO_SECTIONS;
        return RDBAPI_TRUE;
    } else {
        // same with RDBNodeInfoSection
        const char *sections[] = {
            "SERVER",        // 0
            "CLIENTS",       // 1
            "MEMORY",        // 2
            "PERSISTENCE",   // 3
            "STATS",         // 4
            "REPLICATION",   // 5
            "CPU",           // 6
            "CLUSTER",       // 7
            "KEYSPACE",      // 8
            0
        };

        int i = 0;
        const char *section = NULL;
        
        cstr_toupper(sec, chlen);

        while ((section = sections[i]) != NULL) {
            if (! cstr_compare_len(section, (int)strlen(section), sec, chlen)) {
                parser->info.section = (RDBNodeInfoSection) i;
                break;
            }

            i++;
        }

        if (section) {
            return RDBAPI_TRUE;
        }

        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "Bad INFO Section(%.*s). (Section should be: %s | %s | %s | %s | %s | %s | %s | %s)",
            cstr_length(sec, 12), sec,
            sections[0],
            sections[1],
            sections[2],
            sections[3],
            sections[4],
            sections[5],
            sections[6],
            sections[7],
            sections[8]);

        return RDBAPI_FALSE;
    }
}

RDBSQLStmt  onParseShow (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
    char *s = cstr_LRtrim_chr(cstr_LRtrim_chr(sql, 32), ';');

    len = cstr_length(s, 10);
    if (len < 6) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) bad sql. should be: SHOW DATABASES | TABLES <database>", __FILE__, __LINE__);
        return RDBSQL_INVALID;
    }

    if (len == 9 && ! strcmp(s, "DATABASES")) {
        // SHOW DATABASES
        return RDBSQL_SHOW_DATABASES;
    }

    if (len == 6 && ! strcmp(s, "TABLES")) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) bad sql. database name not given. should be: SHOW TABLES database;", __FILE__, __LINE__);
        return RDBSQL_INVALID;
    }

    if (strstr(s, "TABLES ") == s) {
        char * dbname = cstr_Ltrim_chr(s + 7, 32);
        len = cstr_length(dbname, RDB_KEY_NAME_MAXLEN + 1);
        if (len < 2) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) bad sql. database name is too short: %s", __FILE__, __LINE__, dbname);
            return RDBSQL_INVALID;
        }
        if (len > RDB_KEY_NAME_MAXLEN) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) bad sql. database name is too long: %.*s...", __FILE__, __LINE__, RDB_KEY_NAME_MAXLEN, dbname);
            return RDBSQL_INVALID;
        }

        if (!strcmp(dbname, "redisdb")) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) bad sql. redisdb not allowed", __FILE__, __LINE__, RDB_KEY_NAME_MAXLEN);
            return RDBSQL_INVALID;
        }

        snprintf_chkd_V1(parser->showtables.tablespace, sizeof(parser->showtables.tablespace), "%.*s", len, dbname);
        return RDBSQL_SHOW_TABLES;
    }

    snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) bad sql: SHOW %s", __FILE__, __LINE__, s);
    return RDBSQL_INVALID;
}

static void RDBResultRowDeleteCallback (void * _row, void * _map)
{
    RDBResultMap resultMap = (RDBResultMap)_map;
    RDBResultRow resultRow = (RDBResultRow)_row;

    RedisDeleteKey(resultMap->ctxh, resultRow->replykey->str, NULL, 0);
}


ub8 RDBSQLExecute (RDBCtx ctx, RDBSQLParser parser, RDBResultMap *outResultMap)
{
    RDBAPI_RESULT res;

    *outResultMap = NULL;

    const char **vtnames = (const char **) ctx->env->valtypenames;

    if (parser->stmt == RDBSQL_SELECT || parser->stmt == RDBSQL_DELETE) {
        RDBResultMap resultMap;

        res = RDBTableScanFirst(ctx,
                parser->stmt,
                parser->select.tablespace, parser->select.tablename,
                parser->select.numkeys,
                (const char **) parser->select.keys,
                parser->select.keyexprs,
                (const char **) parser->select.keyvals,
                parser->select.numfields,
                (const char **) parser->select.fields,
                parser->select.fieldexprs,
                (const char **) parser->select.fieldvals,
                NULL,
                NULL,
                parser->select.count,
                (const char **) parser->select.resultfields,
                &resultMap);

        if (res == RDBAPI_SUCCESS) {
            ub8 offset = RDBTableScanNext(resultMap, parser->select.offset, parser->select.limit);

            if (offset != RDB_ERROR_OFFSET) {
                if (parser->stmt == RDBSQL_DELETE) {
                    RDBResultMapTraverse(resultMap, RDBResultRowDeleteCallback, resultMap);
                }

                *outResultMap = resultMap;
                return offset;
            }

            RDBResultMapFree(resultMap);
            return 0;
        }
    } else if (parser->stmt == RDBSQL_CREATE) {
        RDBTableDes_t tabledes = {0};
        res = RDBTableDescribe(ctx, parser->create.tablespace, parser->create.tablename, &tabledes);

        if (res == RDBAPI_SUCCESS && parser->create.fail_on_exists) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERROR: table already existed");
            return (ub8) RDBAPI_ERROR;
        }

        res = RDBTableCreate(ctx, parser->create.tablespace, parser->create.tablename, parser->create.tablecomment, parser->create.numfields, parser->create.fielddefs);

        if (res == RDBAPI_SUCCESS) {
            bzero(&tabledes, sizeof(tabledes));

            res = RDBTableDescribe(ctx, parser->create.tablespace, parser->create.tablename, &tabledes);

            if (res == RDBAPI_SUCCESS && tabledes.nfields == parser->create.numfields) {
                RDBResultMap resultMap;

                RDBResultMapNew(ctx, NULL, parser->stmt, parser->create.tablespace, parser->create.tablename, tabledes.nfields, tabledes.fielddes, NULL, &resultMap);

                RDBResultMapSetDelimiter(resultMap, RDB_TABLE_DELIMITER_CHAR);

                *outResultMap = resultMap;
                return RDBAPI_SUCCESS;
            }
        }
    } else if (parser->stmt == RDBSQL_DESC_TABLE) {
        RDBTableDes_t tabledes = {0};

        if (RDBTableDescribe(ctx, parser->desctable.tablespace, parser->desctable.tablename, &tabledes) != RDBAPI_SUCCESS) {
            return (ub8) RDBAPI_ERROR;
        } else {
            RDBResultMap resultMap;
            RDBResultMapNew(ctx, NULL, parser->stmt, parser->create.tablespace, parser->create.tablename, tabledes.nfields, tabledes.fielddes, NULL, &resultMap);

            resultMap->table_timestamp = tabledes.table_timestamp;

            snprintf_chkd_V1(resultMap->table_datetime, sizeof(resultMap->table_datetime), "%s", tabledes.table_datetime);
            snprintf_chkd_V1(resultMap->table_comment, sizeof(resultMap->table_comment), "%s", tabledes.table_comment);

            *outResultMap = resultMap;
            return RDBAPI_SUCCESS;
        }
    } else if (parser->stmt == RDBSQL_DROP_TABLE) {
        RDBTableDes_t tabledes = {0};
        char keyname[256];

        if (RDBTableDescribe(ctx, parser->droptable.tablespace, parser->droptable.tablename, &tabledes) == RDBAPI_SUCCESS) {
            int i, rklen = (int) strlen(tabledes.table_rowkey);

            for (i = 0; i < tabledes.nfields; i++) {
                snprintf_chkd_V1(keyname, sizeof(keyname), "%.*s:%s}", rklen - 1, tabledes.table_rowkey, tabledes.fielddes[i].fieldname);
                RedisDeleteKey(ctx, keyname, NULL, 0);
            }

            return RedisDeleteKey(ctx, tabledes.table_rowkey, NULL, 0);
        }        
    } else if (parser->stmt == RDBSQL_INFO_SECTION) {
        if (RDBCtxCheckInfo(ctx, parser->info.section, parser->info.whichnode) == RDBAPI_SUCCESS) {
            int section;

            char keyprefix[RDBAPI_PROP_MAXSIZE];

            RDBResultMap resultMap;

            const char *sections[] = {
                "Server",        // 0
                "Clients",       // 1
                "Memory",        // 2
                "Persistence",   // 3
                "Stats",         // 4
                "Replication",   // 5
                "Cpu",           // 6
                "Cluster",       // 7
                "Keyspace",      // 8
                0
            };

            int startInfoSecs = parser->info.section;
            int nposInfoSecs = parser->info.section + 1;
            if (parser->info.section == MAX_NODEINFO_SECTIONS) {
                startInfoSecs = NODEINFO_SERVER;
                nposInfoSecs = MAX_NODEINFO_SECTIONS;
            }

            RDBResultMapNew(ctx, NULL, parser->stmt, NULL, NULL, 0, NULL, NULL, &resultMap);

            threadlock_lock(&ctx->env->thrlock);

            do {
                int nodeindex = 0;
                int endNodeid = RDBEnvNumNodes(ctx->env);

                if (parser->info.whichnode) {
                    endNodeid = parser->info.whichnode;                    
                    nodeindex = endNodeid - 1;                    
                }

                for (; nodeindex < endNodeid; nodeindex++) {
                    RDBCtxNode ctxnode = RDBCtxGetNode(ctx, nodeindex);
                    RDBEnvNode envnode = RDBCtxNodeGetEnvNode(ctxnode);

                    // node as row: {clusternode(%d)::$host:$port}
                    RDBResultRow rowdata = (RDBResultRow) RDBMemAlloc(sizeof(RDBResultRow_t));
                    int len = snprintf_chkd_V1(keyprefix, sizeof(keyprefix), "{clusternode(%d)::%s}", envnode->index + 1, envnode->key);

                    rowdata->replykey = RDBStringReplyCreate(keyprefix, len);

                    // section as fields
                    for (section = startInfoSecs; section != nposInfoSecs; section++) {
                        RDBPropNode propnode;
                        RDBPropMap propmap = envnode->nodeinfo[section];

                        for (propnode = propmap; propnode != NULL; propnode = propnode->hh.next) {
                            RDBNameReply nnode;

                            int namelen = snprintf_chkd_V1(keyprefix, sizeof(keyprefix), "%s::%s", sections[section], propnode->name);
                            int valuelen = cstr_length(propnode->value, RDB_ROWKEY_MAX_SIZE);

                            nnode = (RDBNameReply) RDBMemAlloc(sizeof(RDBNameReply_t) + namelen + 1 + valuelen + 1);

                            nnode->name = nnode->namebuf;
                            nnode->namelen = namelen;
                            memcpy(nnode->namebuf, keyprefix, namelen);

                            nnode->subval = &nnode->namebuf[namelen + 1];
                            nnode->sublen = valuelen;
                            memcpy(nnode->subval, propnode->value, valuelen);

                            HASH_ADD_STR_LEN(rowdata->fieldmap, name, nnode->namelen, nnode);
                        }
                    }

                    do {
                        // insert row into result
                        int is_new_node;
                        red_black_node_t *node = rbtree_insert_unique(&resultMap->rbtree, (void *) rowdata, &is_new_node);

                        if (! node) {
                            // out of memory
                            RDBResultRowFree(rowdata);
                            RDBResultMapFree(resultMap);
                            return RDBAPI_ERR_NOMEM;
                        }

                        if (! is_new_node) {
                            RDBResultRowFree(rowdata);
                            RDBResultMapFree(resultMap);
                            return RDBAPI_ERR_EXISTED;
                        }
                    } while(0);
                }
            } while(0);

            threadlock_unlock(&ctx->env->thrlock);

            *outResultMap = resultMap;
            return RDBAPI_SUCCESS;
        }
    } else if (parser->stmt == RDBSQL_SHOW_DATABASES) {
        // {redisdb::$database:*}
        RDBResultMap resultMap;
        redisReply *replyRows;

        RDBBlob_t pattern = {0};

        // result cursor state
        RDBTableCursor_t *nodestates = RDBMemAlloc(sizeof(RDBTableCursor_t) * RDBEnvNumNodes(ctx->env));

        int nodeindex = 0;
        int prefixlen = (int) strlen(RDB_SYSTEM_TABLE_PREFIX) + 3;

        RDBResultMapNew(ctx, NULL, parser->stmt, NULL, NULL, 0, NULL, NULL, &resultMap);

        pattern.maxsz = prefixlen + 8;
        pattern.str = RDBMemAlloc(pattern.maxsz);
        pattern.length = snprintf_chkd_V1(pattern.str, pattern.maxsz, "{%s::*:*}", RDB_SYSTEM_TABLE_PREFIX);

        while (nodeindex < RDBEnvNumNodes(ctx->env)) {
            RDBEnvNode envnode = RDBEnvGetNode(ctx->env, nodeindex);

            // scan only on master node
            if (RDBEnvNodeGetMaster(envnode, NULL) == RDBAPI_TRUE) {
                RDBCtxNode ctxnode = RDBCtxGetNode(ctx, nodeindex);

                RDBTableCursor nodestate = &nodestates[nodeindex];
                if (nodestate->finished) {
                    // goto next node since this current node has finished
                    nodeindex++;
                    continue;
                }

                res = RDBTableScanOnNode(ctxnode, nodestate, &pattern, 200, &replyRows);

                if (res == RDBAPI_SUCCESS) {
                    // here we should add new rows
                    size_t i = 0;

                    for (; i != replyRows->elements; i++) {
                        redisReply * reply = replyRows->element[i];

                        if (cstr_startwith(reply->str, reply->len, pattern.str, prefixlen)) {
                            char *end = strchr(reply->str + prefixlen, ':');
                            if (end) {
                                redisReply * replydb;

                                *end = '\0';

                                replydb = RDBStringReplyCreate(reply->str + prefixlen, end - reply->str - prefixlen);

                                RDBResultMapInsert(resultMap, replydb);
                            }
                        }
                    }

                    RedisFreeReplyObject(&replyRows);
                }
            } else {
                nodeindex++;
            }
        }

        RDBMemFree(nodestates);
        RDBMemFree(pattern.str);

        *outResultMap = resultMap;
        return RDBResultMapSize(resultMap);
    } else if (parser->stmt == RDBSQL_SHOW_TABLES) {
        // {redisdb::database:$tablename}
        RDBResultMap resultMap;        
        redisReply *replyRows;
        
        RDBBlob_t pattern = {0};

        // result cursor state
        RDBTableCursor_t *nodestates = RDBMemAlloc(sizeof(RDBTableCursor_t) * RDBEnvNumNodes(ctx->env));

        int nodeindex = 0;
        int dbnlen = cstr_length(parser->showtables.tablespace, RDB_KEY_NAME_MAXLEN);
        int prefixlen = (int) strlen(RDB_SYSTEM_TABLE_PREFIX) + 3;

        RDBResultMapNew(ctx, NULL, parser->stmt, NULL, NULL, 0, NULL, NULL, &resultMap);

        pattern.maxsz = prefixlen + dbnlen + 8;
        pattern.str = RDBMemAlloc(pattern.maxsz);
        pattern.length = snprintf_chkd_V1(pattern.str, pattern.maxsz, "{%s::%.*s:*}", RDB_SYSTEM_TABLE_PREFIX, dbnlen, parser->showtables.tablespace);

        while (nodeindex < RDBEnvNumNodes(ctx->env)) {
            RDBEnvNode envnode = RDBEnvGetNode(ctx->env, nodeindex);

            // scan only on master node
            if (RDBEnvNodeGetMaster(envnode, NULL) == RDBAPI_TRUE) {
                RDBCtxNode ctxnode = RDBCtxGetNode(ctx, nodeindex);

                RDBTableCursor nodestate = &nodestates[nodeindex];
                if (nodestate->finished) {
                    // goto next node since this current node has finished
                    nodeindex++;
                    continue;
                }

                res = RDBTableScanOnNode(ctxnode, nodestate, &pattern, 200, &replyRows);

                if (res == RDBAPI_SUCCESS) {
                    // here we should add new rows
                    size_t i = 0;

                    for (; i != replyRows->elements; i++) {
                        redisReply * reply = replyRows->element[i];

                        char *end = strchr(reply->str + prefixlen + dbnlen + 1, ':');
                        if (end) {
                            redisReply * replytbl;

                            *end = 0;
                            *(reply->str + prefixlen - 1) = 0;
                            *(reply->str + prefixlen + dbnlen) = '.';

                            replytbl = RDBStringReplyCreate(reply->str + prefixlen, end - reply->str - prefixlen);

                            RDBResultMapInsert(resultMap, replytbl);
                        }
                    }

                    RedisFreeReplyObject(&replyRows);
                }
            } else {
                nodeindex++;
            }
        }

        RDBMemFree(nodestates);
        RDBMemFree(pattern.str);

        *outResultMap = resultMap;
        return RDBResultMapSize(resultMap);
    }

    return (ub8) RDBAPI_ERROR;
}


ub8 RDBSQLExecuteSQL (RDBCtx ctx, const RDBBlob_t *sqlblob, RDBResultMap *outResultMap)
{
    int err;
    ub8 offset;
    RDBResultMap resultMap;

    RDBSQLParser sqlparser = NULL;

    *outResultMap = NULL;

    err = RDBSQLParserNew(ctx, sqlblob->str, sqlblob->length, &sqlparser);
    if (err) {
        printf("RDBSQLParserNew failed: %s\n", RDBCtxErrMsg(ctx));
        goto ret_error;
    }

    offset = RDBSQLExecute(ctx, sqlparser, &resultMap);
    if (offset == RDBAPI_ERROR) {
        printf("RDBSQLExecute failed: %s", RDBCtxErrMsg(ctx));
        goto ret_error;
    }

    // success
    RDBSQLParserFree(sqlparser);

    *outResultMap = resultMap;
    return (ub8) offset;

ret_error:
    RDBSQLParserFree(sqlparser);
    return (ub8) RDBAPI_ERROR;
}


int RDBSQLExecuteFile (RDBCtx ctx, const char *sqlfile, RDBResultMap **outResultMaps)
{
    FILE * fp;
    ub8 offset;

    if ((fp = fopen(sqlfile, "r")) != NULL) {
        int len;
        char line[256];

        RDBBlob_t sqlblob;

        int nmaps = 0;
        int mapsz = 256;
        RDBResultMap *resultMaps = (RDBResultMap *) RDBMemAlloc(sizeof(RDBResultMap) * mapsz);

        sqlblob.maxsz = 4096;
        sqlblob.length = 0;
        sqlblob.str = RDBMemAlloc(sqlblob.maxsz);

        while ((len = cstr_readline (fp, line, 255)) != -1) {
            if (len > 0 && line[0] != '#') {
                char *endp = strrchr(line, ';');
                if (endp) {
                    *endp = 0;
                    len = (int) (endp - line);
                }

                memcpy(sqlblob.str + sqlblob.length, line, len);
                sqlblob.length += len;

                if (sqlblob.maxsz - sqlblob.length < sizeof(line)) {
                    sqlblob.str = RDBMemRealloc(sqlblob.str, sqlblob.maxsz, sqlblob.maxsz + sizeof(line) * 4);
                    sqlblob.maxsz += sizeof(line) * 4;
                }

                if (endp) {
                    RDBResultMap resultMap = NULL;
                    offset = RDBSQLExecuteSQL(ctx, &sqlblob, &resultMap);
                    resultMaps[nmaps++] = resultMap;

                    if (nmaps == mapsz) {
                        resultMaps = (RDBResultMap *) RDBMemRealloc(resultMaps, sizeof(RDBResultMap) * mapsz, sizeof(RDBResultMap) * (mapsz + 256));
                        mapsz += 256;
                    }

                    sqlblob.length = 0;
                    bzero(sqlblob.str, sqlblob.maxsz);
                }
            }
        }

        if (sqlblob.length > 0) {
            RDBResultMap resultMap = NULL;
            RDBSQLExecuteSQL(ctx, &sqlblob, &resultMap);
            resultMaps[nmaps++] = resultMap;
        }

        RDBMemFree(sqlblob.str);
        fclose(fp);

        *outResultMaps = resultMaps;

        return nmaps;
    }

    snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(%s:%d) failed open file: %s", __FILE__, __LINE__, sqlfile);
    return RDBAPI_ERROR;
}
