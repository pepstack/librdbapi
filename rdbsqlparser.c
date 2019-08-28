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
#include "common/re.h"


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
            char *fields[RDBAPI_ARGV_MAXNUM + 1];
            RDBFilterExpr fieldexprs[RDBAPI_ARGV_MAXNUM + 1];
            char *fieldvals[RDBAPI_ARGV_MAXNUM + 1];

            // keys filter
            int numkeys;
            char *keys[RDBAPI_ARGV_MAXNUM + 1];
            RDBFilterExpr keyexprs[RDBAPI_ARGV_MAXNUM + 1];
            char *keyvals[RDBAPI_ARGV_MAXNUM + 1];

            // result fields
            int count;
            char *resultfields[RDBAPI_ARGV_MAXNUM + 1];

            ub8 offset;
            ub4 limit;
        } select;

        struct UPSERT_INTO {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];

            int numfields;
            char *fieldnames[RDBAPI_ARGV_MAXNUM + 1];
            char *fieldvalues[RDBAPI_ARGV_MAXNUM + 1];

            // 0 for IGNORE, > 0 for UPDATE
            int numupdates;
            char *updatenames[RDBAPI_ARGV_MAXNUM + 1];
            char *updatevalues[RDBAPI_ARGV_MAXNUM + 1];
        } upsert;

        struct CREATE_TABLE {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];

            int numfields;
            RDBFieldDes_t fielddefs[RDBAPI_ARGV_MAXNUM + 1];

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


void onSqlParserSelect (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause);
void onSqlParserDelete (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause);
void onSqlParserUpsert (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause);
void onSqlParserCreate (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause);
void onSqlParserDesc   (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause);
void onSqlParserInfo   (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause);
void onSqlParserShowDatabases (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause);
void onSqlParserShowTables    (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause);
void onSqlParserDrop   (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause);


RDBAPI_RESULT RDBSQLParserNew (RDBCtx ctx, const char *sql, size_t sqlen, RDBSQLParser *outParser)
{
    RDBSQLParser parser = NULL;

    int len = 0;
    int offs = 0;

    *ctx->errmsg = 0;

    // check sql length
    //
    if (sqlen == (size_t)(-1)) {
        len = (int) strnlen(sql, RDB_SQL_TOTAL_LEN_MAX + 1);
    } else {
        len = (int) sqlen;
    }

    if (len < 4) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: sql is too short");
        return RDBAPI_ERR_RDBSQL;
    }
    if (len > RDB_SQL_TOTAL_LEN_MAX) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: sql is too long");
        return RDBAPI_ERR_RDBSQL;
    }

    sqlen = len;

    // copy a buf for sql, and trim whilespaces
    //
    do {
        char * sqlbuf = RDBMemAlloc(sqlen + 1);
        memcpy(sqlbuf, sql, sqlen);

        char *sqlfix = cstr_Ltrim_whitespace(sqlbuf);
        sqlen -= (int)(sqlfix - sqlbuf);

        sqlen = cstr_Rtrim_whitespace(sqlfix, (int) sqlen);

        sqlfix = cstr_Rtrim_chr(sqlfix, ';');
        sqlen = cstr_Rtrim_whitespace(sqlfix, (int) strlen(sqlfix));

        offs = (int) (strstr(sql, sqlfix) - sql);

        parser = (RDBSQLParser) RDBMemAlloc(sizeof(RDBSQLParser_t) + sqlen + 1);
        parser->sqlen = (int) sqlen;
        memcpy(parser->sql, sqlfix, parser->sqlen);

        RDBMemFree(sqlbuf);
    } while(0);

    // use pattern to match sql
    //
    len = re_match(RDBSQL_PATTERN_SELECT_FROM, parser->sql);
    if (len != -1) {
        onSqlParserSelect(parser, ctx, len, offs, sql);
        goto parser_finish;
    }

    len = re_match(RDBSQL_PATTERN_DELETE_FROM, parser->sql);
    if (len != -1) {
        onSqlParserDelete(parser, ctx, len, offs, sql);
        goto parser_finish;
    }

    len = re_match(RDBSQL_PATTERN_UPSERT_INTO, parser->sql);
    if (len != -1) {
        onSqlParserUpsert(parser, ctx, len, offs, sql);
        goto parser_finish;
    }

    len = re_match(RDBSQL_PATTERN_CREATE_TABLE, parser->sql);
    if (len != -1) {
        onSqlParserCreate(parser, ctx, len, offs, sql);
        goto parser_finish;
    }

    len = re_match(RDBSQL_PATTERN_DESC_TABLE, parser->sql);
    if (len != -1) {
        onSqlParserDesc(parser, ctx, len, offs, sql);
        goto parser_finish;
    }

    len = re_match(RDBSQL_PATTERN_INFO_SECTION, parser->sql);
    if (len != -1) {
        onSqlParserInfo(parser, ctx, len, offs, sql);
        goto parser_finish;
    }

    len = re_match(RDBSQL_PATTERN_SHOW_DATABASES, parser->sql);
    if (len != -1) {
        onSqlParserShowDatabases(parser, ctx, len, offs, sql);
        goto parser_finish;
    }

    len = re_match(RDBSQL_PATTERN_SHOW_TABLES, parser->sql);
    if (len != -1) {
        onSqlParserShowTables(parser, ctx, len, offs, sql);
        goto parser_finish;
    }

    len = re_match(RDBSQL_PATTERN_DROP_TABLE, parser->sql);
    if (len != -1) {
        onSqlParserDrop(parser, ctx, len, offs, sql);
        goto parser_finish;
    }

    snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: parse failed: '%s'", sql);

parser_finish:
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
            cstr_varray_free(parser->select.resultfields, RDBAPI_ARGV_MAXNUM);
            cstr_varray_free(parser->select.keys, RDBAPI_ARGV_MAXNUM);
            cstr_varray_free(parser->select.keyvals, RDBAPI_ARGV_MAXNUM);
            cstr_varray_free(parser->select.fields, RDBAPI_ARGV_MAXNUM);
            cstr_varray_free(parser->select.fieldvals, RDBAPI_ARGV_MAXNUM);
        } else if (parser->stmt == RDBSQL_UPSERT) {
            cstr_varray_free(parser->upsert.fieldnames, RDBAPI_ARGV_MAXNUM);
            cstr_varray_free(parser->upsert.fieldvalues, RDBAPI_ARGV_MAXNUM);
            cstr_varray_free(parser->upsert.updatenames, RDBAPI_ARGV_MAXNUM);
            cstr_varray_free(parser->upsert.updatevalues, RDBAPI_ARGV_MAXNUM);
        } else if (parser->stmt == RDBSQL_CREATE) {
            // TODO:
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


// CREATE TABLE IF NOT EXISTS xsdb.connect (
//    sid UB4 NOT NULL COMMENT 'server id',
//    connfd UB4 NOT NULL COMMENT 'connect socket fd',
//    sessionid UB8X COMMENT 'session id',
//    port UB4 COMMENT 'client port', host STR(30) COMMENT 'client host ip',
//    hwaddr STR(30) COMMENT 'mac addr',
//    agent STR(30) COMMENT 'client agent',
//    ROWKEY (sid , connfd)
// ) COMMENT 'log file entry';
//
// CREATE TABLE IF NOT EXISTS mine.test ( sid UB4 NOT NULL, connfd UB4 NOT NULL COMMENT 'socket fd', host STR( 30 ), port UB4, port2 UB4 NOT NULL, maddr STR COMMENT 'hardware address', price FLT64 (14,4)  COMMENT 'property (ver1.2, build=3) ok, price', ROWKEY(sid , connfd) ) COMMENT 'file entry';
//
static char * parse_create_field (char *sqladdr, int sqloffs,
    char *sql, int sqlen, const char **vtnames,
    RDBFieldDes_t *fieldes, char **rowkeys,
    char *tblcomment, size_t tblcommsz,
    char *buf, size_t bufsz)
{
    int j, np, len, errat;
    char *endp, *nextp;
    char *sqlc = sql;

    bzero(fieldes, sizeof(*fieldes));
    fieldes->nullable = 1;

    // name TYPE
    np = re_match("[\\s]*[\\w]+[\\s]+[A-Z]+[\\d]*", sqlc);
    if (np == -1) {
        np = re_match("[\\s]*ROWKEY[\\s]*(", sqlc);
        if (np != -1) {
            sqlc = cstr_Ltrim_whitespace(cstr_Ltrim_whitespace(sqlc) + 6);
            endp = strchr(sqlc, 41);
            if (!endp) {
                errat = (int)(sqlc - sqladdr) + sqloffs;
                snprintf_chkd_V1(buf, bufsz, "SQLError: ROWKEY not enclosed - error char at(%d): '%s'", errat, sqladdr + errat);
                return NULL;
            }

            *endp++ = 0;
            sqlc++;

            len = cstr_length(sqlc, endp - sqlc);

            len = cstr_slpit_chr(sqlc, len, 44, rowkeys, RDBAPI_SQL_KEYS_MAX + 1);
            if (len > RDBAPI_SQL_KEYS_MAX) {
                errat = (int)(sqlc - sqladdr) + sqloffs;
                snprintf_chkd_V1(buf, bufsz, "SQLError: too many keys in ROWKEY - error char at(%d): '%s'", errat, sqladdr + errat);
                return NULL;
            }

            sqlc = endp;
            endp = strchr(sqlc, 41);
            if (! endp) {
                errat = (int)(sqlc - sqladdr) + sqloffs;
                snprintf_chkd_V1(buf, bufsz, "SQLError: CREATE not enclosed - error char at(%d): '%s'", errat, sqladdr + errat);
                return NULL;
            }

            *endp++ = 0;
            sqlc = endp;

            // table comment
            np = re_match("[\\s]*COMMENT[\\s]+'", sqlc);
            if (np != -1) {
                endp = strstr(sqlc, "COMMENT");
                endp[7] = 0;
                sqlc = endp + 8;

                endp = strrchr(sqlc, 39);
                sqlc = strchr(sqlc, 39);
                if (! endp || ! sqlc || sqlc == endp) {
                    errat = (int)(sqlc - sqladdr) + sqloffs;
                    snprintf_chkd_V1(buf, bufsz, "SQLError: invalid COMMENT - error char at(%d): '%s'", errat, sqladdr + errat);
                    return NULL;
                }

                *endp = 0;
                *sqlc++ = 0;

                snprintf_chkd_V1(tblcomment, tblcommsz, "%.*s", (int)(endp - sqlc), sqlc);
            }

            // finish ok
            *buf = 0;
            return NULL;
        }

        errat = (int)(sqlc - sqladdr) + sqloffs;
        snprintf_chkd_V1(buf, bufsz, "SQLError: field name not found - error char at(%d): '%s'", errat, sqladdr + errat);
        return NULL;
    }

    sqlc = cstr_Ltrim_whitespace(sqlc);

    np = re_match("[\\s]+[A-Z]+[\\d]*", sqlc);
    sqlc[np] = 0;

    fieldes->namelen = snprintf_chkd_V1(fieldes->fieldname, sizeof(fieldes->fieldname), "%.*s", np, sqlc);

    sqlc = &sqlc[np + 1];
    sqlc = cstr_Ltrim_whitespace(sqlc);

    // TYPE
    np = re_match("[\\W]+", sqlc);
    if (np == -1) {
        errat = (int)(sqlc - sqladdr) + sqloffs;
        snprintf_chkd_V1(buf, bufsz, "SQLError: field type not found - error char at(%d): '%s'", errat, sqladdr + errat);
        return NULL;
    }

    snprintf_chkd_V1(buf, bufsz, "%.*s", np, sqlc);

    for (j = 0; j < 256; j++) {
        if (vtnames[j] && ! strcmp(buf, vtnames[j])) {
            break;
        }
    }
    if (j == 256) {
        errat = (int)(sqlc - sqladdr) + sqloffs;
        snprintf_chkd_V1(buf, bufsz, "SQLError: bad field type - error char at(%d): '%s'", errat, sqladdr + errat);
        return NULL;                    
    }
    fieldes->fieldtype = (RDBValueType) j;

    // (length<,scale>)
    sqlc = cstr_Ltrim_whitespace(sqlc + np);
    if (*sqlc == 44) {
        // next field
        return sqlc + 1;
    }

    if (*sqlc == 40) {
        nextp = strchr(sqlc, 41);
        if (!nextp) {
            errat = (int)(sqlc - sqladdr) + sqloffs;
            snprintf_chkd_V1(buf, bufsz, "SQLError: not enclosed - error char at(%d): '%s'", errat, sqladdr + errat);
            return NULL;    
        }

        *nextp++ = 0;

        np = re_match("([\\s]*[\\d]+[\\s]*,[\\s]*[\\d]+[\\s]*", sqlc);
        if (np == 0) {
            // (length, scale)
            sqlc = cstr_Ltrim_whitespace(sqlc + 1);
            endp = strchr(sqlc, 44);
            *endp++ = 0;

            len = cstr_Rtrim_whitespace(sqlc, (int) strlen(sqlc));
            snprintf_chkd_V1(buf, bufsz, "%.*s", len, sqlc);
            fieldes->length = atoi(buf);

            len = cstr_Rtrim_whitespace(endp, (int) strlen(endp));
            snprintf_chkd_V1(buf, bufsz, "%.*s", len, endp);
            fieldes->dscale = atoi(buf);
        } else {
            np = re_match("([\\s]*[\\d]+[\\s]*", sqlc);
            if (np != 0) {
                errat = (int)(sqlc - sqladdr) + sqloffs;
                snprintf_chkd_V1(buf, bufsz, "SQLError: invalid type length - error char at(%d): '%s'", errat, sqladdr + errat);
                return NULL;
            }

            len = cstr_Rtrim_whitespace(sqlc, (int) strlen(sqlc));
            snprintf_chkd_V1(buf, bufsz, "%.*s", len, sqlc);
            fieldes->length = atoi(buf);
        }

        sqlc = cstr_Ltrim_whitespace(nextp);
    }

    np = re_match("NOT[\\s]+NULL[\\W]+", sqlc);
    if (np == 0) {
        fieldes->nullable = 0;
        sqlc = strstr(sqlc, "NULL") + 4;
        sqlc = cstr_Ltrim_whitespace(sqlc);
    }

    np = re_match("COMMENT[\\s]+'", sqlc);
    if (np == 0) {
        sqlc = &sqlc[np + 8];
        sqlc = cstr_Ltrim_whitespace(sqlc) + 1;

        endp = strchr(sqlc, 39);
        if (! endp) {
            errat = (int)(sqlc - sqladdr) + sqloffs;
            snprintf_chkd_V1(buf, bufsz, "SQLError: invalid COMMENT - error char at(%d): '%s'", errat, sqladdr + errat);
            return NULL;
        }

        *endp++ = 0;

        snprintf_chkd_V1(fieldes->comment, sizeof(fieldes->comment), "%.*s", (int)(endp - sqlc - 1), sqlc);

        sqlc = endp;  
    }

    sqlc = cstr_Ltrim_whitespace(sqlc);
    if (*sqlc == 44) {
        // next field
        return sqlc+1;
    }

    // finish field
    errat = (int)(sqlc - sqladdr) + sqloffs;
    snprintf_chkd_V1(buf, bufsz, "SQLError: ROWKEY not found - error char at(%d): '%s'", errat, sqladdr + errat);
    return NULL;
}


// ( a,b,'hello,shanghai' )
//
static char * parse_upsert_value (char *sqladdr, int sqloffs, char *sql, char **value, char *buf, size_t bufsz)
{
    int vlen = 0;
    char *endp = NULL;
    char *valp = cstr_Ltrim_whitespace(sql);

    int quot = 0;

    *value = NULL;

    if (*valp == 39) {
        valp++;

        endp = strchr(valp, 39);
        if (!endp) {
            // error
            return NULL;
        }

        vlen = (int)(endp - valp);
        *endp++ = 0;

        endp = cstr_Ltrim_whitespace(endp);
        if (*endp == 44) {
            *endp++ = 0;
        }

        quot = 1;
    } else {
        endp = strchr(valp, 44);
        if (endp) {
            *endp++ = 0;
        }

        vlen = cstr_Rtrim_whitespace(valp, (int) strlen(valp));
    }

    *value = RDBMemAlloc(vlen + 1 + quot + quot);

    if (quot) {
        snprintf_chkd_V1(*value, vlen + 1 + quot + quot, "'%.*s'", vlen, valp);
    } else {
        snprintf_chkd_V1(*value, vlen + 1 + quot + quot, "%.*s", vlen, valp);
    }

    return endp;
}


// UPDATE a=b, c=c+1, e='hello= , world';
//
static char * parse_upsert_field (char *sqladdr, int sqloffs,
    char *sql, char **updname, char **updvalue,
    char *buf, size_t bufsz)
{
    int np, klen, vlen;
    char *endp;

    char *key, *val;

    int quot = 0;

    *updname = NULL;
    *updvalue = NULL;

    // fldname='value',
    np = re_match("[\\W]*[\\w]+[\\s]*=[\\s]*", sql);
    if (np == -1) {
        return NULL;
    }

    val = strchr(sql, '=');
    *val++ = 0;

    key = cstr_Ltrim_whitespace(sql);
    klen = cstr_Rtrim_whitespace(key, cstr_length(key, (int)(val - key)));

    val = cstr_Ltrim_whitespace(val);
    if (*val == 39) {
        val++;
        endp = strchr(val, 39);
        if (!endp) {
            // error
            return NULL;
        }

        vlen = (int)(endp - val);
        *endp++ = 0;

        endp = cstr_Ltrim_whitespace(endp);
        if (*endp == 44) {
            *endp++ = 0;
        }

        quot = 1;
    } else {
        endp = strchr(val, 44);
        if (endp) {
            *endp++ = 0;
        }
        vlen = cstr_Rtrim_whitespace(val, (int) strlen(val));
    }

    do {
        char *kbuf = RDBMemAlloc(klen + 1);
        char *vbuf = RDBMemAlloc(vlen + 1 + quot *2);

        memcpy(kbuf, key, klen);

        if (quot) {
            snprintf_chkd_V1(vbuf, vlen + 1 + quot *2, "'%.*s'", vlen, val);
        } else {
            snprintf_chkd_V1(vbuf, vlen + 1 + quot *2, "%.*s", vlen, val);
        }

        *updname = kbuf;
        *updvalue = vbuf;
    } while(0);

    return endp;
}


// SELECT $fields FROM $database.$tablename WHERE $condition OFFSET $position LIMIT $count
// SELECT * FROM $database.$tablename <WHERE $condition> <OFFSET $position> <LIMIT $count>
//
void onSqlParserSelect (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause)
{
    char *sqlc = parser->sql + start + 7;
    char *psz;
    int len;

    int nwhere, noffset, nlimit;
    char * pwhere, *poffset, *plimit;

    sqlc = cstr_Ltrim_whitespace(sqlc);

    psz = strstr(sqlc, "FROM");
    if (! psz) {
        int errat = (int)(sqlc - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: FROM not found - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    *psz = 0;
    len = cstr_Rtrim_whitespace(sqlc, (int)(psz - sqlc));

    parser->select.count = parse_fields(sqlc, parser->select.resultfields);
    if (! parser->select.count) {
        int errat = (int)(sqlc - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: parse fields failed - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    if (! isspace(psz[4])) {
        int errat = (int)(psz - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: FROM not found - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    // find WHERE ...
    sqlc = cstr_Ltrim_whitespace(psz + 4);

    nwhere = re_match("[\\s]+WHERE[\\s]+", sqlc);
    noffset = re_match("[\\s]+OFFSET[\\s]+", sqlc);
    nlimit = re_match("[\\s]+LIMIT[\\s]+", sqlc);

    pwhere = poffset = plimit = NULL;

    if (nwhere != -1) {
        sqlc[nwhere] = 0;
        pwhere = strstr(sqlc + nwhere + 1, "WHERE") + 5;
        pwhere = cstr_Ltrim_whitespace(pwhere);
        nwhere = cstr_length(pwhere, parser->sqlen);
    }
    if (noffset != -1) {
        sqlc[noffset] = 0;
        poffset = strstr(sqlc + noffset + 1, "OFFSET") + 6;
        poffset = cstr_Ltrim_whitespace(poffset);
        noffset = cstr_length(poffset, parser->sqlen);
    }
    if (nlimit != -1) {
        sqlc[nlimit] = 0;
        plimit = strstr(sqlc + nlimit + 1, "LIMIT") + 5;
        plimit = cstr_Ltrim_whitespace(plimit);
        nlimit = cstr_length(plimit, parser->sqlen);
    }

    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, parser->sqlen));
    if (! parse_table(sqlc, parser->select.tablespace, parser->select.tablename)) {
        int errat = (int)(sqlc - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: bad table name - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    if (pwhere) {
        nwhere = cstr_Rtrim_whitespace(pwhere, nwhere);

        parser->select.numfields = parse_where(pwhere, parser->select.fields, parser->select.fieldexprs, parser->select.fieldvals);
        if (parser->select.numfields < 0) {
            int errat = (int)(pwhere - parser->sql) + sqloffs;
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: parse WHERE failed - error char at(%d): '%s'", errat, sqlclause + errat);
            return;
        }
    }

    if (poffset) {
        if (cstr_to_ub8(10, poffset, noffset, &parser->select.offset) < 0) {
            int errat = (int)(poffset - parser->sql) + sqloffs;
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: parse OFFSET failed - error char at(%d): '%s'", errat, sqlclause + errat);
            return;
        }
    }

    if (plimit) {
        ub8 limit = 10;

        if (cstr_to_ub8(10, plimit, nlimit, &limit) < 0 || limit == (-1) || limit > RDB_TABLE_LIMIT_MAX) {
            int errat = (int)(plimit - parser->sql) + sqloffs;
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: parse LIMIT failed - error char at(%d): '%s'", errat, sqlclause + errat);
            return;
        }

        parser->select.limit = (ub4) limit;
    }

    parser->stmt = RDBSQL_SELECT;
}


// DELETE FROM $database.$tablename <WHERE $condition> <OFFSET $position> <LIMIT $count>
//
void onSqlParserDelete (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause)
{
    char *sqlc = parser->sql + start + 7;
    char *psz;
    int len;

    int nwhere, noffset, nlimit;
    char * pwhere, *poffset, *plimit;

    sqlc = cstr_Ltrim_whitespace(sqlc);

    psz = strstr(sqlc, "FROM");
    if (! psz) {
        int errat = (int)(sqlc - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: FROM not found - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    *psz = 0;
    len = cstr_Rtrim_whitespace(sqlc, (int)(psz - sqlc));
    if (! isspace(psz[4])) {
        int errat = (int)(psz - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: FROM not found - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    // find WHERE ...
    sqlc = cstr_Ltrim_whitespace(psz + 4);

    nwhere = re_match("[\\s]+WHERE[\\s]+", sqlc);
    noffset = re_match("[\\s]+OFFSET[\\s]+", sqlc);
    nlimit = re_match("[\\s]+LIMIT[\\s]+", sqlc);

    pwhere = poffset = plimit = NULL;

    if (nwhere != -1) {
        sqlc[nwhere] = 0;
        pwhere = strstr(sqlc + nwhere + 1, "WHERE") + 5;
        pwhere = cstr_Ltrim_whitespace(pwhere);
        nwhere = cstr_length(pwhere, parser->sqlen);
    }
    if (noffset != -1) {
        sqlc[noffset] = 0;
        poffset = strstr(sqlc + noffset + 1, "OFFSET") + 6;
        poffset = cstr_Ltrim_whitespace(poffset);
        noffset = cstr_length(poffset, parser->sqlen);
    }
    if (nlimit != -1) {
        sqlc[nlimit] = 0;
        plimit = strstr(sqlc + nlimit + 1, "LIMIT") + 5;
        plimit = cstr_Ltrim_whitespace(plimit);
        nlimit = cstr_length(plimit, parser->sqlen);
    }

    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, parser->sqlen));
    if (! parse_table(sqlc, parser->select.tablespace, parser->select.tablename)) {
        int errat = (int)(sqlc - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: bad table name - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    if (pwhere) {
        nwhere = cstr_Rtrim_whitespace(pwhere, nwhere);

        parser->select.numfields = parse_where(pwhere, parser->select.fields, parser->select.fieldexprs, parser->select.fieldvals);
        if (parser->select.numfields < 0) {
            int errat = (int)(pwhere - parser->sql) + sqloffs;
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: parse WHERE failed - error char at(%d): '%s'", errat, sqlclause + errat);
            return;
        }
    }

    if (poffset) {
        if (cstr_to_ub8(10, poffset, noffset, &parser->select.offset) < 0) {
            int errat = (int)(poffset - parser->sql) + sqloffs;
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: parse OFFSET failed - error char at(%d): '%s'", errat, sqlclause + errat);
            return;
        }
    }

    if (plimit) {
        ub8 limit = 10;

        if (cstr_to_ub8(10, plimit, nlimit, &limit) < 0 || limit == (-1) || limit > RDB_TABLE_LIMIT_MAX) {
            int errat = (int)(plimit - parser->sql) + sqloffs;
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: parse LIMIT failed - error char at(%d): '%s'", errat, sqlclause + errat);
            return;
        }

        parser->select.limit = (ub4) limit;
    }

    parser->stmt = RDBSQL_DELETE;
}


// UPSERT INTO xsdb.test(name,id) VALUES('foo', 123);
// UPSERT INTO xsdb.test(id, counter) VALUES(123, 0) ON DUPLICATE KEY UPDATE id=1,counter = counter + 1;
// UPSERT INTO xsdb.test(id, my_col) VALUES(123, 0) ON DUPLICATE KEY IGNORE;
//
// UPSERT INTO mine.test(sid, connfd, host) VALUES(1,1,'shanghai') ON DUPLICATE KEY IGNORE;
//
void onSqlParserUpsert (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause)
{
    char *sqlc;
    int np, len;

    char *startp, *endp;

    int numfields = 0;
    int numvalues = 0;
    int update = 0;

    sqlc = cstr_Ltrim_whitespace(parser->sql + start + 7);

    // table name
    sqlc = cstr_Ltrim_whitespace(sqlc + 5);
    startp = strchr(sqlc, 40);
    if (! startp) {
        return;
    }
    *startp++ = 0;

    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, parser->sqlen));
    if (! parse_table(sqlc, parser->select.tablespace, parser->select.tablename)) {
        int errat = (int)(sqlc - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: bad table name - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    // fieldnames
    sqlc = cstr_Ltrim_whitespace(startp);
    endp = strchr(sqlc, 41);
    if (! endp) {
        return;
    }
    *endp++ = 0;

    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, parser->sqlen));
    numfields = cstr_slpit_chr(sqlc, len, 44, parser->upsert.fieldnames, RDBAPI_ARGV_MAXNUM + 1);
    if (numfields == 0) {
        return;
    }
    if (numfields > RDBAPI_ARGV_MAXNUM) {
        return;
    }

    // VALUES
    sqlc = cstr_Ltrim_whitespace(endp);
    np = re_match("VALUES[\\s]*(", sqlc);
    if (np != 0) {
        return;
    }

    sqlc = cstr_Ltrim_whitespace(sqlc + 6);

    np = re_match(")[\\s]*ON[\\s]+DUPLICATE[\\s]+KEY[\\s]+", sqlc);
    if (np == -1) {
        endp = strrchr(sqlc, 41);
    } else {
        endp = &sqlc[np];
    }

    if (!endp) {
        return;
    }
    *endp++ = 0;

    sqlc = cstr_Ltrim_whitespace(sqlc + 1);
    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, parser->sqlen));

    // ( a,b,'hello,shanghai' )
    while (sqlc && numvalues < numfields) {
        char *value = NULL;
        sqlc = parse_upsert_value(parser->sql, sqloffs, sqlc, &value, ctx->errmsg, sizeof(ctx->errmsg));
        if (value) {
            parser->upsert.fieldvalues[numvalues++] = value;
        }
    }

    if (numvalues != numfields) {
        return;
    }

    sqlc = endp;
    if (np != -1) {
        np = re_match("ON[\\s]+DUPLICATE[\\s]+KEY[\\s]+UPDATE[\\s]+", sqlc);

        if (np == -1) {
            np = re_match("ON[\\s]+DUPLICATE[\\s]+KEY[\\s]+IGNORE[\\W]*", sqlc);
            if (np == -1) {
                return;
            }
        } else {
            startp = strstr(sqlc, "UPDATE") + 6;
            *startp++ = 0;
            sqlc = cstr_Ltrim_whitespace(startp);
            update = 1;
        }
    }

    parser->upsert.numfields = numfields;

    if (! update) {
        // IGNORE
        parser->stmt = RDBSQL_UPSERT;
        return;
    }

    // UPDATE a=b, c=c+1, e='hello= , world';
    while (sqlc && parser->upsert.numupdates < numfields) {
        char *upname = 0;
        char *upvalue = 0;

        sqlc = parse_upsert_field(parser->sql, sqloffs, sqlc, &upname, &upvalue, ctx->errmsg, sizeof(ctx->errmsg));
        if (upname && upvalue) {
            parser->upsert.updatenames[parser->upsert.numupdates] = upname;
            parser->upsert.updatevalues[parser->upsert.numupdates] = upvalue;
            parser->upsert.numupdates++;
        }
    }

    if (! parser->upsert.numupdates) {
        return;
    }

    // success
    parser->stmt = RDBSQL_UPSERT;
}


void onSqlParserCreate (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause)
{
    int i;

    char *sqlc;
    int len;

    char *sqlnext;
    int nextlen;

    char *rowkeys[RDBAPI_SQL_KEYS_MAX + 1] = {0};

    sqlc = cstr_Ltrim_whitespace(parser->sql + start + 7);
    sqlc = cstr_Ltrim_whitespace(sqlc + 6);

    len = re_match("IF[\\s]+NOT[\\s]+EXISTS[\\s]+", sqlc);
    if (len == 0) {
        parser->create.fail_on_exists = 0;
    } else {
        parser->create.fail_on_exists = 1;
    }

    len = cstr_length(sqlc, parser->sqlen);

    sqlnext = cstr_Lfind_chr(sqlc, len, '(');
    if (! sqlnext) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: bad CREATE clause: '%s'", sqlclause);
        return;
    }

    if (! parser->create.fail_on_exists) {
        sqlc = strstr(sqlc, "EXISTS");
        sqlc = cstr_Ltrim_whitespace(sqlc + 6);
    }

    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, (int)(sqlnext - sqlc)));
    if (! parse_table(sqlc, parser->create.tablespace, parser->create.tablename)) {
        int errat = (int)(sqlc - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: parse table failed - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    // parse all fields
    sqlnext++;
    nextlen = cstr_length(sqlnext, parser->sqlen);
    do {
        sqlnext = parse_create_field(parser->sql, sqloffs,
            sqlnext, nextlen, (const char **) ctx->env->valtypenames,
            &parser->create.fielddefs[parser->create.numfields++], rowkeys,
            parser->create.tablecomment, sizeof(parser->create.tablecomment),
            ctx->errmsg, sizeof(ctx->errmsg));

        if (! sqlnext && *ctx->errmsg) {
            // failed on error
            cstr_varray_free(rowkeys, RDBAPI_SQL_KEYS_MAX);
            return;
        }
    } while(sqlnext && parser->create.numfields <= RDBAPI_ARGV_MAXNUM);

    if (! *rowkeys) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: no keys for ROWKEY");
        return;
    }

    parser->create.numfields--;

    if (parser->create.numfields < 1) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: fields not found");
        cstr_varray_free(rowkeys, RDBAPI_SQL_KEYS_MAX);
        return;
    }
    if (parser->create.numfields > RDBAPI_ARGV_MAXNUM) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: too many fields");
        cstr_varray_free(rowkeys, RDBAPI_SQL_KEYS_MAX);
        return;
    }


    for (i = 0; i < parser->create.numfields; i++) {
        parser->create.fielddefs[i].rowkey = 1 +
            cstr_findstr_in(parser->create.fielddefs[i].fieldname, parser->create.fielddefs[i].namelen, (const char **) rowkeys, -1);
    }

    cstr_varray_free(rowkeys, RDBAPI_SQL_KEYS_MAX);

    // success
    parser->stmt = RDBSQL_CREATE;
}


// DESC $database.$table
//
void onSqlParserDesc (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause)
{
    char table[RDB_KEY_NAME_MAXLEN * 2 + 4] = {0};
    char *sqlc = cstr_Ltrim_whitespace(parser->sql + start + 5);

    snprintf_chkd_V1(table, sizeof(table), "%.*s", cstr_length(sqlc, sizeof(table)), sqlc);
    if (! parse_table(table, parser->desctable.tablespace, parser->desctable.tablename)) {
        int errat = (int)(sqlc - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: table not found - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    parser->stmt = RDBSQL_DESC_TABLE;
}


// INFO <section> <nodeid>
// INFO <nodeid>
//
void onSqlParserInfo (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause)
{
    char *sqlc = cstr_Ltrim_whitespace(parser->sql + start + 4);
    int len = cstr_length(sqlc, parser->sqlen);

    // 1-based. 0 for all.
    int nodeid = 0;

    if (len == 0) {
        // INFO;
        parser->info.section = MAX_NODEINFO_SECTIONS;
        parser->info.whichnode = nodeid;

        parser->stmt = RDBSQL_INFO_SECTION;
    } else {
        int i = 0;

        // MUST be same Order with RDBNodeInfoSection
        const char *sections[] = {
            "SERVER[\\W]*",        // 0
            "CLIENTS[\\W]*",       // 1
            "MEMORY[\\W]*",        // 2
            "PERSISTENCE[\\W]*",   // 3
            "STATS[\\W]*",         // 4
            "REPLICATION[\\W]*",   // 5
            "CPU[\\W]*",           // 6
            "CLUSTER[\\W]*",       // 7
            "KEYSPACE[\\W]*",      // 8
            0
        };

        const char *secpattern;

        char *sqlc0 = sqlc;

        cstr_toupper(sqlc, len);

        while ((secpattern = sections[i]) != NULL) {
            if (re_match(secpattern, sqlc) != -1) {
                parser->info.section = (RDBNodeInfoSection) i;

                sqlc += strchr(secpattern, '[') - secpattern;
                sqlc = cstr_Ltrim_whitespace(sqlc);
                break;
            }

            i++;
        }

        // find nodeid
        if (cstr_length(sqlc, 10) > 0 && re_match("^[\\d]+", sqlc) == -1) {
            int errat = (int)(sqlc - parser->sql) + sqloffs;
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: bad section or nodeid - error char at(%d): '%s'", errat, sqlclause + errat);
            return;
        }

        nodeid = atoi(sqlc);

        if (nodeid < 0 || nodeid > RDBEnvNumNodes(ctx->env)) {
            int errat = (int)(sqlc - parser->sql) + sqloffs;
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: bad nodeid - error char at(%d): '%s'", errat, sqlclause + errat);
            return;
        }

        if (! secpattern) {
            if (sqlc0 == sqlc) {
                parser->info.section = MAX_NODEINFO_SECTIONS;
            } else {
                int errat = (int)(sqlc0 - parser->sql) + sqloffs;
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: bad section - error char at(%d): '%s'", errat, sqlclause + errat);
                return;
            }
        }

        parser->info.whichnode = nodeid;
        parser->stmt = RDBSQL_INFO_SECTION;
    }
}


// SHOW DATABASES;
//
void onSqlParserShowDatabases (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause)
{
    parser->stmt = RDBSQL_SHOW_DATABASES;
}


// SHOW TABLES $database
//
void onSqlParserShowTables (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause)
{
    int len;
    char *sqlc = parser->sql + start + 5;

    start = re_match("[\\s]*TABLES[\\s]+", sqlc);

    sqlc = cstr_Ltrim_whitespace(sqlc + start + 6);
    len = cstr_length(sqlc, parser->sqlen);
    if (! len) {
        int errat = (int)(sqlc - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: database not found - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    if (len > RDB_KEY_NAME_MAXLEN) {
        int errat = (int)(sqlc - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: database too long - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    snprintf_chkd_V1(parser->showtables.tablespace, sizeof(parser->showtables.tablespace), "%.*s", len, sqlc);

    parser->stmt = RDBSQL_SHOW_TABLES;
}


// DROP TABLE $database.$tablename
//
void onSqlParserDrop (RDBSQLParser parser, RDBCtx ctx, int start, int sqloffs, const char *sqlclause)
{
    char table[RDB_KEY_NAME_MAXLEN * 2 + 4] = {0};
    char *sqlc = parser->sql + start + 5;

    sqlc = cstr_Ltrim_whitespace(sqlc + start + 6);

    snprintf_chkd_V1(table, sizeof(table), "%.*s", cstr_length(sqlc, sizeof(table)), sqlc);
    if (! parse_table(table, parser->droptable.tablespace, parser->droptable.tablename)) {
        int errat = (int)(sqlc - parser->sql) + sqloffs;
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: table not found - error char at(%d): '%s'", errat, sqlclause + errat);
        return;
    }

    parser->stmt = RDBSQL_DROP_TABLE;
}


void RDBSQLParserPrint (RDBSQLParser sqlParser, FILE *fout)
{
    int j;

    switch (sqlParser->stmt) {
    case RDBSQL_UPSERT:
        fprintf(fout, "  UPSERT INTO %s.%s (\n", sqlParser->upsert.tablespace, sqlParser->upsert.tablename);
        fprintf(fout, "    %s", sqlParser->upsert.fieldnames[0]);
        for (j = 1; j < sqlParser->upsert.numfields; j++) {
            fprintf(fout, ",\n    %s", sqlParser->upsert.fieldnames[j]);
        }
        fprintf(fout, "\n  ) VALUES (\n");
        fprintf(fout, "    %s", sqlParser->upsert.fieldvalues[0]);
        for (j = 1; j < sqlParser->upsert.numfields; j++) {
            fprintf(fout, ",\n    %s", sqlParser->upsert.fieldvalues[j]);
        }
        if (! sqlParser->upsert.numupdates) {
            fprintf(fout, "\n  ) ON DUPLICATE KEY IGNORE");
        } else {
            fprintf(fout, "\n  ) ON DUPLICATE KEY UPDATE\n");
            fprintf(fout, "    %s=%s", sqlParser->upsert.updatenames[0], sqlParser->upsert.updatevalues[0]);
            for (j = 1; j < sqlParser->upsert.numupdates; j++) {
                fprintf(fout, ",\n    %s=%s", sqlParser->upsert.updatenames[j], sqlParser->upsert.updatevalues[j]);
            }
        }
        fprintf(fout, ";\n");
        break;
    }
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
        goto ret_error;
    }

#ifdef _DEBUG
    RDBSQLParserPrint(sqlparser, stdout);
#endif

    offset = RDBSQLExecute(ctx, sqlparser, &resultMap);
    if (offset == RDBAPI_ERROR) {
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
