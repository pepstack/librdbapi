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

    char *datatypes[256];

    char datatypesbuf[256];

    union {
        struct SELECT {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];

            // fields filter
            int numfields;
            char      *fields[RDBAPI_ARGV_MAXNUM];
            RDBFilterExpr fieldexprs[RDBAPI_ARGV_MAXNUM];
            char      *fieldvals[RDBAPI_ARGV_MAXNUM];

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

        struct DELETE_FROM {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];

            // fields filter
            int numfields;
            char      *fields[RDBAPI_ARGV_MAXNUM];
            RDBFilterExpr fieldexprs[RDBAPI_ARGV_MAXNUM];
            char      *fieldvals[RDBAPI_ARGV_MAXNUM];

            // keys filter
            int numkeys;
            char *keys[RDBAPI_ARGV_MAXNUM];
            RDBFilterExpr keyexprs[RDBAPI_ARGV_MAXNUM];
            char *keyvals[RDBAPI_ARGV_MAXNUM];
        } delfrom;

        struct CREATE_TABLE {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];

            int numfields;
            RDBFieldDesc fielddefs[RDBAPI_ARGV_MAXNUM];

            char tablecomment[RDB_KEY_VALUE_SIZE];

            int fail_on_exists;
        } create;

        struct DESC_TABLE {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];
        } desctable;
    };

    int sqlen;
    char sql[0];
} RDBSQLParser_t;

RDBAPI_BOOL onParseSelect (RDBSQLParser_t * parser, RDBCtx ctx, char *selsql, int len);
RDBAPI_BOOL onParseDelete (RDBSQLParser_t * parser, RDBCtx ctx, char *delsql, int len);
RDBAPI_BOOL onParseUpdate (RDBSQLParser_t * parser, RDBCtx ctx, char *updsql, int len);
RDBAPI_BOOL onParseCreate (RDBSQLParser_t * parser, RDBCtx ctx, char *cresql, int len);
RDBAPI_BOOL onParseDesc (RDBSQLParser_t * parser, RDBCtx ctx, char *cresql, int len);


RDBAPI_RESULT RDBSQLParserNew (RDBCtx ctx, const char *sql, size_t sqlen, RDBSQLParser *outParser)
{
    RDBSQLParser parser;
    int parseOk = 0;
    int len = 0;

    if (sqlen == (size_t) -1) {
        len = (int) strnlen(sql, RDB_SQL_TOTAL_LEN_MAX + 1);
    } else {
        len = (int) sqlen;
    }

    if (len < 8) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_RDBSQL: sql is too short");
        RDBCTX_ERRMSG_DONE(ctx);
        return RDBAPI_ERR_RDBSQL;
    }

    if (len > RDB_SQL_TOTAL_LEN_MAX) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_RDBSQL: sql is too long");
        RDBCTX_ERRMSG_DONE(ctx);
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

    } else if (cstr_startwith(parser->sql, parser->sqlen, "UPDATE ", (int)strlen("UPDATE "))) {

        parseOk = onParseUpdate(parser, ctx, parser->sql + strlen("UPDATE "), parser->sqlen - (int)strlen("UPDATE "));
        parser->stmt = parseOk? RDBSQL_UPDATE : RDBSQL_INVALID;

    } else if (cstr_startwith(parser->sql, parser->sqlen, "CREATE TABLE ", (int)strlen("CREATE TABLE "))) {

        parseOk = onParseCreate(parser, ctx, parser->sql + strlen("CREATE TABLE "), parser->sqlen - (int)strlen("CREATE TABLE "));
        parser->stmt = parseOk? RDBSQL_CREATE : RDBSQL_INVALID;

    } else if (cstr_startwith(parser->sql, parser->sqlen, "DESC ", (int)strlen("DESC "))) {

        parseOk = onParseDesc(parser, ctx, parser->sql + strlen("DESC "), parser->sqlen - (int)strlen("DESC "));
        parser->stmt = parseOk? RDBSQL_DESC_TABLE : RDBSQL_INVALID;

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

    len = 0;
    snprintf(parser->datatypesbuf + len, 6, "SB2");
    parser->datatypes[RDBVT_SB2] = &parser->datatypesbuf[len];

    len += 8;
    snprintf(parser->datatypesbuf + len, 6, "UB2");
    parser->datatypes[RDBVT_UB2] = &parser->datatypesbuf[len];

    len += 8;
    snprintf(parser->datatypesbuf + len, 6, "UB4");
    parser->datatypes[RDBVT_UB4] = &parser->datatypesbuf[len];

    len += 8;
    snprintf(parser->datatypesbuf + len, 6, "UB4X");
    parser->datatypes[RDBVT_UB4X] = &parser->datatypesbuf[len];

    len += 8;
    snprintf(parser->datatypesbuf + len, 6, "SB8");
    parser->datatypes[RDBVT_SB8] = &parser->datatypesbuf[len];

    len += 8;
    snprintf(parser->datatypesbuf + len, 6, "UB8");
    parser->datatypes[RDBVT_UB8] = &parser->datatypesbuf[len];

    len += 8;
    snprintf(parser->datatypesbuf + len, 6, "UB8X");
    parser->datatypes[RDBVT_UB8X] = &parser->datatypesbuf[len];

    len += 8;
    snprintf(parser->datatypesbuf + len, 6, "CHAR");
    parser->datatypes[RDBVT_CHAR] = &parser->datatypesbuf[len];

    len += 8;
    snprintf(parser->datatypesbuf + len, 6, "BYTE");
    parser->datatypes[RDBVT_BYTE] = &parser->datatypesbuf[len];

    len += 8;
    snprintf(parser->datatypesbuf + len, 6, "STR");
    parser->datatypes[RDBVT_STR] = &parser->datatypesbuf[len];

    len += 8;
    snprintf(parser->datatypesbuf + len, 6, "FLT64");
    parser->datatypes[RDBVT_FLT64] = &parser->datatypesbuf[len];

    len += 8;
    snprintf(parser->datatypesbuf + len, 6, "BLOB");
    parser->datatypes[RDBVT_BLOB] = &parser->datatypesbuf[len];

    len += 8;
    snprintf(parser->datatypesbuf + len, 6, "DEC");
    parser->datatypes[RDBVT_DEC] = &parser->datatypesbuf[len];

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


ub8 RDBSQLExecute (RDBCtx ctx, RDBSQLParser parser, RDBResultMap *outResultMap)
{
    RDBAPI_RESULT res;

    *outResultMap = NULL;

    if (parser->stmt == RDBSQL_SELECT) {
        RDBResultMap resultMap;

        res = RDBTableScanFirst(ctx, parser->select.tablespace, parser->select.tablename,
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
                parser->select.count,
                parser->select.resultfields,
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
    } else if (parser->stmt == RDBSQL_CREATE) {
        RDBFieldDesc fielddes[RDBAPI_ARGV_MAXNUM] = {0};
        int nfields = RDBTableDesc(ctx, parser->create.tablespace, parser->create.tablename, fielddes);

        if (nfields && parser->create.fail_on_exists) {
            snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERROR: table already existed");
            return (ub8) RDBAPI_ERROR;
        }

        res = RDBTableCreate(ctx, parser->create.tablespace, parser->create.tablename, parser->create.tablecomment, parser->create.numfields, parser->create.fielddefs);

        if (res == RDBAPI_SUCCESS) {
            bzero(fielddes, sizeof(fielddes));
            nfields = RDBTableDesc(ctx, parser->create.tablespace, parser->create.tablename, fielddes);

            if (nfields == parser->create.numfields) {
                RDBResultMap hMap = (RDBResultMap) RDBMemAlloc(sizeof(RDBResultMap_t) + sizeof(RDBFieldDesc) * nfields);

                RDBResultMapSetDelimiter(hMap, RDB_TABLE_DELIMITER_CHAR);

                hMap->kplen = RDBBuildKeyFormat(parser->create.tablespace, parser->create.tablename, fielddes, nfields, hMap->rowkeyid, &hMap->keyprefix);

                memcpy(hMap->fielddes, fielddes, sizeof(fielddes[0]) * nfields);
                hMap->numfields = nfields;

                hMap->ctxh = ctx;

                *outResultMap = hMap;
                return RDBAPI_SUCCESS;
            }
        }
    } else if (parser->stmt == RDBSQL_DESC_TABLE) {
        int nfields = 0;
        RDBFieldDesc fielddefs[RDBAPI_ARGV_MAXNUM + 1] = {0};

        if (strcmp(parser->desctable.tablespace, RDB_SYSTEM_TABLE_PREFIX)) {
            nfields = RDBTableDesc(ctx, parser->desctable.tablespace, parser->desctable.tablename, fielddefs);
            if (nfields) {
                // TODO: fill result map
                int j;

                printf("# Table: %s.%s\n", parser->desctable.tablespace, parser->desctable.tablename);
                printf("#[     fieldname     | fieldtype | length |  scale  | rowkey | nullable | comment ]\n");
                printf("#--------------------+-----------+--------+---------+--------+----------+----------\n");

                for (j = 0; j < nfields; j++) {
                    RDBFieldDesc *fdes = &fielddefs[j];

                    printf(" %-20s| %8s  | %-6d |%8d |  %4d  |   %4d   | %s\n",
                        fdes->fieldname,
                        parser->datatypes[fdes->fieldtype],
                        fdes->length,
                        fdes->dscale,
                        fdes->rowkey,
                        fdes->nullable,
                        fdes->comment);
                }

                printf("#----------------------------------------------------------------------------------\n");
                return RDBAPI_SUCCESS;
            }
        } else {
            snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERROR: bad tablespace");
            return (ub8) RDBAPI_ERROR;
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


RDBSQLStmt RDBSQLParserGetStmt (RDBSQLParser sqlParser, char **parsedClause, int pretty)
{
    return sqlParser->stmt;
}


static int parse_table (char *table, char tablespace[RDB_KEY_NAME_MAXLEN + 1], char tablename[RDB_KEY_NAME_MAXLEN + 1])
{
    int len = (int) strlen(table);
    char *p = strchr(table, '.');

    if (p && p == strrchr(table, '.')) {
        snprintf(tablespace, RDB_KEY_NAME_MAXLEN, "%.*s", (int)(p - table), table);
        *p++ = 0;
        snprintf(tablename, RDB_KEY_NAME_MAXLEN, "%.*s", (int)(len - (p - table)), p);

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
            fieldexprs[k] = FILEX_NOT_EQUAL;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a != b
        if (cstr_split_substr(substrs[i], "!=", 2, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = FILEX_NOT_EQUAL;
            fieldvalues[k] = strdup(pair[1]);
            k++;
            continue;
        }

        // a >= b
        if (cstr_split_substr(substrs[i], ">=", 2, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = FILEX_GREAT_EQUAL;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a <= b
        if (cstr_split_substr(substrs[i], "<=", 2, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = FILEX_LESS_EQUAL;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a = b
        if (cstr_split_substr(substrs[i], "=", 1, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = FILEX_EQUAL;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a > b
        if (cstr_split_substr(substrs[i], ">", 1, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = FILEX_GREAT_THAN;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a < b
        if (cstr_split_substr(substrs[i], "<", 1, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = FILEX_LESS_THAN;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a LLIKE b
        if (cstr_split_substr(substrs[i], " LLIKE ", 7, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = FILEX_LEFT_LIKE;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a RLIKE b
        if (cstr_split_substr(substrs[i], " RLIKE ", 7, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = FILEX_RIGHT_LIKE;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a LIKE b
        if (cstr_split_substr(substrs[i], " LIKE ", 6, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = FILEX_LIKE;
            fieldvalues[k] = CheckNewFieldValue(pair[1]);
            k++;
            continue;
        }

        // a MATCH b
        if (cstr_split_substr(substrs[i], " MATCH ", 7, pair, 2) == 2) {
            fieldnames[k] = CheckNewFieldName(pair[0]);
            fieldexprs[k] = FILEX_REG_MATCH;
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
    /* -S"redplus.exe -S"CREATE TABLE IF NOT EXISTS xsdb.connect (sid UB4 NOT NULL COMMENT '服务ID', connfd UB4 NOT NULL COMMENT '连接描述符', sessionid UB8X COMMENT '临时会话键', port UB4 COMMENT '客户端端口', host STR(30) COMMENT '客户端IP地址', hwaddr STR(30) COMMENT '网卡地址(字符串)', agent STR(30) COMMENT '连接终端代理类型', ROWKEY(sid | connfd) ) COMMENT '客户端临时连接表';"

        CREATE TABLE IF NOT EXISTS xsdb.connect (
          sid       UB4     NOT NULL COMMENT '服务ID',
          connfd    UB4     NOT NULL COMMENT '连接描述符',
          sessionid UB8X             COMMENT '临时会话键',
          port      UB4              COMMENT '客户端端口',
          host      STR(30)          COMMENT '客户端IP地址',
          hwaddr    STR(30)          COMMENT '网卡地址(字符串)',
          agent     STR(30)          COMMENT '连接终端代理类型',
          ROWKEY(sid | connfd)
        ) COMMENT '客户端临时连接表';
    */
    char table[RDB_KEY_NAME_MAXLEN * 2 + 10 + 1] = {0};

    char * ifnex = NULL;

    char *leftp  = strchr(sql, '(');
    char *rightp = strrchr(sql, ')');

    if (!leftp || !rightp || (rightp - leftp) < 16) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "error CREATE TABLE grammar");
        return RDBAPI_FALSE;      
    }

    ifnex = strstr(sql, "IF NOT EXISTS ");
    if (ifnex) {
        if (leftp - ifnex > 14) {
            snprintf(table, sizeof(table) -1, "%.*s", (int)(leftp - ifnex - 14), ifnex + 14);
        }
        parser->create.fail_on_exists = 0;
    } else {
        snprintf(table, sizeof(table) -1, "%.*s", (int)(leftp - sql), sql);
        parser->create.fail_on_exists = 1;
    }

    if (! parse_table(cstr_LRtrim_chr(table, 32), parser->create.tablespace, parser->create.tablename)) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "parse_table failed");
        return RDBAPI_FALSE;
    }

    do {
        RDBFieldDesc fielddefs[RDBAPI_ARGV_MAXNUM] = {0};
        int numfields = 0;

        int n, nflds;
        char *flds[RDBAPI_ARGV_MAXNUM];

        char *rowkeys[RDBAPI_SQL_KEYS_MAX] = {0};
        int k, rk = 0;

        n = cstr_slpit_chr(leftp + 1, (int)(rightp - leftp - 1), ',', 0, 0);
        if (! n) {
            snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "field not found");
            return RDBAPI_FALSE;
        }
        if (n > RDBAPI_ARGV_MAXNUM) {
            snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "too many fields");
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
                        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "ROWKEY not found");
                        return RDBAPI_FALSE;
                    }
                    if (k > RDBAPI_SQL_KEYS_MAX) {
                        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "ROWKEY too many");
                        return RDBAPI_FALSE;
                    }

                    rk = cstr_slpit_chr(lp + 1, (int)(rp - lp - 1), '|', rowkeys, k);
                }
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
                snprintf(fielddefs[numfields].fieldname, sizeof(fielddefs[numfields].fieldname) - 1, "%s", fld);

                fld = cstr_Ltrim_chr(p, 32);
                int j = cstr_startwith_mul(fld, (int) strlen(fld), parser->datatypes, NULL, 256);
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

                    snprintf(valbuf, sizeof(valbuf) - 1, "%.*s", b - a - 1, a + 1);

                    fielddefs[numfields].length = atoi(cstr_LRtrim_chr(valbuf, 32));
                    if (fielddefs[numfields].length <= 0) {
                        // error

                    }
                } else if (fielddefs[numfields].fieldtype == RDBVT_DEC) {
                    // DEC(length|dscale)
                    // error: not support now
                }

                fielddefs[numfields].rowkey = 1 + cstr_findstr_in(fielddefs[numfields].fieldname, (int)strlen(fielddefs[numfields].fieldname), rowkeys, rk);

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

                    snprintf(fielddefs[numfields].comment, sizeof(fielddefs[numfields].comment) - 1, "%.*s", c2 - c1 - 1, c1 + 1);
                }

                numfields++;
            }

            free(flds[n]);
        }

        for (n = 0; n < RDBAPI_SQL_KEYS_MAX; n++) {
            free(rowkeys[n]);
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

        snprintf(parser->create.tablecomment, sizeof(parser->create.tablecomment) - 1, "%.*s", t2 - t1 - 1, t1 + 1);
    }

    return RDBAPI_TRUE;
}


RDBAPI_BOOL onParseDesc (RDBSQLParser_t * parser, RDBCtx ctx, char *sql, int len)
{
    char table[RDB_KEY_NAME_MAXLEN * 2 + 10 + 1] = {0};

    snprintf(table, sizeof(table) - 1, "%.*s", RDB_KEY_NAME_MAXLEN * 2 + 10, sql);

    if (! parse_table(cstr_LRtrim_chr(table, 32), parser->desctable.tablespace, parser->desctable.tablename)) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "parse_table failed");
        return RDBAPI_FALSE;
    }

    return RDBAPI_TRUE;
}