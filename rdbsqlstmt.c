﻿/***********************************************************************
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
 * rdbsqlstmt.c
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#include "rdbsqlstmt.h"


static ub8 RDBTableGetTimestamp (RDBCtx ctx, const char *tablespace, const char *tablename)
{
    ub8 u8val = (ub8)(-1);

    redisReply *reply = NULL;

    char table_rowkey[256] = {0};

    const char *fldnames[] = {"timestamp", 0};

    snprintf_chkd_V1(table_rowkey, sizeof(table_rowkey), "{%s::%s:%s}", RDB_SYSTEM_TABLE_PREFIX, tablespace, tablename);

    // HMGET {redisdb::$tablespace:$tablename} timestamp
    if (RedisHMGet(ctx, table_rowkey, fldnames, &reply) == RDBAPI_SUCCESS) {
        cstr_to_ub8(10, reply->element[0]->str, reply->element[0]->len, &u8val);
    }

    if (reply) {
        freeReplyObject(reply);
    }

    return u8val;
}


// create stmt object and trim sql unnecessary whilespaces
//
static RDBSQLStmt RDBSQLStmtObjectNew (RDBCtx ctx, const char *sql_block, size_t sql_len)
{
    RDBSQLStmt sqlstmt = (RDBSQLStmt) RDBMemAlloc(sizeof(RDBSQLStmt_t) + sql_len + 1);
    if (sqlstmt) {
        sqlstmt->ctx = ctx;

        memcpy(sqlstmt->sqlblock, sql_block, sql_len);
        sqlstmt->sqlblocksize = (ub4) (sql_len + 1);

        sql_len = cstr_Rtrim_whitespace(sqlstmt->sqlblock, (int) sql_len);

        cstr_Rtrim_chr(sqlstmt->sqlblock, ';', NULL);

        sqlstmt->sqloffset = cstr_Ltrim_whitespace(sqlstmt->sqlblock);
        sqlstmt->offsetlen = cstr_Rtrim_whitespace(sqlstmt->sqloffset, cstr_length(sqlstmt->sqloffset, sql_len));
    } else {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(rdbsqlstmt.c:%d) out of memory.", __LINE__);
    }

    return sqlstmt;
}


static RDBResultMap ResultMapBuildDescTable (const char *tablespace, const char *tablename, const RDBZString valtypetable[256], RDBTableDes_t *tabledes)
{
    RDBAPI_RESULT res;

    int j, len;
    char buf[RDB_KEY_VALUE_SIZE];

    RDBResultMap tablemap = NULL;
    RDBResultMap fieldsmap = NULL;

    RDBRow tablerow = NULL;

    const char *tblnames[] = {
        "tablespace",
        "tablename",
        "timestamp",
        "comment",
        "fields",
        0
    };

    int tblnameslen[] = {10, 9, 9, 7, 6, 0};

    const char *fldnames[] = {
        "fieldname",
        "fieldtype",
        "length",
        "scale",
        "rowkey",
        "nullable",
        "comment",
        0
    };

    int fldnameslen[] = {9, 9, 6, 5, 6, 8, 7, 0};

    snprintf_chkd_V1(buf, sizeof(buf), "{%s::%s}", tablespace, tablename);
    res = RDBResultMapCreate(buf, tblnames, tblnameslen, 5, 0, &tablemap);
    if (res != RDBAPI_SUCCESS) {
        fprintf(stderr, "(%s:%d) RDBResultMapCreate('%s') failed", __FILE__, __LINE__, buf);
        exit(EXIT_FAILURE);
    }

    res = RDBResultMapCreate("=>fields", fldnames, fldnameslen, 7, 0, &fieldsmap);
    if (res != RDBAPI_SUCCESS) {
        fprintf(stderr, "(%s:%d) RDBResultMapCreate('=>fields') failed", __FILE__, __LINE__);
        exit(EXIT_FAILURE);
    }

    res = RDBRowNew(tablemap, tabledes->table_rowkey, sizeof(tabledes->table_rowkey) - 1, &tablerow);
    if (res != RDBAPI_SUCCESS) {
        fprintf(stderr, "(%s:%d) RDBRowNew('%s') failed", __FILE__, __LINE__, tabledes->table_rowkey);
        exit(EXIT_FAILURE);
    }

    RDBCellSetString(RDBRowCell(tablerow, 0), tablespace, -1);
    RDBCellSetString(RDBRowCell(tablerow, 1), tablename, -1);
    RDBCellSetInteger(RDBRowCell(tablerow, 2), (sb8) tabledes->table_timestamp);
    RDBCellSetString(RDBRowCell(tablerow, 3), tabledes->table_comment, -1);
    RDBCellSetResult(RDBRowCell(tablerow, 4), fieldsmap);

    RDBResultMapInsertRow(tablemap, tablerow);

    for (j = 0;  j < tabledes->nfields; j++) {
        RDBRow fieldrow;
        RDBFieldDes_t *fldes = &tabledes->fielddes[j];

        len = snprintf_chkd_V1(buf, sizeof(buf), "{%s::%s:%.*s}", tablespace, tablename, fldes->namelen, fldes->fieldname);

        RDBRowNew(fieldsmap, buf, len, &fieldrow);

        RDBCellSetString(RDBRowCell(fieldrow, 0), fldes->fieldname, fldes->namelen);
        RDBCellSetString(RDBRowCell(fieldrow, 1), RDBCZSTR(valtypetable[fldes->fieldtype]), RDBZSTRLEN(valtypetable[fldes->fieldtype]));

        len = snprintf_chkd_V1(buf, sizeof(buf), "%d", fldes->length);
        RDBCellSetString(RDBRowCell(fieldrow, 2), buf, len);

        len = snprintf_chkd_V1(buf, sizeof(buf), "%d", fldes->dscale);
        RDBCellSetString(RDBRowCell(fieldrow, 3), buf, len);

        len = snprintf_chkd_V1(buf, sizeof(buf), "%d", fldes->rowkey);
        RDBCellSetString(RDBRowCell(fieldrow, 4), buf, len);

        len = snprintf_chkd_V1(buf, sizeof(buf), "%c", fldes->nullable? 'y' : 'N');
        RDBCellSetString(RDBRowCell(fieldrow, 5), buf, len);

        RDBCellSetString(RDBRowCell(fieldrow, 6), fldes->comment, -1);

        RDBResultMapInsertRow(fieldsmap, fieldrow);
    }

    return tablemap;
}


static int RDBSQLNameValidate (const char *name, int len, int maxlen)
{
    const char *pch;

    if (maxlen == -1) {
        maxlen = RDB_KEY_NAME_MAXLEN;
    }

    if (! name || len < 1) {
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
    return len;
}


static int RDBSQLNameValidateMaxLen (const char *name, int maxlen)
{
    if (! name) {
        return 0;
    } else {
        return RDBSQLNameValidate(name, cstr_length(name, maxlen+1), maxlen);
    }
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
    } else if (! cstr_compare_len(table, len, "dual", 4)) {
        snprintf_chkd_V1(tablespace, RDB_KEY_NAME_MAXLEN + 1, RDB_SYSTEM_TABLE_PREFIX);
        snprintf_chkd_V1(tablename, RDB_KEY_NAME_MAXLEN + 1, "%.*s", len, table);

        // ok
        return 1;
    }

    // not found
    return 0;
}


static int parse_select_fields (char *fields, char *fieldnames[], int fieldnameslen[])
{
    char namebuf[RDB_KEY_NAME_MAXLEN * 2 + 1];

    char *p;
    char *saveptr;
    int namelen;
    int i = 0;

    if (fields[0] == '*') {
        // all fields
        return -1;
    }

    p = strtok_r(fields, ",", &saveptr);
    while (p && i <= RDBAPI_ARGV_MAXNUM) {
        char *name;
        namelen = snprintf_chkd_V1(namebuf, RDB_KEY_NAME_MAXLEN * 2, "%s", p);

        name = cstr_LRtrim_chr(namebuf, 32, NULL);
        namelen = RDBSQLNameValidateMaxLen(name, RDB_KEY_NAME_MAXLEN * 2);

        if (! namelen) {
            namelen = cstr_length(name, RDB_KEY_NAME_MAXLEN * 2);

            if (cstr_startwith(name, namelen, "COUNT(", 6) ||
                cstr_startwith(name, namelen, "SUM(", 4) ||
                cstr_startwith(name, namelen, "AVG(", 4) ||
                cstr_startwith(name, namelen, "NOWDATE(", 8) ||
                cstr_startwith(name, namelen, "NOWTIME(", 8) ||
                cstr_startwith(name, namelen, "NOWSTAMP(", 9) ||
                cstr_startwith(name, namelen, "TODATE(", 7) ||
                cstr_startwith(name, namelen, "TOTIME(", 7) ||
                cstr_startwith(name, namelen, "TOSTAMP(", 8) ||
                0) {

                if (! cstr_startwith(name, namelen, "TOSTAMP(", 8)) {
                    name = cstr_trim_whitespace(name);
                    namelen = cstr_length(name, RDB_KEY_NAME_MAXLEN * 2);
                }
            }

            if (! cstr_compare_len(name, namelen, "COUNT(*)", 8) ||
                ! cstr_compare_len(name, namelen, "COUNT(1)", 8) ||
                0) {
                fieldnames[i] = strdup("COUNT(*)");
                fieldnameslen[i] = namelen;
                return 1;
            }

            if (! cstr_compare_len(name, namelen, "NOWDATE()", 9)) {
                fieldnames[i] = strdup(name);
                fieldnameslen[i] = namelen;
                return 1;
            }

            if (! cstr_compare_len(name, namelen, "NOWTIME()", 9)) {
                fieldnames[i] = strdup(name);
                fieldnameslen[i] = namelen;
                return 1;
            }

            if (! cstr_compare_len(name, namelen, "NOWSTAMP()", 10)) {
                fieldnames[i] = strdup(name);
                fieldnameslen[i] = namelen;
                return 1;
            }

            if (cstr_endwith(name, namelen, ")", 1)) {
                fieldnames[i] = strdup(name);
                fieldnameslen[i] = namelen;
                return 1;
            }

            //?? TODO:

            return 0;
        }

        fieldnames[i] = strdup(name);
        fieldnameslen[i] = namelen;

        i++;

        p = strtok_r(0, ",", &saveptr);
    }

    return i;
}


static int parse_field_values (char *fields, char *fieldvalues[RDBAPI_ARGV_MAXNUM])
{
    char valbuf[RDB_SQLSTMT_SQLBLOCK_MAXLEN + 1];

    char *p;
    char *saveptr;
    int i = 0;

    if (fields[0] == '*') {
        // all fields
        return -1;
    }

    p = strtok_r(fields, ",", &saveptr);
    while (p) {
        char *value;

        snprintf_chkd_V1(valbuf, sizeof(valbuf), "%s", p);

        value = cstr_LRtrim_chr(cstr_LRtrim_chr(valbuf, 32, NULL), '\'', NULL);

        fieldvalues[i++] = strdup( value );

        p = strtok_r(0, ",", &saveptr);
    }

    return i;
}


static char * ParseWhereFieldName (const char *name, int *namelen)
{
    char *namestr;
    char tmpbuf[RDB_KEY_NAME_MAXLEN * 2];
    snprintf_chkd_V1(tmpbuf, sizeof(tmpbuf), "%s", name);
    namestr = cstr_LRtrim_chr(tmpbuf, 32, NULL);
    *namelen = RDBSQLNameValidateMaxLen(namestr, RDB_KEY_NAME_MAXLEN);
    return (*namelen)? strdup(namestr) : NULL;
}


static char * ParseWhereFieldValue (const char *value, int *valuelen)
{
    char *valstr;
    char tmpbuf[RDB_KEY_VALUE_SIZE * 2] = {0};

    // make a copy of value
    snprintf_chkd_V1(tmpbuf, sizeof(tmpbuf), "%s", value);

    // trim extra chars
    valstr = cstr_LRtrim_chr(cstr_LRtrim_chr(tmpbuf, 32, NULL), 39, NULL);

    *valuelen = cstr_length(valstr, RDB_KEY_VALUE_SIZE);
    return (*valuelen < RDB_KEY_VALUE_SIZE)? strdup(valstr) : NULL;
}


// WHERE cretime > 123456 AND cretime < 999999
static int parse_where (char *wheres,
    char *fieldnames[RDBAPI_ARGV_MAXNUM],
    int fieldnameslen[RDBAPI_ARGV_MAXNUM],
    RDBFilterExpr fieldexprs[RDBAPI_ARGV_MAXNUM],
    char *fieldvalues[RDBAPI_ARGV_MAXNUM],
    int fieldvalueslen[RDBAPI_ARGV_MAXNUM])
{
    int i, num, k = 0;
    char *pstr;
    char *substrs[RDBAPI_ARGV_MAXNUM] = {0};

    if (!wheres || !*wheres) {
        return 0;
    }

    num = cstr_split_substr(wheres, " AND ", 5, substrs, RDBAPI_ARGV_MAXNUM);

    for (i = 0; i < num; i++) {
        char *pair[2] = {0};

        // a <> b
        if (cstr_split_substr(substrs[i], "<>", 2, pair, 2) == 2) {
            fieldexprs[k] = RDBFIL_NOT_EQUAL;

            pstr = ParseWhereFieldName(pair[0], &fieldnameslen[k]);
            if (!pstr) {
                fprintf(stderr, "invalid WHERE field");
                return (-1);
            }
            fieldnames[k] = pstr;

            pstr = ParseWhereFieldValue(pair[1], &fieldvalueslen[k]);
            if (! pstr) {
                fprintf(stderr, "bad WHERE clause: %s <> ...\n", fieldnames[k]);
                return (-1);
            }
            fieldvalues[k] = pstr;

            k++;
            continue;
        }

        // a != b
        if (cstr_split_substr(substrs[i], "!=", 2, pair, 2) == 2) {
            fieldexprs[k] = RDBFIL_NOT_EQUAL;

            pstr = ParseWhereFieldName(pair[0], &fieldnameslen[k]);
            if (!pstr) {
                fprintf(stderr, "invalid WHERE field");
                return (-1);
            }
            fieldnames[k] = pstr;

            pstr = ParseWhereFieldValue(pair[1], &fieldvalueslen[k]);
            if (! pstr) {
                fprintf(stderr, "bad WHERE clause: %s != ...\n", fieldnames[k]);
                return (-1);
            }
            fieldvalues[k] = pstr;

            k++;
            continue;
        }

        // a >= b
        if (cstr_split_substr(substrs[i], ">=", 2, pair, 2) == 2) {
            fieldexprs[k] = RDBFIL_GREAT_EQUAL;

            pstr = ParseWhereFieldName(pair[0], &fieldnameslen[k]);
            if (!pstr) {
                fprintf(stderr, "invalid WHERE field");
                return (-1);
            }
            fieldnames[k] = pstr;

            pstr = ParseWhereFieldValue(pair[1], &fieldvalueslen[k]);
            if (! pstr) {
                fprintf(stderr, "bad WHERE clause: %s >= ...\n", fieldnames[k]);
                return (-1);
            }
            fieldvalues[k] = pstr;

            k++;
            continue;
        }

        // a <= b
        if (cstr_split_substr(substrs[i], "<=", 2, pair, 2) == 2) {
            fieldexprs[k] = RDBFIL_LESS_EQUAL;

            pstr = ParseWhereFieldName(pair[0], &fieldnameslen[k]);
            if (!pstr) {
                fprintf(stderr, "invalid WHERE field");
                return (-1);
            }
            fieldnames[k] = pstr;

            pstr = ParseWhereFieldValue(pair[1], &fieldvalueslen[k]);
            if (! pstr) {
                fprintf(stderr, "bad WHERE clause: %s <= ...\n", fieldnames[k]);
                return (-1);
            }
            fieldvalues[k] = pstr;

            k++;
            continue;
        }

        // a = b
        if (cstr_split_substr(substrs[i], "=", 1, pair, 2) == 2) {
            fieldexprs[k] = RDBFIL_EQUAL;

            pstr = ParseWhereFieldName(pair[0], &fieldnameslen[k]);
            if (!pstr) {
                fprintf(stderr, "invalid WHERE field");
                return (-1);
            }
            fieldnames[k] = pstr;

            pstr = ParseWhereFieldValue(pair[1], &fieldvalueslen[k]);
            if (! pstr) {
                fprintf(stderr, "bad WHERE clause: %s = ...\n", fieldnames[k]);
                return (-1);
            }
            fieldvalues[k] = pstr;

            k++;
            continue;
        }

        // a > b
        if (cstr_split_substr(substrs[i], ">", 1, pair, 2) == 2) {
            fieldexprs[k] = RDBFIL_GREAT_THAN;

            pstr = ParseWhereFieldName(pair[0], &fieldnameslen[k]);
            if (!pstr) {
                fprintf(stderr, "invalid WHERE field");
                return (-1);
            }
            fieldnames[k] = pstr;

            pstr = ParseWhereFieldValue(pair[1], &fieldvalueslen[k]);
            if (! pstr) {
                fprintf(stderr, "bad WHERE clause: %s > ...\n", fieldnames[k]);
                return (-1);
            }
            fieldvalues[k] = pstr;

            k++;
            continue;
        }

        // a < b
        if (cstr_split_substr(substrs[i], "<", 1, pair, 2) == 2) {
            fieldexprs[k] = RDBFIL_LESS_THAN;

            pstr = ParseWhereFieldName(pair[0], &fieldnameslen[k]);
            if (!pstr) {
                fprintf(stderr, "invalid WHERE field");
                return (-1);
            }
            fieldnames[k] = pstr;

            pstr = ParseWhereFieldValue(pair[1], &fieldvalueslen[k]);
            if (! pstr) {
                fprintf(stderr, "bad WHERE clause: %s < ...\n", fieldnames[k]);
                return (-1);
            }
            fieldvalues[k] = pstr;

            k++;
            continue;
        }

        // a LLIKE b
        if (cstr_split_substr(substrs[i], " LLIKE ", 7, pair, 2) == 2) {
            fieldexprs[k] = RDBFIL_LEFT_LIKE;

            pstr = ParseWhereFieldName(pair[0], &fieldnameslen[k]);
            if (!pstr) {
                fprintf(stderr, "invalid WHERE field");
                return (-1);
            }
            fieldnames[k] = pstr;

            pstr = ParseWhereFieldValue(pair[1], &fieldvalueslen[k]);
            if (! pstr) {
                fprintf(stderr, "bad WHERE clause: %s LLIKE ...\n", fieldnames[k]);
                return (-1);
            }
            fieldvalues[k] = pstr;

            k++;
            continue;
        }

        // a RLIKE b
        if (cstr_split_substr(substrs[i], " RLIKE ", 7, pair, 2) == 2) {
            fieldexprs[k] = RDBFIL_RIGHT_LIKE;

            pstr = ParseWhereFieldName(pair[0], &fieldnameslen[k]);
            if (!pstr) {
                fprintf(stderr, "invalid WHERE field");
                return (-1);
            }
            fieldnames[k] = pstr;

            pstr = ParseWhereFieldValue(pair[1], &fieldvalueslen[k]);
            if (! pstr) {
                fprintf(stderr, "bad WHERE clause: %s RLIKE ...\n", fieldnames[k]);
                return (-1);
            }
            fieldvalues[k] = pstr;

            k++;
            continue;
        }

        // a LIKE b
        if (cstr_split_substr(substrs[i], " LIKE ", 6, pair, 2) == 2) {
            fieldexprs[k] = RDBFIL_LIKE;

            pstr = ParseWhereFieldName(pair[0], &fieldnameslen[k]);
            if (!pstr) {
                fprintf(stderr, "invalid WHERE field");
                return (-1);
            }
            fieldnames[k] = pstr;

            pstr = ParseWhereFieldValue(pair[1], &fieldvalueslen[k]);
            if (! pstr) {
                fprintf(stderr, "bad WHERE clause: %s LIKE ...\n", fieldnames[k]);
                return (-1);
            }
            fieldvalues[k] = pstr;

            k++;
            continue;
        }

        // a MATCH b
        if (cstr_split_substr(substrs[i], " MATCH ", 7, pair, 2) == 2) {
            fieldexprs[k] = RDBFIL_MATCH;

            pstr = ParseWhereFieldName(pair[0], &fieldnameslen[k]);
            if (!pstr) {
                fprintf(stderr, "invalid WHERE field");
                return (-1);
            }
            fieldnames[k] = pstr;

            pstr = ParseWhereFieldValue(pair[1], &fieldvalueslen[k]);
            if (! pstr) {
                fprintf(stderr, "bad WHERE clause: %s MATCH ...\n", fieldnames[k]);
                return (-1);
            }
            fieldvalues[k] = pstr;

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
static char * parse_create_field (const char *sqlblockaddr,
    char *sql, int sqlen, const RDBZString valtypetable[256],
    RDBFieldDes_t *fieldes, char **rowkeys,
    char *tblcomment, size_t tblcommsz,
    char *buf, size_t bufsz)
{
    int j, np, len;
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
                //??//errat = (int)(sqlc - sqladdr) + sqloffs;
                //??//snprintf_chkd_V1(buf, bufsz, "SQLError: ROWKEY not enclosed - error char at(%d): '%s'", errat, sqladdr + errat);
                return NULL;
            }

            *endp++ = 0;
            sqlc++;

            len = cstr_length(sqlc, endp - sqlc);

            len = cstr_slpit_chr(sqlc, len, 44, rowkeys, NULL, RDBAPI_SQL_KEYS_MAX + 1);
            if (len > RDBAPI_SQL_KEYS_MAX) {
                //??//errat = (int)(sqlc - sqladdr) + sqloffs;
                //??//snprintf_chkd_V1(buf, bufsz, "SQLError: too many keys in ROWKEY - error char at(%d): '%s'", errat, sqladdr + errat);
                return NULL;
            }

            sqlc = endp;
            endp = strchr(sqlc, 41);
            if (! endp) {
                //??//errat = (int)(sqlc - sqladdr) + sqloffs;
                //??//snprintf_chkd_V1(buf, bufsz, "SQLError: CREATE not enclosed - error char at(%d): '%s'", errat, sqladdr + errat);
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
                    //??//errat = (int)(sqlc - sqladdr) + sqloffs;
                    //??//snprintf_chkd_V1(buf, bufsz, "SQLError: invalid COMMENT - error char at(%d): '%s'", errat, sqladdr + errat);
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

        //??//errat = (int)(sqlc - sqladdr) + sqloffs;
        //??//snprintf_chkd_V1(buf, bufsz, "SQLError: field name not found - error char at(%d): '%s'", errat, sqladdr + errat);
        return NULL;
    }

    sqlc = cstr_Ltrim_whitespace(sqlc);

    np = re_match("[\\s]+[A-Z]+[\\d]*", sqlc);
    sqlc[np] = 0;

    fieldes->namelen = snprintf_chkd_V1(fieldes->fieldname, sizeof(fieldes->fieldname), "%.*s", np, sqlc);

    sqlc = cstr_Ltrim_whitespace(sqlc + np + 1);

    // TYPE
    np = re_match("[\\W]+", sqlc);
    if (np == -1) {
        //??//errat = (int)(sqlc - sqladdr) + sqloffs;
        //??//snprintf_chkd_V1(buf, bufsz, "SQLError: field type not found - error char at(%d): '%s'", errat, sqladdr + errat);
        return NULL;
    }

    len = snprintf_chkd_V1(buf, bufsz, "%.*s", np, sqlc);

    for (j = 0; j < 256; j++) {
        if (valtypetable[j] && ! RDBZStringCmp(valtypetable[j], buf, len)) {
            break;
        }
    }
    if (j == 256) {
        //??//errat = (int)(sqlc - sqladdr) + sqloffs;
        //??//snprintf_chkd_V1(buf, bufsz, "SQLError: bad field type - error char at(%d): '%s'", errat, sqladdr + errat);
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
            //??//errat = (int)(sqlc - sqladdr) + sqloffs;
            //??//snprintf_chkd_V1(buf, bufsz, "SQLError: not enclosed - error char at(%d): '%s'", errat, sqladdr + errat);
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
                //??//errat = (int)(sqlc - sqladdr) + sqloffs;
                //??//snprintf_chkd_V1(buf, bufsz, "SQLError: invalid type length - error char at(%d): '%s'", errat, sqladdr + errat);
                return NULL;
            }

            sqlc = cstr_Ltrim_whitespace(sqlc + 1);
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
            //??//errat = (int)(sqlc - sqladdr) + sqloffs;
            //??//snprintf_chkd_V1(buf, bufsz, "SQLError: invalid COMMENT - error char at(%d): '%s'", errat, sqladdr + errat);
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
    //??//errat = (int)(sqlc - sqladdr) + sqloffs;
    //??//snprintf_chkd_V1(buf, bufsz, "SQLError: ROWKEY not found - error char at(%d): '%s'", errat, sqladdr + errat);
    return NULL;
}


// ( a,b,'hello,shanghai' )
//
static char * parse_upsert_value (char *sqlblockaddr, char *sql, char **value, int *valuelen)
{
    int vlen = 0;
    char *endp = NULL;
    char *valp = cstr_Ltrim_whitespace(sql);

    int quot = 0;

    *value = NULL;

    if (*valp == '\0') {
        return NULL;
    }

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
        *valuelen = snprintf_chkd_V1(*value, vlen + 1 + quot + quot, "'%.*s'", vlen, valp);
    } else {
        *valuelen = snprintf_chkd_V1(*value, vlen + 1 + quot + quot, "%.*s", vlen, valp);
    }

    return endp;
}


// UPDATE a=b, c=c+1, e='hello= , world';
//
static char * parse_upsert_col (char *sqlblockaddr, char *sql, char **colname, int *colnamelen, char **colvalue, int *colvaluelen)
{
    int np, klen, vlen;
    char *endp;

    char *key, *val;

    int quot = 0;

    *colname = NULL;
    *colvalue = NULL;

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
        char *namebuf = RDBMemAlloc(klen + 1);
        char *valuebuf = RDBMemAlloc(vlen + 1 + quot *2);

        memcpy(namebuf, key, klen);

        *colnamelen = klen;

        if (quot) {
            *colvaluelen = snprintf_chkd_V1(valuebuf, vlen + 1 + quot *2, "'%.*s'", vlen, val);
        } else {
            *colvaluelen = snprintf_chkd_V1(valuebuf, vlen + 1 + quot *2, "%.*s", vlen, val);
        }

        *colname = namebuf;
        *colvalue = valuebuf;
    } while(0);

    return endp;
}



// SELECT $fields FROM $database.$tablename WHERE $condition OFFSET $position LIMIT $count
// SELECT * FROM $database.$tablename <WHERE $condition> <OFFSET $position> <LIMIT $count>
//
void SQLStmtParseSelect (RDBCtx ctx, RDBSQLStmt sqlstmt)
{
    int len, nwhere, noffset, nlimit;
    char *psz, *pwhere, *poffset, *plimit;

    char *sqlc = cstr_Ltrim_whitespace(sqlstmt->sqloffset + 7);

    len = re_match("[\\s]+FROM[\\s]+", sqlc);
    if (len == -1) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: not found FROM. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
        return;
    }

    psz = &sqlc[len];
    *psz++ = '\0';

    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, -1));
    sqlstmt->select.numselect = parse_select_fields(sqlc, sqlstmt->select.selectfields, sqlstmt->select.selectfieldslen);
    if (! sqlstmt->select.numselect || sqlstmt->select.numselect > RDBAPI_ARGV_MAXNUM) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: none fields. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
        return;
    }

    // skip _FROM_
    sqlc = cstr_Ltrim_whitespace(cstr_Ltrim_whitespace(psz) + 5);

    // find WHERE ...
    nwhere = re_match("[\\s]+WHERE[\\s]+", sqlc);
    noffset = re_match("[\\s]+OFFSET[\\s]+", sqlc);
    nlimit = re_match("[\\s]+LIMIT[\\s]+", sqlc);

    pwhere = poffset = plimit = NULL;

    if (nwhere != -1) {
        psz = &sqlc[nwhere];
        *psz++ = 0;

        pwhere = cstr_Ltrim_whitespace(strstr(psz, "WHERE") + 5);
    }

    if (noffset != -1) {
        psz = &sqlc[noffset];
        *psz++ = 0;

        poffset = cstr_Ltrim_whitespace(strstr(psz, "OFFSET") + 6);
    }

    if (nlimit != -1) {
        psz = &sqlc[nlimit];
        *psz++ = 0;

        plimit = cstr_Ltrim_whitespace(strstr(psz, "LIMIT") + 5);
    }

    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, -1));
    if (! parse_table(sqlc, sqlstmt->select.tablespace, sqlstmt->select.tablename)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: bad tablename. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
        return;
    }

    if (pwhere) {
        nwhere = cstr_Rtrim_whitespace(pwhere, cstr_length(pwhere, -1));

        sqlstmt->select.numwhere = parse_where(pwhere, sqlstmt->select.fields, sqlstmt->select.fieldslen, sqlstmt->select.fieldexprs, sqlstmt->select.fieldvals, sqlstmt->select.fieldvalslen);
        if (sqlstmt->select.numwhere < 0 || sqlstmt->select.numwhere > RDBAPI_ARGV_MAXNUM) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: invalid WHERE clause. errat(%d): '%s'", (int)(pwhere - sqlstmt->sqlblock), pwhere);
            return;
        }
    }

    if (poffset) {
        noffset = cstr_Rtrim_whitespace(poffset, cstr_length(poffset, -1));
        
        if (cstr_to_ub8(10, poffset, noffset, &sqlstmt->select.offset) < 0) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: invalid OFFSET. errat(%d): '%s'", (int)(poffset - sqlstmt->sqlblock), poffset);
            return;
        }
    }

    if (plimit) {
        ub8 limit = 10;

        nlimit = cstr_Rtrim_whitespace(plimit, cstr_length(plimit, -1));

        if (cstr_to_ub8(10, plimit, nlimit, &limit) < 0 || limit == (-1) || limit > RDB_TABLE_LIMIT_MAX) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: invalid LIMIT. errat(%d): '%s'", (int)(plimit - sqlstmt->sqlblock), plimit);
            return;
        }

        sqlstmt->select.limit = (ub4) limit;
    }

    sqlstmt->stmt = RDBSQL_SELECT;
}


// DELETE FROM $database.$tablename <WHERE $condition> <OFFSET $position> <LIMIT $count>
//
void SQLStmtParseDelete (RDBCtx ctx, RDBSQLStmt sqlstmt)
{
    int len, nwhere, noffset, nlimit;
    char *psz, *pwhere, *poffset, *plimit;

    char *sqlc = cstr_Ltrim_whitespace(sqlstmt->sqloffset + 7);

    // tablename
    sqlc = cstr_Ltrim_whitespace(strstr(sqlc, "FROM") + 5);

    // find WHERE ...
    nwhere = re_match("[\\s]+WHERE[\\s]+", sqlc);
    noffset = re_match("[\\s]+OFFSET[\\s]+", sqlc);
    nlimit = re_match("[\\s]+LIMIT[\\s]+", sqlc);

    pwhere = poffset = plimit = NULL;

    if (nwhere != -1) {
        psz = &sqlc[nwhere];
        *psz++ = 0;

        pwhere = cstr_Ltrim_whitespace(strstr(psz, "WHERE") + 5);
    }

    if (noffset != -1) {
         psz = &sqlc[noffset];
        *psz++ = 0;

        poffset = cstr_Ltrim_whitespace(strstr(psz, "OFFSET") + 6);
    }

    if (nlimit != -1) {
        psz = &sqlc[nlimit];
        *psz++ = 0;

        plimit = cstr_Ltrim_whitespace(strstr(psz, "LIMIT") + 5);
    }

    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, -1));
    if (! parse_table(sqlc, sqlstmt->select.tablespace, sqlstmt->select.tablename)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: bad tablename. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
        return;
    }

    if (pwhere) {
        nwhere = cstr_Rtrim_whitespace(pwhere, cstr_length(pwhere, -1));
        sqlstmt->select.numwhere = parse_where(pwhere, sqlstmt->select.fields, sqlstmt->select.fieldslen, sqlstmt->select.fieldexprs, sqlstmt->select.fieldvals, sqlstmt->select.fieldvalslen);
        if (sqlstmt->select.numwhere < 0 || sqlstmt->select.numwhere > RDBAPI_ARGV_MAXNUM) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: invalid WHERE clause. errat(%d): '%s'", (int)(pwhere - sqlstmt->sqlblock), pwhere);
            return;
        }
    }

    if (poffset) {
        noffset = cstr_Rtrim_whitespace(poffset, cstr_length(poffset, -1));
        if (cstr_to_ub8(10, poffset, noffset, &sqlstmt->select.offset) < 0) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: invalid OFFSET. errat(%d): '%s'", (int)(poffset - sqlstmt->sqlblock), poffset);
            return;
        }
    }

    if (plimit) {
        ub8 limit = 10;

        nlimit = cstr_Rtrim_whitespace(plimit, cstr_length(plimit, -1));
        if (cstr_to_ub8(10, plimit, nlimit, &limit) < 0 || limit == (-1) || limit > RDB_TABLE_LIMIT_MAX) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: invalid LIMIT. errat(%d): '%s'", (int)(plimit - sqlstmt->sqlblock), plimit);
            return;
        }

        sqlstmt->select.limit = limit;
    }

    sqlstmt->stmt = RDBSQL_DELETE;
}


void SQLStmtParseUpsert (RDBCtx ctx, RDBSQLStmt sqlstmt)
{
    int i, np, len;

    char *startp, *endp;

    int numfields = 0;
    int numvalues = 0;
    int update = 0;

    char *selectsql = NULL;

    // pass UPSERT_
    char *sqlc = cstr_Ltrim_whitespace(sqlstmt->sqloffset + 7);

    // pass INTO_
    sqlc = cstr_Ltrim_whitespace(sqlc + 5);

    // find '('
    startp = strchr(sqlc, 40);
    if (! startp) {
        int select = re_match("[\\s]+SELECT[\\s]+", sqlc);

        if (select == -1) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: tablename not assigned. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
            return;
        }

        selectsql = &sqlc[select];
        *selectsql++ = '\0';

        // no field names, use all fields default
        sqlstmt->upsert.fields_by_select = 1;
    } else {
        int select = re_match("([\\s]*SELECT[\\s]+", startp);
        if (select == -1) {
            select = re_match("[\\s]+SELECT[\\s]+", startp);
        } else {
            endp = strrchr(startp, 41);
            if (! endp) {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: non closure char '('. errat(%d): '%.*s ...'",
                    (int) (startp + select - sqlstmt->sqlblock),
                    cstr_length(startp + select, 40), startp + select);
                return;
            }
            *endp = '\0';
        }

        if (select != -1) {
            selectsql = &startp[select];
            *selectsql++ = '\0';
        }

        if (select == 0) {
            // no field names, use all fields default
            sqlstmt->upsert.fields_by_select = 1;
        }

        *startp++ = 0;
    }

    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, -1));
    if (! parse_table(sqlc, sqlstmt->upsert.tablespace, sqlstmt->upsert.tablename)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: illegal tablename. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
        return;
    }

    if (! sqlstmt->upsert.fields_by_select) {
        // fieldnames
        sqlc = cstr_Ltrim_whitespace(startp);
        endp = strchr(sqlc, 41);
        if (! endp) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: non closure char. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
            return;
        }
        *endp++ = 0;

        len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, -1));

        numfields = cstr_slpit_chr(sqlc, len, 44, sqlstmt->upsert.fieldnames, sqlstmt->upsert.fieldnameslen, RDBAPI_ARGV_MAXNUM + 1);
        if (numfields == 0) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: fields not found");
            return;
        }
        if (numfields > RDBAPI_ARGV_MAXNUM) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: too many fields");
            return;
        }
        for(i = 0; i < numfields; i++) {
            if (sqlstmt->upsert.fieldnameslen[i] > RDB_KEY_NAME_MAXLEN) {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: too long fieldname: %s", sqlstmt->upsert.fieldnames[i]);
                return;
            }
        }

        sqlstmt->upsert.numfields = numfields;
        sqlc = endp;
    } else {
        // fields_by_select
        sqlc = NULL;
    }

    if (selectsql) {
        sqlstmt->upsert.selectstmt = RDBSQLStmtObjectNew(ctx, selectsql, cstr_length(selectsql, -1));
        if (! sqlstmt->upsert.selectstmt) {
            return;
        }

        SQLStmtParseSelect(ctx, sqlstmt->upsert.selectstmt);

        if (sqlstmt->upsert.selectstmt->stmt != RDBSQL_SELECT) {
            return;
        }

        sqlstmt->upsert.upsertmode = RDBSQL_UPSERT_MODE_SELECT;
    } else if (sqlc) {
        // VALUES(...)
        sqlc = cstr_Ltrim_whitespace(sqlc);
        np = re_match("VALUES[\\s]*(", sqlc);
        if (np != 0) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: VALUES not found. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
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
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: non closure char. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
            return;
        }
        *endp++ = 0;

        sqlc = cstr_Ltrim_whitespace(sqlc + 1);
        len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, -1));

        // ( a,b,'hello,shanghai' )
        while (sqlc && numvalues < numfields + 1) {
            char *value = NULL;
            int valuelen = 0;
            sqlc = parse_upsert_value(sqlstmt->sqlblock, sqlc, &value, &valuelen);
            if (value) {
                sqlstmt->upsert.fieldvalues[numvalues] = value;
                sqlstmt->upsert.fieldvalueslen[numvalues] = valuelen;
                numvalues++;
            }
        }

        if (numvalues > numfields) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: excess field value: '%s'", sqlstmt->upsert.fieldvalues[numfields]);
            return;
        } else if (numvalues < numfields) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: excess fieldname: '%s'", sqlstmt->upsert.fieldnames[numvalues]);
            return;
        }

        sqlstmt->upsert.upsertmode = RDBSQL_UPSERT_MODE_INSERT;

        sqlc = endp;
        if (np != -1) {
            np = re_match("ON[\\s]+DUPLICATE[\\s]+KEY[\\s]+UPDATE[\\s]+", sqlc);

            if (np == -1) {
                np = re_match("ON[\\s]+DUPLICATE[\\s]+KEY[\\s]+IGNORE[\\W]*", sqlc);
                if (np == -1) {
                    snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: non closure char. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
                    return;
                }
                sqlstmt->upsert.upsertmode = RDBSQL_UPSERT_MODE_IGNORE;
            } else {
                startp = strstr(sqlc, "UPDATE") + 6;
                *startp++ = 0;
                sqlc = cstr_Ltrim_whitespace(startp);

                sqlstmt->upsert.upsertmode = RDBSQL_UPSERT_MODE_UPDATE;
            }
        }

        if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_UPDATE) {
            // UPDATE a=b, c=c+1, e='hello= , world';
            while (sqlc && sqlstmt->upsert.updcols < RDBAPI_ARGV_MAXNUM + 1) {
                char *colname;
                char *colvalue;
                int colnamelen;
                int colvaluelen;

                sqlc = parse_upsert_col(sqlstmt->sqlblock, sqlc, &colname, &colnamelen, &colvalue, &colvaluelen);

                if (colname && colvalue) {
                    sqlstmt->upsert.updcolnames[sqlstmt->upsert.updcols] = colname;
                    sqlstmt->upsert.updcolvalues[sqlstmt->upsert.updcols] = colvalue;

                    sqlstmt->upsert.updcolnameslen[sqlstmt->upsert.updcols] = colnamelen;
                    sqlstmt->upsert.updcolvalueslen[sqlstmt->upsert.updcols] = colvaluelen;

                    sqlstmt->upsert.updcols++;
                }
            }
            if (! sqlstmt->upsert.updcols || sqlstmt->upsert.updcols > RDBAPI_ARGV_MAXNUM) {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: UPDATE clause error. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
                return;
            }
        }
    }

    // success
    sqlstmt->stmt = RDBSQL_UPSERT;
}


void SQLStmtParseCreate (RDBCtx ctx, RDBSQLStmt sqlstmt)
{
    int i;

    char *sqlc;
    int len;

    char *sqlnext;
    int nextlen;

    char *rowkeys[RDBAPI_SQL_KEYS_MAX + 1] = {0};

    sqlc = cstr_Ltrim_whitespace(sqlstmt->sqloffset + 7);
    sqlc = cstr_Ltrim_whitespace(sqlc + 6);

    len = re_match("IF[\\s]+NOT[\\s]+EXISTS[\\s]+", sqlc);
    if (len == 0) {
        sqlstmt->create.fail_on_exists = 0;
    } else {
        sqlstmt->create.fail_on_exists = 1;
    }

    len = cstr_length(sqlc, -1);

    sqlnext = cstr_Lfind_chr(sqlc, len, '(');
    if (! sqlnext) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: bad CREATE clause: '%s'", sqlc);
        return;
    }

    if (! sqlstmt->create.fail_on_exists) {
        sqlc = strstr(sqlc, "EXISTS");
        sqlc = cstr_Ltrim_whitespace(sqlc + 6);
    }

    *sqlnext++ = 0;

    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, (int)(sqlnext - sqlc)));
    if (! parse_table(sqlc, sqlstmt->create.tablespace, sqlstmt->create.tablename)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: parse table failed. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
        return;
    }

    // parse all fields
    nextlen = cstr_length(sqlnext, -1);
    do {
        sqlnext = parse_create_field(sqlstmt->sqlblock,
            sqlnext, nextlen, ctx->env->valtypetable,
            &sqlstmt->create.fielddefs[sqlstmt->create.numfields++], rowkeys,
            sqlstmt->create.tablecomment, sizeof(sqlstmt->create.tablecomment),
            ctx->errmsg, sizeof(ctx->errmsg));

        if (! sqlnext && *ctx->errmsg) {
            // failed on error
            cstr_varray_free(rowkeys, RDBAPI_SQL_KEYS_MAX);
            return;
        }
    } while(sqlnext && sqlstmt->create.numfields <= RDBAPI_ARGV_MAXNUM);

    if (! *rowkeys) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: no keys for ROWKEY");
        return;
    }

    sqlstmt->create.numfields--;

    if (sqlstmt->create.numfields < 1) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: fields not found");
        cstr_varray_free(rowkeys, RDBAPI_SQL_KEYS_MAX);
        return;
    }
    if (sqlstmt->create.numfields > RDBAPI_ARGV_MAXNUM) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: too many fields");
        cstr_varray_free(rowkeys, RDBAPI_SQL_KEYS_MAX);
        return;
    }


    for (i = 0; i < sqlstmt->create.numfields; i++) {
        sqlstmt->create.fielddefs[i].rowkey = 1 +
            cstr_findstr_in(sqlstmt->create.fielddefs[i].fieldname, sqlstmt->create.fielddefs[i].namelen, (const char **) rowkeys, -1);
    }

    cstr_varray_free(rowkeys, RDBAPI_SQL_KEYS_MAX);

    // success
    sqlstmt->stmt = RDBSQL_CREATE;
}


// DESC $database.$table
//
void SQLStmtParseDesc (RDBCtx ctx, RDBSQLStmt sqlstmt)
{
    char tblbuf[RDB_KEY_NAME_MAXLEN * 2 + 4] = {0};

    char *sqlc = cstr_Ltrim_whitespace(sqlstmt->sqloffset + 5);

    int len = re_match("[\\s]+", sqlc);
    if (len != -1) {
        RDBSQLStmtError(RDBSQL_ERR_ILLEGAL_CHAR, sqlc);
        return;
    }

    len = cstr_length(sqlc, sizeof(tblbuf));
    if (len == sizeof(tblbuf)) {
        RDBSQLStmtError(RDBSQL_ERR_TOO_LONG, sqlc);
        return;
    }

    snprintf_chkd_V1(tblbuf, sizeof(tblbuf), "%.*s", len, sqlc);

    if (! parse_table(tblbuf, sqlstmt->desctable.tablespace, sqlstmt->desctable.tablename)) {
        RDBSQLStmtError(RDBSQL_ERR_INVAL_TABLE, sqlc);
        return;
    }

    sqlstmt->stmt = RDBSQL_DESC_TABLE;
}


// INFO <section> <nodeid>
// INFO <nodeid>
//
void SQLStmtParseInfo (RDBCtx ctx, RDBSQLStmt sqlstmt)
{
    char *sqlc = cstr_Ltrim_whitespace(sqlstmt->sqloffset + 4);

    int len = cstr_length(sqlc, sqlstmt->offsetlen);

    // 1-based. 0 for all.
    int nodeid = 0;

    if (len == 0) {
        // INFO
        sqlstmt->info.section = MAX_NODEINFO_SECTIONS;
        sqlstmt->info.whichnode = nodeid;

        sqlstmt->stmt = RDBSQL_INFO_SECTION;
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
            NULL
        };

        const char *secpattern;

        char *sqlc0 = sqlc;

        cstr_toupper(sqlc, len);

        while ((secpattern = sections[i]) != NULL) {
            if (re_match(secpattern, sqlc) != -1) {
                sqlstmt->info.section = (RDBNodeInfoSection) i;

                sqlc += strchr(secpattern, '[') - secpattern;
                sqlc = cstr_Ltrim_whitespace(sqlc);
                break;
            }

            i++;
        }

        // find nodeid
        if (cstr_length(sqlc, 10) > 0 && re_match("^[\\d]+", sqlc) == -1) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: bad section or nodeid - errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
            return;
        }

        nodeid = atoi(sqlc);
        if (nodeid < 0 || nodeid > RDBEnvNumNodes(ctx->env)) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: bad nodeid - errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
            return;
        }

        if (! secpattern) {
            if (sqlc0 == sqlc) {
                sqlstmt->info.section = MAX_NODEINFO_SECTIONS;
            } else {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: bad section - errat(%d): '%s'", (int)(sqlc0 - sqlstmt->sqlblock), sqlc0);
                return;
            }
        }

        sqlstmt->info.whichnode = nodeid;
        sqlstmt->stmt = RDBSQL_INFO_SECTION;
    }
}


// SHOW DATABASES;
//
void SQLStmtParseShowDatabases (RDBCtx ctx, RDBSQLStmt sqlstmt)
{
    sqlstmt->stmt = RDBSQL_SHOW_DATABASES;
}


// SHOW TABLES $database
//
void SQLStmtParseShowTables (RDBCtx ctx, RDBSQLStmt sqlstmt)
{
    int len;

    char *sqlc = cstr_Ltrim_whitespace(sqlstmt->sqloffset + 5);

    sqlc = cstr_Ltrim_whitespace(sqlc + 7);

    len = re_match("[\\s]+", sqlc);
    if (len != -1) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: illegal char at(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock + len), sqlc);
        return;
    }

    len = cstr_length(sqlc, sqlstmt->offsetlen);
    if (! len) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: database not assigned. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
        return;
    }
    if (len >= sizeof(sqlstmt->showtables.tablespace)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: too long database name. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
        return;
    }

    snprintf_chkd_V1(sqlstmt->showtables.tablespace, sizeof(sqlstmt->showtables.tablespace), "%.*s", len, sqlc);
    sqlstmt->stmt = RDBSQL_SHOW_TABLES;
}


// DROP TABLE $database.$tablename
//
void SQLStmtParseDrop (RDBCtx ctx, RDBSQLStmt sqlstmt)
{
    char table[RDB_KEY_NAME_MAXLEN * 2 + 4] = {0};

    char *sqlc = cstr_Ltrim_whitespace(strstr(sqlstmt->sqloffset + 4, "TABLE") + 6);

    snprintf_chkd_V1(table, sizeof(table), "%.*s", cstr_length(sqlc, (int)sizeof(table)), sqlc);

    if (! parse_table(table, sqlstmt->droptable.tablespace, sqlstmt->droptable.tablename)) {
        RDBSQLStmtError(RDBSQL_ERR_INVAL_TABLE, sqlc);
        return;
    }

    sqlstmt->stmt = RDBSQL_DROP_TABLE;
}

// COMMAND
//
void SQLStmtParseCommand (RDBCtx ctx, RDBSQLStmt sqlstmt, int envcmd)
{
    if (envcmd == RDBENV_COMMAND_VERBOSE_ON) {
        ctx->env->verbose = 1;
        sqlstmt->stmt = envcmd;
        return;
    }

    if (envcmd == RDBENV_COMMAND_VERBOSE_OFF) {
        ctx->env->verbose = 0;
        sqlstmt->stmt = envcmd;
        return;
    }

    if (envcmd == RDBENV_COMMAND_DELIMITER) {
        char *p1 = strchr(sqlstmt->sqloffset, 39);
        char *p2 = strrchr(sqlstmt->sqloffset, 39);
        if (p1 && p2 && p2-p1 == 2) {
            p1++;
            ctx->env->delimiter = *p1;
            sqlstmt->stmt = envcmd;
            return;
        }

        if (!p1 && !p2) {
            p1 = cstr_Ltrim_whitespace(sqlstmt->sqloffset + 10);
            if (cstr_length(p1, sqlstmt->offsetlen) == 1) {
                ctx->env->delimiter = *p1;
                sqlstmt->stmt = envcmd;
                return;
            }
        }
    }

    RDBSQLStmtError(RDBSQL_ERR_INVAL_SQL, sqlstmt->sqloffset);
}


/***********************************************************************
 * 
 * RDBSQLStmt Public Api
 * 
 **********************************************************************/

RDBAPI_RESULT RDBSQLStmtCreate (RDBCtx ctx, const char *sql_block, size_t sql_len, RDBSQLStmt *outsqlstmt)
{
    RDBSQLStmt sqlstmt;

    // check sql_block length if sql_len = -1

    if (sql_len == (size_t)(-1)) {
        sql_len = cstr_length(sql_block, RDB_SQLSTMT_SQLBLOCK_MAXLEN + 1);
    }

    if (sql_len < 4) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(rdbsqlstmt.c:%d) RDBSQLErr-%04d: SQL is too short.", __LINE__, RDBSQL_ERR_INVAL_SQL);
        return RDBAPI_ERR_RDBSQL;
    }

    if (sql_len > RDB_SQLSTMT_SQLBLOCK_MAXLEN) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "(rdbsqlstmt.c:%d) RDBSQLErr-%04d: SQL is too long.", __LINE__, RDBSQL_ERR_INVAL_SQL);
        return RDBAPI_ERR_RDBSQL;
    }

    // create stmt object and trim sql unnecessary whilespaces
    //
    sqlstmt = RDBSQLStmtObjectNew(ctx, sql_block, sql_len);
    if (! sqlstmt) {
        return RDBAPI_ERR_NOMEM;
    }

    // use pattern to match sqlstmt
    //
    if (re_match(RDBSQL_PATTERN_UPSERT_INTO, sqlstmt->sqloffset) == 0) {
        SQLStmtParseUpsert(ctx, sqlstmt);
        goto parse_finished;
    }

    if (re_match(RDBSQL_PATTERN_SELECT_FROM, sqlstmt->sqloffset) == 0) {
        SQLStmtParseSelect(ctx, sqlstmt);
        goto parse_finished;
    }

    if (re_match(RDBSQL_PATTERN_DESC_TABLE, sqlstmt->sqloffset) == 0) {
        SQLStmtParseDesc(ctx, sqlstmt);
        goto parse_finished;
    }

    if (re_match(RDBSQL_PATTERN_DELETE_FROM, sqlstmt->sqloffset) == 0) {
        SQLStmtParseDelete(ctx, sqlstmt);
        goto parse_finished;
    }

    if (re_match(RDBSQL_PATTERN_INFO_SECTION, sqlstmt->sqloffset) == 0) {
        SQLStmtParseInfo(ctx, sqlstmt);
        goto parse_finished;
    }

    if (re_match(RDBSQL_PATTERN_SHOW_DATABASES, sqlstmt->sqloffset) == 0) {
        SQLStmtParseShowDatabases(ctx, sqlstmt);
        goto parse_finished;
    }

    if (re_match(RDBSQL_PATTERN_SHOW_TABLES, sqlstmt->sqloffset) == 0) {
        SQLStmtParseShowTables(ctx, sqlstmt);
        goto parse_finished;
    }

    if (re_match(RDBSQL_PATTERN_DROP_TABLE, sqlstmt->sqloffset) == 0) {
        SQLStmtParseDrop(ctx, sqlstmt);
        goto parse_finished;
    }

    if (re_match(RDBSQL_PATTERN_CREATE_TABLE, sqlstmt->sqloffset) == 0) {
        SQLStmtParseCreate(ctx, sqlstmt);
        goto parse_finished;
    }

    if (re_match(RDBSQL_COMMAND_VERBOSE_ON, sqlstmt->sqloffset) == 0) {
        SQLStmtParseCommand(ctx, sqlstmt, RDBENV_COMMAND_VERBOSE_ON);
        goto parse_finished;
    }

    if (re_match(RDBSQL_COMMAND_VERBOSE_OFF, sqlstmt->sqloffset) == 0) {
        SQLStmtParseCommand(ctx, sqlstmt, RDBENV_COMMAND_VERBOSE_OFF);
        goto parse_finished;
    }

    if (re_match(RDBSQL_COMMAND_DELIMITER, sqlstmt->sqloffset) == 0) {
        SQLStmtParseCommand(ctx, sqlstmt, RDBENV_COMMAND_DELIMITER);
        goto parse_finished;
    }

    RDBSQLStmtError(RDBSQL_ERR_INVAL_SQL, sqlstmt->sqloffset);

parse_finished:
    if (sqlstmt->stmt == RDBSQL_INVALID) {
        RDBSQLStmtFree(sqlstmt);
        return RDBAPI_ERR_RDBSQL;
    }

    // all is ok
    *ctx->errmsg = 0;
    *outsqlstmt = sqlstmt;
    return RDBAPI_SUCCESS;
}


void RDBSQLStmtFree (RDBSQLStmt sqlstmt)
{
    if (! sqlstmt) {
        return;
    }

    if (sqlstmt->stmt == RDBSQL_SELECT || sqlstmt->stmt == RDBSQL_DELETE) {
        cstr_varray_free(sqlstmt->select.selectfields, RDBAPI_ARGV_MAXNUM);
        cstr_varray_free(sqlstmt->select.fields, RDBAPI_ARGV_MAXNUM);
        cstr_varray_free(sqlstmt->select.fieldvals, RDBAPI_ARGV_MAXNUM);
    } else if (sqlstmt->stmt == RDBSQL_UPSERT) {
        cstr_varray_free(sqlstmt->upsert.fieldnames, RDBAPI_ARGV_MAXNUM);
        cstr_varray_free(sqlstmt->upsert.fieldvalues, RDBAPI_ARGV_MAXNUM);
        cstr_varray_free(sqlstmt->upsert.updcolnames, RDBAPI_ARGV_MAXNUM);
        cstr_varray_free(sqlstmt->upsert.updcolvalues, RDBAPI_ARGV_MAXNUM);

        RDBSQLStmtFree(sqlstmt->upsert.selectstmt);

        zstringbufFree(&sqlstmt->upsert.prepare.keypattern);
    } else if (sqlstmt->stmt == RDBSQL_CREATE) {
        // TODO:
    }

    RDBMemFree(sqlstmt);
}


RDBSQLStmtType RDBSQLStmtGetSql (RDBSQLStmt sqlstmt, int indent, RDBZString *outsql)
{
    int j;

    RDBSQLStmtType stmt = RDBSQL_INVALID;

    int rowids[RDBAPI_KEYS_MAXNUM + 1] = {0};

    char indents[] = {32, 32, 32, 32,  32, 32, 32, 32, 0};

    RDBCtx ctx = sqlstmt->ctx;
    RDBEnv env = ctx->env;

    zstringbuf sqlbuf = NULL;

    *outsql = NULL;

    if (indent >= 0 && indent < 8) {
        indents[indent] = '\0';
    }

    switch (sqlstmt->stmt) {
    case RDBSQL_SELECT:
        if (sqlstmt->select.numselect == -1) {
            sqlbuf = zstringbufCat(sqlbuf, "%sSELECT *\n", indents);
        } else {
            sqlbuf = zstringbufCat(sqlbuf, "%sSELECT %.*s", indents, sqlstmt->select.selectfieldslen[0], sqlstmt->select.selectfields[0]);

            for (j = 1; j < sqlstmt->select.numselect; j++) {
                sqlbuf = zstringbufCat(sqlbuf, ",\n%s%s%s%.*s", indents, indents, indents, sqlstmt->select.selectfieldslen[j], sqlstmt->select.selectfields[j]);
            }

            sqlbuf = zstringbufCat(sqlbuf, "\n");
        }
        sqlbuf = zstringbufCat(sqlbuf, "%s%sFROM %s.%s", indents, indents, sqlstmt->select.tablespace, sqlstmt->select.tablename);

        if (sqlstmt->select.numwhere) {
            sqlbuf = zstringbufCat(sqlbuf, "\n%s%sWHERE\n", indents, indents);

            sqlbuf = zstringbufCat(sqlbuf, "%s%s%s%s%.*s %s %.*s",
                indents, indents, indents, indents,
                sqlstmt->select.fieldslen[0], sqlstmt->select.fields[0],
                ctx->env->filterexprs[ sqlstmt->select.fieldexprs[0] ],
                sqlstmt->select.fieldvalslen[0], sqlstmt->select.fieldvals[0]);

            for (j = 1; j < sqlstmt->select.numwhere; j++) {
                sqlbuf = zstringbufCat(sqlbuf, "\n%s%s%sAND\n", indents, indents, indents);

                sqlbuf = zstringbufCat(sqlbuf, "%s%s%s%s%.*s %s %.*s",
                    indents, indents, indents, indents,
                    sqlstmt->select.fieldslen[j], sqlstmt->select.fields[j],
                    ctx->env->filterexprs[ sqlstmt->select.fieldexprs[j] ],
                    sqlstmt->select.fieldvalslen[j], sqlstmt->select.fieldvals[j]);
            }
        }

        if (sqlstmt->select.offset) {
            sqlbuf = zstringbufCat(sqlbuf, "\n%s%sOFFSET %"PRIu64, indents, indents, sqlstmt->select.offset);
        }

        if (sqlstmt->select.limit != -1) {
            sqlbuf = zstringbufCat(sqlbuf, "\n%s%sLIMIT %"PRIu64, indents, indents, sqlstmt->select.limit);
        }

        sqlbuf = zstringbufCat(sqlbuf, "\n");
        stmt = sqlstmt->stmt;
        break;

    case RDBSQL_UPSERT:
        sqlbuf = zstringbufCat(sqlbuf, "%sUPSERT INTO %s.%s ", indents, sqlstmt->upsert.tablespace, sqlstmt->upsert.tablename);

        if (sqlstmt->upsert.numfields) {
            sqlbuf = zstringbufCat(sqlbuf, "(\n%s%s%s%.*s", indents, indents, indents, sqlstmt->upsert.fieldnameslen[0], sqlstmt->upsert.fieldnames[0]);

            for (j = 1; j < sqlstmt->upsert.numfields; j++) {
                sqlbuf = zstringbufCat(sqlbuf, ",\n%s%s%s%.*s", indents, indents, indents, sqlstmt->upsert.fieldnameslen[j], sqlstmt->upsert.fieldnames[j]);
            }

            sqlbuf = zstringbufCat(sqlbuf, "\n%s%s)", indents, indents);
        }

        if (*sqlstmt->upsert.fieldvalues) {
            sqlbuf = zstringbufCat(sqlbuf, "\n%s%sVALUES (\n%s%s%s%.*s", indents, indents, indents, indents, indents, sqlstmt->upsert.fieldvalueslen[0], sqlstmt->upsert.fieldvalues[0]);

            for (j = 1; j < sqlstmt->upsert.numfields; j++) {
                sqlbuf = zstringbufCat(sqlbuf, ",\n%s%s%s%.*s", indents, indents, indents, sqlstmt->upsert.fieldvalueslen[j], sqlstmt->upsert.fieldvalues[j]);
            }

            sqlbuf = zstringbufCat(sqlbuf, "\n%s%s)", indents, indents);
        }

        if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_INSERT) {
            sqlbuf = zstringbufCat(sqlbuf, "\n");
        } else if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_IGNORE) {
            sqlbuf = zstringbufCat(sqlbuf, " ON DUPLICATE KEY IGNORE;\n");
        } else if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_UPDATE) {
            sqlbuf = zstringbufCat(sqlbuf, " ON DUPLICATE KEY UPDATE\n");
            sqlbuf = zstringbufCat(sqlbuf, "%s%s%.*s=%.*s", indents, indents,
                sqlstmt->upsert.updcolnameslen[0], sqlstmt->upsert.updcolnames[0],
                sqlstmt->upsert.updcolvalueslen[0], sqlstmt->upsert.updcolvalues[0]);

            for (j = 1; j < sqlstmt->upsert.updcols; j++) {
                sqlbuf = zstringbufCat(sqlbuf, ",\n%s%s%.*s=%.*s", indents, indents,
                    sqlstmt->upsert.updcolnameslen[j], sqlstmt->upsert.updcolnames[j],
                    sqlstmt->upsert.updcolvalueslen[j], sqlstmt->upsert.updcolvalues[j]);
            }
            sqlbuf = zstringbufCat(sqlbuf, "\n");
        } else if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_SELECT) {
            RDBZString selsqlb;
            sqlbuf = zstringbufCat(sqlbuf, "\n", indents);

            if (RDBSQLStmtGetSql(sqlstmt->upsert.selectstmt, indent*2, &selsqlb) == RDBSQL_SELECT) {
                sqlbuf = zstringbufCat(sqlbuf, "%.*s", RDBZSTRLEN(selsqlb), RDBCZSTR(selsqlb));
                RDBZStringFree(selsqlb);
            }
        }
        stmt = sqlstmt->stmt;
        break;

    case RDBSQL_CREATE:
        if (! sqlstmt->create.fail_on_exists) {
            sqlbuf = zstringbufCat(sqlbuf, "%sCREATE TABLE IF NOT EXISTS %s.%s (\n", indents, sqlstmt->create.tablespace, sqlstmt->create.tablename);
        } else {
            sqlbuf = zstringbufCat(sqlbuf, "%sCREATE TABLE %s.%s (\n", indents, sqlstmt->create.tablespace, sqlstmt->create.tablename);
        }

        for (j = 0; j < sqlstmt->create.numfields; j++) {
            const RDBFieldDes_t *fdes = &sqlstmt->create.fielddefs[j];

            if (fdes->rowkey) {
                rowids[fdes->rowkey] = j;
                rowids[0] += 1;
            }

            sqlbuf = zstringbufCat(sqlbuf, "%s%s%-20.*s %s", indents, indents, fdes->namelen, fdes->fieldname,
                RDBCZSTR(env->valtypetable[(ub1)fdes->fieldtype]));

            if (fdes->length) {
                sqlbuf = zstringbufCat(sqlbuf, "(%d", fdes->length);

                if (fdes->dscale) {
                    sqlbuf = zstringbufCat(sqlbuf, ",%d)", fdes->dscale);
                } else {
                    sqlbuf = zstringbufCat(sqlbuf, ")");
                }
            }

            if (! fdes->nullable) {
                sqlbuf = zstringbufCat(sqlbuf, " NOT NULL");
            }

            if (*fdes->comment) {
                sqlbuf = zstringbufCat(sqlbuf, " COMMENT '%s'", fdes->comment);
            }

            sqlbuf = zstringbufCat(sqlbuf, ",\n");
        }

        sqlbuf = zstringbufCat(sqlbuf, "%s%sROWKEY (", indents, indents);

        for (j = 1; j <= rowids[0]; j++) {
            // colindex: 0-based
            int colindex = rowids[j];
            if (j == 1) {
                sqlbuf = zstringbufCat(sqlbuf, "%.*s", sqlstmt->create.fielddefs[colindex].namelen, sqlstmt->create.fielddefs[colindex].fieldname);
            } else {
                sqlbuf = zstringbufCat(sqlbuf, ", %.*s", sqlstmt->create.fielddefs[colindex].namelen, sqlstmt->create.fielddefs[colindex].fieldname);
            }
        }
        sqlbuf = zstringbufCat(sqlbuf, ")");

        if (*sqlstmt->create.tablecomment) {
            sqlbuf = zstringbufCat(sqlbuf, "\n%s) COMMENT '%s'\n", indents, sqlstmt->create.tablecomment);
        } else {
            sqlbuf = zstringbufCat(sqlbuf, "\n%s)\n", indents);
        }
        stmt = sqlstmt->stmt;
        break;

    case RDBSQL_DROP_TABLE:
        sqlbuf = zstringbufCat(sqlbuf, "%sDROP TABLE %s.%s\n", indents, sqlstmt->droptable.tablespace, sqlstmt->droptable.tablename);
        stmt = sqlstmt->stmt;
        break;

    case RDBSQL_DESC_TABLE:
        sqlbuf = zstringbufCat(sqlbuf, "%sDESC %s.%s\n", indents, sqlstmt->desctable.tablespace, sqlstmt->desctable.tablename);
        stmt = sqlstmt->stmt;
        break;
    }

    if (sqlbuf) {
        *outsql = RDBZStringNew(sqlbuf->str, sqlbuf->len);

        zstringbufFree(&sqlbuf);
    }

    return stmt;
}


static int assign_fieldvalue (const char *valstr, int len, const char **ppvalout)
{
    if (len > 1) {
        if (valstr[0] == 39 && valstr[len - 1] == 39) {
            *ppvalout = &valstr[1];
            return len - 2;
        }
    }

    *ppvalout = valstr;
    return len;
}


RDBAPI_RESULT RDBSQLStmtPrepare (RDBSQLStmt sqlstmt)
{
    int i, j, k;

    ub8 tstamp = -1;
    char tmbuf[24] = {0};

    RDBCtx ctx = sqlstmt->ctx;
 
    if (sqlstmt->stmt == RDBSQL_SELECT) {
        if (! strcmp(sqlstmt->select.tablespace, RDB_SYSTEM_TABLE_PREFIX) &&
            ! strcmp(sqlstmt->select.tablename, "dual") &&
            1) {
            sqlstmt->select.dual = 1;
        }
    } else if (sqlstmt->stmt == RDBSQL_DELETE) {
        // TODO:

    } else if (sqlstmt->stmt == RDBSQL_UPSERT) {
        zstringbuf keypattern;

        RDBTableDes_t *tabledes = &sqlstmt->upsert.prepare.tabledes;

        int *fields = sqlstmt->upsert.prepare.fields;
        int *rowids = sqlstmt->upsert.prepare.rowids;

        sqlstmt->upsert.prepare.attfields = 0;
        sqlstmt->upsert.prepare.dupkey = 1;

        bzero(fields, sizeof(sqlstmt->upsert.prepare.fields));
        bzero(rowids, sizeof(sqlstmt->upsert.prepare.rowids));

        bzero(&sqlstmt->upsert.prepare.tabledes, sizeof(sqlstmt->upsert.prepare.tabledes));

        zstringbufFree(&sqlstmt->upsert.prepare.keypattern);
        sqlstmt->upsert.prepare.keypattern = zstringbufNew(RDB_ROWKEY_MAX_SIZE, NULL, 0);

        keypattern = sqlstmt->upsert.prepare.keypattern;

        if (RDBTableDescribe(ctx, sqlstmt->upsert.tablespace, sqlstmt->upsert.tablename, tabledes) != RDBAPI_SUCCESS) {
            return RDBAPI_ERROR;
        }

        for (i = 0; i < sqlstmt->upsert.numfields; i++) {
            char *val = sqlstmt->upsert.fieldvalues[i];
            int vlen = sqlstmt->upsert.fieldvalueslen[i];
            int vpos;

            j = RDBTableDesFieldIndex(tabledes, sqlstmt->upsert.fieldnames[i], sqlstmt->upsert.fieldnameslen[i]);
            if (j == -1) {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "field not found: '%.*s'", sqlstmt->upsert.fieldnameslen[i], sqlstmt->upsert.fieldnames[i]);
                return RDBAPI_ERROR;
            }

            sqlstmt->upsert.fielddesid[i] = j;

            if (fields[j + 1]) {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "duplicated field: '%.*s'", sqlstmt->upsert.fieldnameslen[i], sqlstmt->upsert.fieldnames[i]);
                return RDBAPI_ERROR;
            }

            switch (tabledes->fielddes[j].fieldtype) {
            case RDBVT_DATE:
                vpos = re_match("NOWDATE\\s*(\\s*)", val);
                if (vpos == 0) {
                    if (RDBGetServerTime(ctx, tmbuf) == RDBAPI_ERROR) {
                        return RDBAPI_ERROR;
                    }

                    tmbuf[10] = 0;
                    sqlstmt->upsert.fieldvalueslen[i] = 10;
                    sqlstmt->upsert.fieldvalues[i] = strdup(tmbuf);
                    free(val);
                    break;
                }

                vpos = re_match("TODATE\\s*(\\s*", val);
                if (vpos == 0) {
                    char *p = strchr(val, 40);
                    char *q = strrchr(val, 41);

                    *tmbuf = 0;

                    if (p && q && q > p) {
                        *p++ = 0;
                        *q-- = 0;

                        int lll = q - p;

                        if (cstr_timestamp_to_datetime(p, -1, tmbuf)) {
                            tmbuf[10] = 0;
                            sqlstmt->upsert.fieldvalueslen[i] = 10;
                            sqlstmt->upsert.fieldvalues[i] = strdup(tmbuf);
                            free(val);
                        }
                    }

                    if (! *tmbuf) {
                        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "error TODATE(%s)", val);
                        return RDBAPI_ERROR;
                    }
                }
                break;

            case RDBVT_TIME:
                vpos = re_match("NOWTIME\\s*(\\s*)", val);
                if (vpos == 0) {
                    if (RDBGetServerTime(ctx, tmbuf) == RDBAPI_ERROR) {
                        return RDBAPI_ERROR;
                    }

                    tmbuf[19] = 0;
                    sqlstmt->upsert.fieldvalueslen[i] = 19;
                    sqlstmt->upsert.fieldvalues[i] = strdup(tmbuf);
                    free(val);
                    break;
                }

                vpos = re_match("TOTIME\\s*(\\s*", val);
                if (vpos == 0) {
                    char *p = strchr(val, 40);
                    char *q = strrchr(val, 41);

                    *tmbuf = 0;

                    if (p && q && q > p) {
                        *p++ = 0;
                        *q-- = 0;

                        int lll = q-p;
                        if (cstr_timestamp_to_datetime(p, -1, tmbuf)) {
                            tmbuf[19] = 0;
                            sqlstmt->upsert.fieldvalueslen[i] = 19;
                            sqlstmt->upsert.fieldvalues[i] = strdup(tmbuf);
                            free(val);
                        }
                    }

                    if (! *tmbuf) {
                        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "error TOTIME(%s)", val);
                        return RDBAPI_ERROR;
                    }
                }
                break;

            case RDBVT_STAMP:
                tstamp = (ub8)(-1);

                vpos = re_match("NOWSTAMP\\s*(\\s*)", val);
                if (vpos == 0) {
                    tstamp = RDBGetServerTime(ctx, NULL);
                    if (tstamp == (ub8)(-1)) {
                        return RDBAPI_ERROR;
                    }

                    sqlstmt->upsert.fieldvalueslen[i] = snprintf_chkd_V1(tmbuf, sizeof(tmbuf), "%"PRIu64, tstamp);
                    sqlstmt->upsert.fieldvalues[i] = strdup(tmbuf);
                    free(val);
                    break;
                }

                vpos = re_match("TOSTAMP\\s*(\\s*", val);
                if (vpos == 0) {
                    char *p = strchr(val, 40);
                    char *q = strrchr(val, 41);

                    if (p && q && q > p) {
                        *p++ = 0;
                        *q-- = 0;

                        tstamp = cstr_parse_timestamp(p);
                        if (tstamp != (ub8)(-1)) {
                            sqlstmt->upsert.fieldvalueslen[i] = snprintf_chkd_V1(tmbuf, sizeof(tmbuf), "%"PRIu64, tstamp);
                            sqlstmt->upsert.fieldvalues[i] = strdup(tmbuf);
                            free(val);
                        }
                    }

                    if (tstamp == (ub8)(-1)) {
                        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "error stamp format: %s", val);
                        return RDBAPI_ERROR;
                    }
                }
                break;

            case RDBVT_SET:

                break;
            }

            if (fields[0] < j + 1) {
                fields[0] = j + 1;
            }

            k = tabledes->fielddes[j].rowkey;
            if (k > 0) {
                // < 0: rowkey field
                fields[j + 1] = (i + 1) * (-1);

                rowids[ k ] = i + 1;
                if (rowids[0] < k) {
                    rowids[0] = k;
                }
            } else {
                // > 0: attribute fields
                fields[j + 1] = i + 1;
                sqlstmt->upsert.prepare.attfields++;
            }
        }

        for (i = 0; i < sqlstmt->upsert.updcols; i++) {
            j = RDBTableDesFieldIndex(tabledes, sqlstmt->upsert.updcolnames[i], sqlstmt->upsert.updcolnameslen[i]);
            if (j == -1) {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "field for update not found: '%.*s'", sqlstmt->upsert.updcolnameslen[i], sqlstmt->upsert.updcolnames[i]);
                return RDBAPI_ERROR;
            }

            sqlstmt->upsert.updcoldesid[i] = j;

            if (tabledes->fielddes[j].rowkey) {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "cannot update rowkey field: '%.*s'", sqlstmt->upsert.updcolnameslen[i], sqlstmt->upsert.updcolnames[i]);
                return RDBAPI_ERROR;
            }
        }

        keypattern = zstringbufCat(keypattern, "{%s::%s", sqlstmt->upsert.tablespace, sqlstmt->upsert.tablename);
        for (i = 1; i <= tabledes->rowkeyid[0]; i++) {
            j = rowids[i];

            if (j > 0) {
                keypattern = zstringbufCat(keypattern, ":%.*s", sqlstmt->upsert.fieldvalueslen[j - 1], sqlstmt->upsert.fieldvalues[j - 1]);
            } else {
                sqlstmt->upsert.prepare.dupkey = 0;
                keypattern = zstringbufCat(keypattern, ":*");
            }
        }

        sqlstmt->upsert.prepare.keypattern = zstringbufCat(keypattern, "}");

        if (sqlstmt->upsert.selectstmt) {
            if (RDBSQLStmtPrepare(sqlstmt->upsert.selectstmt) != RDBAPI_SUCCESS) {
                return RDBAPI_ERROR;
            }
        }
    } else if (sqlstmt->stmt == RDBSQL_CREATE) {
        // TODO:


    }

    return RDBAPI_SUCCESS;
}


RDBAPI_RESULT RDBSQLStmtExecute (RDBSQLStmt sqlstmt, RDBResultMap *outResultMap)
{
    RDBAPI_RESULT res;

    int i, j, k;

    int keylen;
    char keybuf[RDB_KEY_VALUE_SIZE];

    RDBResultMap resultmap = NULL;

    RDBCtx ctx = sqlstmt->ctx;
    RDBEnv env = ctx->env;

    *outResultMap = NULL;

    if (sqlstmt->upsert.prepare.tabledes.table_timestamp != RDBTableGetTimestamp(ctx, sqlstmt->upsert.tablespace, sqlstmt->upsert.tablename)) {
        res = RDBSQLStmtPrepare(sqlstmt);
        if (res != RDBAPI_SUCCESS) {
            return res;
        }
    }

    if (sqlstmt->stmt == RDBSQL_SELECT) {
        if (sqlstmt->select.dual) {
            RDBRow row;
            char tmbuf[24];

            RDBResultMapCreate("SUCCESS dual:", sqlstmt->select.selectfields, sqlstmt->select.selectfieldslen, sqlstmt->select.numselect, 0, &resultmap);
            RDBRowNew(resultmap, sqlstmt->select.tablename, 4, &row);
            RDBResultMapInsertRow(resultmap, row);

            for (i = 0; i < sqlstmt->select.numselect; i++) {
                if (! cstr_compare_len(sqlstmt->select.selectfields[i], sqlstmt->select.selectfieldslen[i], "NOWSTAMP()", 10)) {
                    ub8 stampMS = RDBGetServerTime(ctx, NULL);
                    if (stampMS != RDBAPI_ERROR) {
                        keylen = snprintf_chkd_V1(keybuf, RDB_KEY_VALUE_SIZE, "%"PRIu64, stampMS);
                        RDBCellSetString(RDBRowCell(row, i), keybuf, keylen);
                    }
                    continue;
                }

                if (! cstr_compare_len(sqlstmt->select.selectfields[i], sqlstmt->select.selectfieldslen[i], "NOWTIME()", 9)) {
                    ub8 stampMS = RDBGetServerTime(ctx, keybuf);
                    if (stampMS != RDBAPI_ERROR) {
                        RDBCellSetString(RDBRowCell(row, i), keybuf, cstr_length(keybuf, 19));
                    }
                    continue;
                }

                if (! cstr_compare_len(sqlstmt->select.selectfields[i], sqlstmt->select.selectfieldslen[i], "NOWDATE()", 9)) {
                    ub8 stampMS = RDBGetServerTime(ctx, keybuf);
                    if (stampMS != RDBAPI_ERROR) {
                        RDBCellSetString(RDBRowCell(row, i), keybuf, cstr_length(keybuf, 10));
                    }
                    continue;
                }

                if (cstr_startwith(sqlstmt->select.selectfields[i], sqlstmt->select.selectfieldslen[i], "TODATE(", 7)) {
                    keylen = snprintf_chkd_V1(keybuf, RDB_KEY_VALUE_SIZE, "%.*s", sqlstmt->select.selectfieldslen[i] - 8, sqlstmt->select.selectfields[i] + 7);
                    if (cstr_timestamp_to_datetime(keybuf, keylen, tmbuf)) {
                        RDBCellSetString(RDBRowCell(row, i), tmbuf, cstr_length(tmbuf, 10));
                    }
                    continue;
                }

                if (cstr_startwith(sqlstmt->select.selectfields[i], sqlstmt->select.selectfieldslen[i], "TOTIME(", 7)) {
                    keylen = snprintf_chkd_V1(keybuf, RDB_KEY_VALUE_SIZE, "%.*s", sqlstmt->select.selectfieldslen[i] - 8, sqlstmt->select.selectfields[i] + 7);
                    if (cstr_timestamp_to_datetime(keybuf, keylen, tmbuf)) {
                        RDBCellSetString(RDBRowCell(row, i), tmbuf, cstr_length(tmbuf, 19));
                    }
                    continue;
                }

                if (cstr_startwith(sqlstmt->select.selectfields[i], sqlstmt->select.selectfieldslen[i], "TOSTAMP(", 8)) {
                    keylen = snprintf_chkd_V1(keybuf, RDB_KEY_VALUE_SIZE, "%.*s", sqlstmt->select.selectfieldslen[i] - 9, sqlstmt->select.selectfields[i] + 8);

                    ub8 stampMS = cstr_parse_timestamp(keybuf);

                    if (stampMS != (ub8)(-1)) {
                        keylen = snprintf_chkd_V1(keybuf, RDB_KEY_VALUE_SIZE, "%"PRIu64, stampMS);
                        RDBCellSetString(RDBRowCell(row, i), keybuf, keylen);
                    }
                    continue;
                }
            }

            *outResultMap = resultmap;
            return RDBAPI_SUCCESS;
        } else {
            res = RDBTableScanFirst(ctx, sqlstmt, &resultmap);
            if (res == RDBAPI_SUCCESS) {
                ub8 offset = RDBTableScanNext(resultmap, sqlstmt->select.offset, sqlstmt->select.limit);

                if (offset != RDB_ERROR_OFFSET) {
                    if (sqlstmt->stmt == RDBSQL_DELETE) {
                        RDBResultMapDeleteAllOnCluster(resultmap);
                    }

                    *outResultMap = resultmap;
                    return RDBAPI_SUCCESS;
                }

                RDBResultMapDestroy(resultmap);
            }
        }
    } else if (sqlstmt->stmt == RDBSQL_DELETE) {
        res = RDBTableScanFirst(ctx, sqlstmt, &resultmap);
        if (res == RDBAPI_SUCCESS) {
            ub8 offset = RDBTableScanNext(resultmap, sqlstmt->select.offset, sqlstmt->select.limit);

            if (offset != RDB_ERROR_OFFSET) {
                if (sqlstmt->stmt == RDBSQL_DELETE) {
                    RDBResultMapDeleteAllOnCluster(resultmap);
                }

                *outResultMap = resultmap;
                return RDBAPI_SUCCESS;
            }

            RDBResultMapDestroy(resultmap);
        }
    } else if (sqlstmt->stmt == RDBSQL_UPSERT) {
        // UPSERT INTO xsdb.connect (sid, connfd, host) VALUES(1,1, 'localhost') ON DUPLICATE KEY UPDATE port=5899;
        // UPSERT INTO xsdb.connect (sid, connfd, host) VALUES(1,1, 'localhost') ON DUPLICATE KEY IGNORE;
        // UPSERT INTO xsdb.connect (sid, connfd, host) VALUES(1,1, 'localhost');
        // UPSERT INTO xsdb.connect (sid, connfd, host) SELECT * FROM xsdb.connect2 WHERE ...;
        // UPSERT INTO xsdb.connect SELECT * FROM xsdb.connect;

        int argc = 0;
        const char *argv[RDBAPI_ARGV_MAXNUM + 4] = {0};
        size_t argvlen[RDBAPI_ARGV_MAXNUM + 4] = {0};

        const char *colnames[] = {"$rowkey", 0};
        int colnameslen[] = {7, 0};

        zstringbuf keypattern = sqlstmt->upsert.prepare.keypattern;

        if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_INSERT) {
            if (! sqlstmt->upsert.prepare.attfields) {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "upsert no field for key: %.*s", keypattern->len, keypattern->str);
                return RDBAPI_ERROR;                    
            }

            if (sqlstmt->upsert.prepare.dupkey) {
                // HMSET keypattern fld1 val1 fld2 val2 ...
                redisReply *replySet;

                argc = 0;
                argv[argc] = "HMSET";
                argvlen[argc++] = 5;

                argv[argc] = keypattern->str;
                argvlen[argc++] = keypattern->len;

                for (j = 1; j <= sqlstmt->upsert.prepare.fields[0]; j++) {
                    i = sqlstmt->upsert.prepare.fields[j];

                    if (i > 0) {
                        argv[argc] = sqlstmt->upsert.fieldnames[i-1];
                        argvlen[argc] = sqlstmt->upsert.fieldnameslen[i-1];

                        argc++;
                        argvlen[argc] = assign_fieldvalue(sqlstmt->upsert.fieldvalues[i-1], sqlstmt->upsert.fieldvalueslen[i-1], &argv[argc]);

                        argc++;
                    }
                }

                replySet = RedisExecCommandArgv(ctx, argc, argv, argvlen);

                if (RedisCheckReplyStatus(replySet, "OK", 2)) {
                    RDBRow row;

                    RDBResultMapCreate("SUCCESS results", colnames, colnameslen, 1, 0, &resultmap);
                    RDBRowNew(resultmap, keypattern->str, keypattern->len, &row);
                    RDBCellSetString(RDBRowCell(row, 0), keypattern->str, keypattern->len);
                    RDBResultMapInsertRow(resultmap, row);

                    RedisFreeReplyObject(&replySet);

                    *outResultMap = resultmap;
                    return RDBAPI_SUCCESS;
                }

                // error
                RedisFreeReplyObject(&replySet);
            } else {
                int nodeindex = 0;
                int haserror = 0;

                // result cursor state
                RDBTableCursor_t *nodestates = RDBMemAlloc(sizeof(RDBTableCursor_t) * RDBEnvNumNodes(ctx->env));

                RDBResultMapCreate("SUCCESS results", colnames, colnameslen, 1, 0, &resultmap);

                while (! haserror && nodeindex < RDBEnvNumNodes(ctx->env)) {
                    RDBEnvNode envnode = RDBEnvGetNode(ctx->env, nodeindex);

                    // scan only on master node
                    if (RDBEnvNodeGetMaster(envnode, NULL) == RDBAPI_TRUE) {
                        redisReply *replyRows = NULL;
                        RDBCtxNode ctxnode = RDBCtxGetNode(ctx, nodeindex);

                        RDBTableCursor nodestate = &nodestates[nodeindex];
                        if (nodestate->finished) {
                            // goto next node since this current node has finished
                            nodeindex++;
                            continue;
                        }

                        res = RDBTableScanOnNode(ctxnode, nodestate, keypattern->str, keypattern->len, 200, &replyRows);
                        if (res == RDBAPI_SUCCESS) {
                            // here we should update rows
                            for (i = 0; ! haserror && i != replyRows->elements; i++) {
                                // get row first
                                redisReply *replyGet;

                                argc = 0;

                                argv[argc] = "HMGET";
                                argvlen[argc++] = 5;

                                // rowkey
                                argv[argc] = replyRows->element[i]->str;
                                argvlen[argc++] = replyRows->element[i]->len;

                                for (j = 1; j <= sqlstmt->upsert.prepare.fields[0]; j++) {
                                    k = sqlstmt->upsert.prepare.fields[j];
                                    if (k > 0) {
                                        argv[argc] = sqlstmt->upsert.fieldnames[k-1];
                                        argvlen[argc++] = sqlstmt->upsert.fieldnameslen[k-1];
                                    }
                                }

                                replyGet = RedisExecCommandArgv(ctx, argc, argv, argvlen);

                                if (replyGet && replyGet->type == REDIS_REPLY_ARRAY && replyGet->elements == sqlstmt->upsert.prepare.attfields) {
                                    redisReply *replySet;

                                    argc = 0;

                                    argv[argc] = "HMSET";
                                    argvlen[argc++] = 5;

                                    argv[argc] = replyRows->element[i]->str;
                                    argvlen[argc++] = replyRows->element[i]->len;

                                    for (j = 1; j <= sqlstmt->upsert.prepare.fields[0]; j++) {
                                        k = sqlstmt->upsert.prepare.fields[j];
                                        if (k > 0) {
                                            argv[argc] = sqlstmt->upsert.fieldnames[k - 1];
                                            argvlen[argc] = sqlstmt->upsert.fieldnameslen[k - 1];

                                            argc++;
                                            argvlen[argc] = assign_fieldvalue(sqlstmt->upsert.fieldvalues[i-1], sqlstmt->upsert.fieldvalueslen[i-1], &argv[argc]);

                                            argc++;
                                        }
                                    }

                                    replySet = RedisExecCommandArgv(ctx, argc, argv, argvlen);

                                    if (RedisCheckReplyStatus(replySet, "OK", 2)) {
                                        RDBRow row;
                                        RDBRowNew(resultmap, replyRows->element[i]->str, replyRows->element[i]->len, &row);
                                        RDBCellSetString(RDBRowCell(row, 0), replyRows->element[i]->str, replyRows->element[i]->len);

                                        // TODO: overflow?
                                        RDBResultMapInsertRow(resultmap, row);
                                    } else {
                                        haserror = 1;
                                    }

                                    RedisFreeReplyObject(&replySet);
                                }

                                RedisFreeReplyObject(&replyGet);
                            }
                        }

                        RedisFreeReplyObject(&replyRows);
                    } else {
                        nodeindex++;
                    }
                }

                RDBMemFree(nodestates);

                if (! haserror) {
                    *outResultMap = resultmap;
                    return RDBAPI_SUCCESS;
                }

                RDBResultMapDestroy(resultmap);
            }
        } else if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_IGNORE) {
            if (! sqlstmt->upsert.prepare.attfields) {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "upsert no field for key: %.*s", keypattern->len, keypattern->str);
                return RDBAPI_ERROR;                    
            }

            if (sqlstmt->upsert.prepare.dupkey) {
                if (RedisExistsKey(ctx, keypattern->str, keypattern->len) == 0) {
                    // key not existed
                    redisReply *replySet;

                    argc = 0;
                    argv[argc] = "HMSET";
                    argvlen[argc++] = 5;

                    argv[argc] = keypattern->str;
                    argvlen[argc++] = keypattern->len;

                    for (j = 1; j <= sqlstmt->upsert.prepare.fields[0]; j++) {
                        i = sqlstmt->upsert.prepare.fields[j];

                        if (i > 0) {
                            argv[argc] = sqlstmt->upsert.fieldnames[i-1];
                            argvlen[argc] = sqlstmt->upsert.fieldnameslen[i-1];

                            argc++;
                            argvlen[argc] = assign_fieldvalue(sqlstmt->upsert.fieldvalues[i-1], sqlstmt->upsert.fieldvalueslen[i-1], &argv[argc]);

                            argc++;
                        }
                    }

                    replySet = RedisExecCommandArgv(ctx, argc, argv, argvlen);

                    if (RedisCheckReplyStatus(replySet, "OK", 2)) {
                        RDBRow row;

                        RDBResultMapCreate("SUCCESS results", colnames, colnameslen, 1, 0, &resultmap);
                        RDBRowNew(resultmap, keypattern->str, keypattern->len, &row);
                        RDBCellSetString(RDBRowCell(row, 0), keypattern->str, keypattern->len);
                        RDBResultMapInsertRow(resultmap, row);

                        RedisFreeReplyObject(&replySet);

                        *outResultMap = resultmap;
                        return RDBAPI_SUCCESS;
                    }

                    // error
                    RedisFreeReplyObject(&replySet);
                } else {
                    snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "upsert on duplicate key ignored: %.*s", keypattern->len, keypattern->str);
                }
            } else {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "not all fields of rowkey assigned: %.*s", keypattern->len, keypattern->str);
            }
        } else if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_UPDATE) {
            if (sqlstmt->upsert.prepare.dupkey) {
                redisReply *replyGet = NULL;
                redisReply *replySet = NULL;

                res = RedisExistsKey(ctx, keypattern->str, keypattern->len);

                if (res == 1) {
                    // duplicate key update
                    argc = 0;
                    argv[argc] = "HMGET";
                    argvlen[argc] = 5;
                    argc++;

                    argv[argc] = keypattern->str;
                    argvlen[argc] = keypattern->len;
                    argc++;

                    for (i = 0; i < sqlstmt->upsert.updcols; i++) {
                        argv[argc] = sqlstmt->upsert.updcolnames[i];
                        argvlen[argc] = sqlstmt->upsert.updcolnameslen[i];
                        argc++;
                    }

                    replyGet = RedisExecCommandArgv(ctx, argc, argv, argvlen);

                    if (replyGet && replyGet->type == REDIS_REPLY_ARRAY && replyGet->elements == sqlstmt->upsert.updcols) {
                        int refcols = 0;

                        // update existed key by updcols
                        argc = 0;
                        argv[argc] = "HMSET";
                        argvlen[argc] = 5;
                        argc++;

                        argv[argc] = keypattern->str;
                        argvlen[argc] = keypattern->len;
                        argc++;

                        for (i = 0; i < sqlstmt->upsert.updcols; i++) {
                            j = sqlstmt->upsert.updcoldesid[i];

                            if (sqlstmt->upsert.prepare.tabledes.fielddes[j].fieldtype != RDBVT_SET) {
                                argv[argc] = sqlstmt->upsert.updcolnames[i];
                                argvlen[argc] = sqlstmt->upsert.updcolnameslen[i];
                                argc++;

                                argvlen[argc] = assign_fieldvalue(sqlstmt->upsert.updcolvalues[i], sqlstmt->upsert.updcolvalueslen[i], &argv[argc]);
                                argc++;
                            } else {
                                refcols++;
                            }
                        }

                        if (argc > 2) {
                            replySet = RedisExecCommandArgv(ctx, argc, argv, argvlen);

                            if (RedisCheckReplyStatus(replySet, "OK", 2)) {
                                RDBRow row;

                                RDBResultMapCreate("SUCCESS results", colnames, colnameslen, 1, 0, &resultmap);
                                RDBRowNew(resultmap, keypattern->str, keypattern->len, &row);
                                RDBCellSetString(RDBRowCell(row, 0), keypattern->str, keypattern->len);
                                RDBResultMapInsertRow(resultmap, row);

                                RedisFreeReplyObject(&replyGet);
                                RedisFreeReplyObject(&replySet);

                                //*outResultMap = resultmap;
                                //return RDBAPI_SUCCESS;
                            }
                        }

                        if (refcols > 0) {
                            for (i = 0; i < sqlstmt->upsert.updcols; i++) {
                                j = sqlstmt->upsert.updcoldesid[i];

                                if (sqlstmt->upsert.prepare.tabledes.fielddes[j].fieldtype == RDBVT_SET) {
                                    // SET Commands:
                                    //   https://www.tutorialspoint.com/redis/redis_sets.htm
                                    // $SADD key member1 member2 ...
                                    int membs = 0;
                                    char *members[RDBAPI_ARGV_MAXNUM] = {0};
                                    int memberslen[RDBAPI_ARGV_MAXNUM] = {0};

                                    const char *name = sqlstmt->upsert.updcolnames[i];
                                    int namelen = sqlstmt->upsert.updcolnameslen[i];

                                    const char *value = sqlstmt->upsert.updcolvalues[i];
                                    int valuelen = sqlstmt->upsert.updcolvalueslen[i];

                                    zstringbuf skey = zstringbufNew(keypattern->len + namelen + 2, keypattern->str, keypattern->len);

                                    skey = zstringbufCat(skey, "$%.*s", namelen, name);

                                    if (value[0] == '{' && value[valuelen - 1] == '}') {
                                        // SADD {sydb::sessionid:1}$fieldname colval1 ...

                                        // DEL key first
                                        RedisDeleteKey(ctx, skey->str, skey->len, NULL, 0);

                                        argc = 0;
                                        argv[argc] = "SADD";
                                        argvlen[argc] = 4;
                                        argc++;

                                        argv[argc] = skey->str;
                                        argvlen[argc] = skey->len;
                                        argc++;

                                        membs = cstr_slpit_chr(&value[1], valuelen - 2, ctx->env->delimiter, members, memberslen, RDBAPI_ARGV_MAXNUM);

                                        for (k = 0; k < membs; k++) {
                                            if (memberslen[k] > 0 && memberslen[k] <= RDB_ROWKEY_MAX_SIZE) {
                                                const char *mstr;
                                                int mlen = assign_fieldvalue(members[k], memberslen[k], &mstr);
                                                if (mlen > 0) {
                                                    argv[argc] = mstr;
                                                    argvlen[argc] = mlen;
                                                    argc++;
                                                }
                                            }
                                        }

                                        if (argc > 2) {
                                            replySet = RedisExecCommandArgv(ctx, argc, argv, argvlen);
                                        }

                                        // TODO:
                                    } else {
                                        membs = cstr_split_multi_chrs(value, valuelen, "+-*/", 4, members, memberslen, RDBAPI_ARGV_MAXNUM);
                                        if (membs == 1 && cstr_compare_len(members[0], memberslen[0], name, namelen)) {
                                            if (! cstr_compare_len(members[0], memberslen[0], "NULL", 4) ||
                                                ! cstr_compare_len(members[0], memberslen[0], "(null)", 6)) {
                                                // DEL key
                                                RedisDeleteKey(ctx, skey->str, skey->len, NULL, 0);
                                            }
                                        } else if (membs > 1 && ! cstr_compare_len(members[0], memberslen[0], name, namelen)) {
                                            // add into SET
                                            argc = 0;
                                            argv[argc] = "SADD";
                                            argvlen[argc] = 4;
                                            argc++;

                                            argv[argc] = skey->str;
                                            argvlen[argc] = skey->len;
                                            argc++;

                                            for (k = 1; k < membs; k++) {
                                                if (*members[k] == '+') {
                                                    int mstart = 1;
                                                    int mend = memberslen[k] - 1;

                                                    int mlen = cstr_shrink_whitespace(members[k], &mstart, &mend);
                                                    if (mlen > 0) {
                                                        const char *mstr;

                                                        mlen = assign_fieldvalue(members[k] + mstart, mlen, &mstr);
                                                        if (mlen > 0) {
                                                            argv[argc] = mstr;
                                                            argvlen[argc] = mlen;
                                                            argc++;
                                                        }
                                                    }
                                                }
                                            }

                                            if (argc > 2) {
                                                replySet = RedisExecCommandArgv(ctx, argc, argv, argvlen);
                                            }

                                            // remove from SET
                                            argc = 0;
                                            argv[argc] = "SREM";
                                            argvlen[argc] = 4;
                                            argc++;

                                            argv[argc] = skey->str;
                                            argvlen[argc] = skey->len;
                                            argc++;

                                            for (k = 1; k < membs; k++) {
                                                if (*members[k] == '-') {
                                                    int mstart = 1;
                                                    int mend = memberslen[k] - 1;

                                                    int mlen = cstr_shrink_whitespace(members[k], &mstart, &mend);
                                                    if (mlen > 0) {
                                                        const char *mstr;

                                                        mlen = assign_fieldvalue(members[k] + mstart, mlen, &mstr);
                                                        if (mlen > 0) {
                                                            argv[argc] = mstr;
                                                            argvlen[argc] = mlen;
                                                            argc++;
                                                        }
                                                    }
                                                }
                                            }

                                            if (argc > 2) {
                                                replySet = RedisExecCommandArgv(ctx, argc, argv, argvlen);
                                            }
                                        }
                                    }
                                }
                            }
                        }                        
                    }
                } else if (res == 0) {
                    // not found key then add new
                    if (! sqlstmt->upsert.prepare.attfields) {
                        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "upsert no field for key: %.*s", keypattern->len, keypattern->str);
                        return RDBAPI_ERROR;                    
                    }

                    argc = 0;
                    argv[argc] = "HMSET";
                    argvlen[argc++] = 5;

                    argv[argc] = keypattern->str;
                    argvlen[argc++] = keypattern->len;

                    for (j = 1; j <= sqlstmt->upsert.prepare.fields[0]; j++) {
                        i = sqlstmt->upsert.prepare.fields[j];

                        if (i > 0) {
                            argv[argc] = sqlstmt->upsert.fieldnames[i-1];
                            argvlen[argc] = sqlstmt->upsert.fieldnameslen[i-1];

                            argc++;
                            argvlen[argc] = assign_fieldvalue(sqlstmt->upsert.fieldvalues[i-1], sqlstmt->upsert.fieldvalueslen[i-1], &argv[argc]);

                            argc++;
                        }
                    }

                    replySet = RedisExecCommandArgv(ctx, argc, argv, argvlen);

                    if (RedisCheckReplyStatus(replySet, "OK", 2)) {
                        RDBRow row;

                        RDBResultMapCreate("SUCCESS results", colnames, colnameslen, 1, 0, &resultmap);
                        RDBRowNew(resultmap, keypattern->str, keypattern->len, &row);
                        RDBCellSetString(RDBRowCell(row, 0), keypattern->str, keypattern->len);
                        RDBResultMapInsertRow(resultmap, row);

                        RedisFreeReplyObject(&replyGet);
                        RedisFreeReplyObject(&replySet);

                        *outResultMap = resultmap;
                        return RDBAPI_SUCCESS;
                    }
                }
            } else {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "not all fields of rowkey assigned: %.*s", keypattern->len, keypattern->str);
            }
        } else if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_SELECT) {
            printf("(%s:%d) TODO: RDBSQL_UPSERT_MODE_SELECT\n", __FILE__, __LINE__);

            for (i = 0; i < sqlstmt->upsert.numfields; i++) {
                printf("(%s)\n", sqlstmt->upsert.fieldnames[i]);
            }

/*
            RDBResultMap selresultmap;

            res = RDBTableScanFirst(ctx, sqlstmt->upsert.selectstmt, &selresultmap);
            if (res == RDBAPI_SUCCESS) {
                ub8 offset = RDBTableScanNext(selresultmap, sqlstmt->upsert.selectstmt->select.offset, sqlstmt->upsert.selectstmt->select.limit);

                if (offset != RDB_ERROR_OFFSET) {

                    return RDBAPI_SUCCESS;
                }

                RDBResultMapDestroy(selresultmap);
            }
*/
        }

        return RDBAPI_ERROR;

    } else if (sqlstmt->stmt == RDBSQL_CREATE) {
        // CREATE TABLE IF NOT EXISTS xsdb.test22(uid UB8 NOT NULL COMMENT 'user id', str STR(30) , ROWKEY(uid)) COMMENT 'test table';
        RDBTableDes_t tabledes = {0};
        res = RDBTableDescribe(ctx, sqlstmt->create.tablespace, sqlstmt->create.tablename, &tabledes);

        if (res == RDBAPI_SUCCESS && sqlstmt->create.fail_on_exists) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERROR: table already existed");
            return RDBAPI_ERROR;
        }

        res = RDBTableCreate(ctx, sqlstmt->create.tablespace, sqlstmt->create.tablename, sqlstmt->create.tablecomment, sqlstmt->create.numfields, sqlstmt->create.fielddefs);
        if (res == RDBAPI_SUCCESS) {
            bzero(&tabledes, sizeof(tabledes));

            res = RDBTableDescribe(ctx, sqlstmt->create.tablespace, sqlstmt->create.tablename, &tabledes);
            if (res == RDBAPI_SUCCESS && tabledes.nfields == sqlstmt->create.numfields) {
                resultmap = ResultMapBuildDescTable(sqlstmt->create.tablespace, sqlstmt->create.tablename, env->valtypetable, &tabledes);

                *outResultMap = resultmap;
                return RDBAPI_SUCCESS;
            }
        }
    } else if (sqlstmt->stmt == RDBSQL_DESC_TABLE) {
        RDBTableDes_t tabledes = {0};
        if (RDBTableDescribe(ctx, sqlstmt->desctable.tablespace, sqlstmt->desctable.tablename, &tabledes) != RDBAPI_SUCCESS) {
            return RDBAPI_ERROR;
        }

        resultmap = ResultMapBuildDescTable(sqlstmt->desctable.tablespace, sqlstmt->desctable.tablename, env->valtypetable, &tabledes);
        *outResultMap = resultmap;
        return RDBAPI_SUCCESS;
    } else if (sqlstmt->stmt == RDBSQL_DROP_TABLE) {
        keylen = snprintf_chkd_V1(keybuf, sizeof(keybuf), "{%s::%s:%s}", RDB_SYSTEM_TABLE_PREFIX, sqlstmt->droptable.tablespace, sqlstmt->droptable.tablename);

        if (RedisDeleteKey(ctx, keybuf, keylen, NULL, 0) == RDBAPI_KEY_DELETED) {
            snprintf_chkd_V1(keybuf, sizeof(keybuf), "SUCCESS: table '%s.%s' drop okay.", sqlstmt->droptable.tablespace, sqlstmt->droptable.tablename);
        } else {
            snprintf_chkd_V1(keybuf, sizeof(keybuf), "FAILED: table '%s.%s' not found !", sqlstmt->droptable.tablespace, sqlstmt->droptable.tablename);
        }

        RDBResultMapCreate(keybuf, NULL, NULL, 0, 0, &resultmap);

        *outResultMap = resultmap;
        return RDBAPI_SUCCESS;
    } else if (sqlstmt->stmt == RDBSQL_INFO_SECTION) {
        if (RDBCtxCheckInfo(ctx, sqlstmt->info.section, sqlstmt->info.whichnode) == RDBAPI_SUCCESS) {
            const char *names[] = {"nodeid", "host:port", "section", "name", "value", 0};
            int nameslen[] = {6, 9, 7, 4, 5, 0};

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

            int startInfoSecs = sqlstmt->info.section;
            int nposInfoSecs = sqlstmt->info.section + 1;
            if (sqlstmt->info.section == MAX_NODEINFO_SECTIONS) {
                startInfoSecs = NODEINFO_SERVER;
                nposInfoSecs = MAX_NODEINFO_SECTIONS;
            }

            RDBResultMapCreate("SUCCESS:", names, nameslen, 5, 0, &resultmap);

            threadlock_lock(&ctx->env->thrlock);

            do {
                int section;
                int nodeindex = 0;
                int endNodeid = RDBEnvNumNodes(ctx->env);

                if (sqlstmt->info.whichnode) {
                    endNodeid = sqlstmt->info.whichnode;                    
                    nodeindex = endNodeid - 1;                    
                }

                for (; nodeindex < endNodeid; nodeindex++) {
                    RDBRow noderow = NULL;

                    RDBCtxNode ctxnode = RDBCtxGetNode(ctx, nodeindex);
                    RDBEnvNode envnode = RDBCtxNodeGetEnvNode(ctxnode);

                    for (section = startInfoSecs; section != nposInfoSecs; section++) {
                        RDBPropNode propnode;
                        RDBPropMap propmap = envnode->nodeinfo[section];

                        for (propnode = propmap; propnode != NULL; propnode = propnode->hh.next) {
                            RDBRow row = NULL;
                            RDBRowNew(resultmap, NULL, 0, &row);

                            keylen = snprintf_chkd_V1(keybuf, sizeof(keybuf), "%d", envnode->index + 1);
                            RDBCellSetString(RDBRowCell(row, 0), keybuf, keylen);

                            RDBCellSetString(RDBRowCell(row, 1), envnode->key, -1);
                            RDBCellSetString(RDBRowCell(row, 2), sections[section], -1);
                            RDBCellSetString(RDBRowCell(row, 3), propnode->name, -1);
                            RDBCellSetString(RDBRowCell(row, 4), propnode->value, -1);

                            RDBResultMapInsertRow(resultmap, row);
                        }
                    }
                }
            } while(0);

            threadlock_unlock(&ctx->env->thrlock);

            *outResultMap = resultmap;
            return RDBAPI_SUCCESS;
        }
    } else if (sqlstmt->stmt == RDBSQL_SHOW_DATABASES) {
        const char *names[] = {"database", 0};

        redisReply *replyRows;
        RDBBlob_t pattern = {0};

        // result cursor state
        RDBTableCursor_t *nodestates = RDBMemAlloc(sizeof(RDBTableCursor_t) * RDBEnvNumNodes(ctx->env));

        int nodeindex = 0;
        int prefixlen = (int) strlen(RDB_SYSTEM_TABLE_PREFIX) + 3;

        pattern.maxsz = prefixlen + 8;
        pattern.str = RDBMemAlloc(pattern.maxsz);
        pattern.length = snprintf_chkd_V1(pattern.str, pattern.maxsz, "{%s::*:*}", RDB_SYSTEM_TABLE_PREFIX);

        RDBResultMapCreate("DATABASES:", names, NULL, 1, 0, &resultmap);

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

                res = RDBTableScanOnNode(ctxnode, nodestate, pattern.str, pattern.length, 200, &replyRows);

                if (res == RDBAPI_SUCCESS) {
                    // here we should add new rows
                    size_t i = 0;

                    for (; i != replyRows->elements; i++) {
                        redisReply * reply = replyRows->element[i];

                        char *end = cstr_Lfind_chr(reply->str + prefixlen, (int)(reply->len - prefixlen), ':');
                        if (end == cstr_Rfind_chr(reply->str, (int)reply->len, ':')) {
                            RDBRow row = NULL;
                            const char *dbkey;

                            RDBRowNew(resultmap, reply->str + prefixlen, (int)(end - reply->str - prefixlen), &row);

                            dbkey = RDBRowGetKey(row, &keylen);

                            RDBCellSetString(RDBRowCell(row, 0), dbkey, keylen);

                            if (RDBResultMapInsertRow(resultmap, row) != RDBAPI_SUCCESS) {
                                RDBRowFree(row);
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

        *outResultMap = resultmap;
        return RDBAPI_SUCCESS;
    } else if (sqlstmt->stmt == RDBSQL_SHOW_TABLES) {
        const char *names[] = {"database.tablename", 0};

        // {redisdb::database:$tablename}
        redisReply *replyRows;

        RDBBlob_t pattern = {0};

        // result cursor state
        RDBTableCursor_t *nodestates = RDBMemAlloc(sizeof(RDBTableCursor_t) * RDBEnvNumNodes(ctx->env));

        int nodeindex = 0;
        int dbnlen = cstr_length(sqlstmt->showtables.tablespace, RDB_KEY_NAME_MAXLEN);
        int prefixlen = (int) strlen(RDB_SYSTEM_TABLE_PREFIX) + 3;

        pattern.maxsz = prefixlen + dbnlen + 8;
        pattern.str = RDBMemAlloc(pattern.maxsz);
        pattern.length = snprintf_chkd_V1(pattern.str, pattern.maxsz, "{%s::%.*s:*}", RDB_SYSTEM_TABLE_PREFIX, dbnlen, sqlstmt->showtables.tablespace);

        RDBResultMapCreate("TABLES:", names, NULL, 1, 0, &resultmap);

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

                res = RDBTableScanOnNode(ctxnode, nodestate, pattern.str, pattern.length, 200, &replyRows);

                if (res == RDBAPI_SUCCESS) {
                    // here we should add new rows
                    size_t i = 0;

                    for (; i != replyRows->elements; i++) {
                        redisReply * reply = replyRows->element[i];

                        if (! strchr(reply->str + prefixlen + dbnlen + 1, ':')) {
                            RDBRow row = NULL;
                            const char *tbkey;

                            reply->str[prefixlen + dbnlen] = '.';

                            RDBRowNew(resultmap, reply->str + prefixlen, (int)(reply->len - prefixlen - 1), &row);

                            tbkey = RDBRowGetKey(row, &keylen);

                            RDBCellSetString(RDBRowCell(row, 0), tbkey, keylen);

                            if (RDBResultMapInsertRow(resultmap, row) != RDBAPI_SUCCESS) {
                                RDBRowFree(row);
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

        *outResultMap = resultmap;

        return RDBAPI_SUCCESS;
    }

    return RDBAPI_ERROR;
}


RDBAPI_RESULT RDBCtxExecuteSql (RDBCtx ctx, RDBZString sqlstr, RDBResultMap *outResultMap)
{
    RDBResultMap resultmap = NULL;
    RDBSQLStmt sqlstmt = NULL;

    *outResultMap = NULL;

    if (RDBSQLStmtCreate(ctx, RDBCZSTR(sqlstr), RDBZSTRLEN(sqlstr), &sqlstmt) != RDBAPI_SUCCESS) {
        goto ret_error;
    }

    if (ctx->env->verbose) {
        int nch = 0;
        RDBZString sqlout = NULL;

        fprintf(stdout, "# VERBOSE ON (INDENT=%d);\n", RDB_PRINT_LINE_INDENT);

        if (RDBSQLStmtGetSql(sqlstmt, RDB_PRINT_LINE_INDENT, &sqlout) != RDBSQL_INVALID) {
            fprintf(stdout, "%.*s", RDBZSTRLEN(sqlout), RDBCZSTR(sqlout));
            RDBZStringFree(sqlout);
            while (nch++ < RDB_PRINT_LINE_INDENT) {
                fprintf(stdout, " ");
            }
            fprintf(stdout, ";\n");
        }

        fflush(stdout);
    }

    if (sqlstmt->stmt < RDBENV_COMMAND_START) {
        if (RDBSQLStmtExecute(sqlstmt, &resultmap) == RDBAPI_ERROR) {
            goto ret_error;
        }
    }

    // success
    RDBSQLStmtFree(sqlstmt);

    *outResultMap = resultmap;
    return RDBAPI_SUCCESS;

ret_error:
    RDBSQLStmtFree(sqlstmt);
    return RDBAPI_ERROR;
}


RDBAPI_RESULT RDBCtxExecuteFile (RDBCtx ctx, const char *scriptfile, RDBResultMap *outResultMap)
{
    FILE * fp;

    if ((fp = fopen(scriptfile, "r")) != NULL) {
        int len;
        char line[4096];

        int num = 1;
        char key[30];
        int keylen;

        RDBResultMap hResultMap = NULL;
        const char *colnames[] = {"results", 0};

        RDBResultMapCreate(scriptfile, colnames, NULL, 1, 0, &hResultMap);

        RDBBlob_t sqlblob;
        sqlblob.maxsz = sizeof(line) * 2;
        sqlblob.length = 0;
        sqlblob.str = RDBMemAlloc(sqlblob.maxsz);

        while ((len = cstr_readline(fp, line, sizeof(line) - 1, 0)) != -1) {
            if (len > 0 && line[0] != '#') {
                char *endp = strrchr(line, ';');
                if (endp) {
                    *endp = 0;
                    len = (int) (endp - line);
                }

                memcpy(sqlblob.str + sqlblob.length, line, len);
                sqlblob.length += len;

                if (sqlblob.maxsz - sqlblob.length < sizeof(line)) {
                    sqlblob.str = RDBMemRealloc(sqlblob.str, sqlblob.maxsz, sqlblob.maxsz + sizeof(line) * 2);
                    sqlblob.maxsz += sizeof(line) * 2;
                }

                if (endp) {
                    RDBResultMap resultmap = NULL;
                    RDBZString sqlstr = RDBZStringNew(sqlblob.str, sqlblob.length);

                    //$hmget ...

                    if (RDBCtxExecuteSql(ctx, sqlstr, &resultmap) == RDBAPI_SUCCESS) {
                        RDBRow row = NULL;
                        keylen = snprintf_chkd_V1(key, sizeof(key), "resultmap(%d)", num++);
                        RDBRowNew(hResultMap, key, keylen, &row);
                        RDBCellSetResult(RDBRowCell(row, 0), resultmap);
                        RDBResultMapInsertRow(hResultMap, row);
                    }

                    RDBZStringFree(sqlstr);

                    sqlblob.length = 0;
                    bzero(sqlblob.str, sqlblob.maxsz);
                }
            }
        }

        if (sqlblob.length > 0) {
            RDBResultMap resultmap = NULL;
            RDBZString sqlstr = RDBZStringNew(sqlblob.str, sqlblob.length);

            if (RDBCtxExecuteSql(ctx, sqlstr, &resultmap) == RDBAPI_SUCCESS) {
                RDBRow row = NULL;
                keylen = snprintf_chkd_V1(key, sizeof(key), "resultmap(%d)", num++);
                RDBRowNew(hResultMap, key, keylen, &row);
                RDBCellSetResult(RDBRowCell(row, 0), resultmap);
                RDBResultMapInsertRow(hResultMap, row);
            }

            RDBZStringFree(sqlstr);
        }

        RDBMemFree(sqlblob.str);

        fclose(fp);

        *outResultMap = hResultMap;
        return RDBAPI_SUCCESS;
    }

    snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "ERROR: failed open file: %s", scriptfile);
    return RDBAPI_ERROR;
}
