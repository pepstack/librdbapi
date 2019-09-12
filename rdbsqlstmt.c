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
 * rdbsqlstmt.c
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#include "rdbsqlstmt.h"


static RDBResultMap ResultMapBuildDescTable (const char *tablespace, const char *tablename, const char **vtnames, RDBTableDes_t *tabledes)
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
        "datetime",
        "timestamp",
        "comment",
        "fields",
        0
    };

    int tblnameslen[] = {10, 9, 8, 9, 7, 6, 0};

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
    res = RDBResultMapCreate(buf, tblnames, tblnameslen, 6, &tablemap);
    if (res != RDBAPI_SUCCESS) {
        fprintf(stderr, "(%s:%d) RDBResultMapCreate('%s') failed", __FILE__, __LINE__, buf);
        exit(EXIT_FAILURE);
    }

    res = RDBResultMapCreate("=>fields", fldnames, fldnameslen, 7, &fieldsmap);
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
    RDBCellSetString(RDBRowCell(tablerow, 2), tabledes->table_datetime, -1);

    len = snprintf_chkd_V1(buf, sizeof(buf), "%"PRIu64"", tabledes->table_timestamp);

    RDBCellSetString(RDBRowCell(tablerow, 3), buf, len);
    RDBCellSetString(RDBRowCell(tablerow, 4), tabledes->table_comment, -1);
    RDBCellSetResult(RDBRowCell(tablerow, 5), fieldsmap);

    RDBResultMapInsertRow(tablemap, tablerow);

    for (j = 0;  j < tabledes->nfields; j++) {
        RDBRow fieldrow;
        RDBFieldDes_t *fldes = &tabledes->fielddes[j];

        len = snprintf_chkd_V1(buf, sizeof(buf), "{%s::%s:%.*s}", tablespace, tablename, fldes->namelen, fldes->fieldname);

        RDBRowNew(fieldsmap, buf, len, &fieldrow);

        RDBCellSetString(RDBRowCell(fieldrow, 0), fldes->fieldname, fldes->namelen);
        RDBCellSetString(RDBRowCell(fieldrow, 1), vtnames[fldes->fieldtype], -1);

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
    }

    // not found
    return 0;
}


static int parse_select_fields (char *fields, char *fieldnames[], int fieldnameslen[])
{
    char namebuf[RDB_KEY_NAME_MAXLEN * 2 + 1];

    char *p;
    int namelen;
    int i = 0;

    if (fields[0] == '*') {
        // all fields
        return -1;
    }

    p = strtok(fields, ",");
    while (p && i <= RDBAPI_ARGV_MAXNUM) {
        char *name;

        snprintf_chkd_V1(namebuf, RDB_KEY_NAME_MAXLEN * 2, "%s", p);

        name = cstr_LRtrim_chr(namebuf, 32);
        namelen = RDBSQLNameValidateMaxLen(name, RDB_KEY_NAME_MAXLEN);

        if (! namelen) {
            namelen = cstr_length(name, RDB_KEY_NAME_MAXLEN);

            if (cstr_startwith(name, namelen, "COUNT(", 6) ||
                cstr_startwith(name, namelen, "SUM(", 4) ||
                cstr_startwith(name, namelen, "AVG(", 4)
            ) {
                name = cstr_trim_whitespace(name);
                namelen = cstr_length(name, RDB_KEY_NAME_MAXLEN);
            }

            if (! cstr_compare_len(name, namelen, "COUNT(*)", 8) ||
                ! cstr_compare_len(name, namelen, "COUNT(1)", 8)
            ) {
                fieldnames[i] = strdup("COUNT(*)");
                fieldnameslen[i] = namelen;
                return 1;
            }

            //?? TODO:

            return 0;
        }

        fieldnames[i] = strdup(name);
        fieldnameslen[i] = namelen;

        i++;

        p = strtok(0, ",");
    }

    return i;
}


static int parse_field_values (char *fields, char *fieldvalues[RDBAPI_ARGV_MAXNUM])
{
    char valbuf[RDB_SQLSTMT_SQLBLOCK_MAXLEN + 1];

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


static char * ParseWhereFieldName (const char *name, int *namelen)
{
    char *namestr;
    char tmpbuf[RDB_KEY_NAME_MAXLEN * 2];
    snprintf_chkd_V1(tmpbuf, sizeof(tmpbuf), "%s", name);
    namestr = cstr_LRtrim_chr(tmpbuf, 32);
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
    valstr = cstr_LRtrim_chr(cstr_LRtrim_chr(tmpbuf, 32), 39);

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
    char *sql, int sqlen, const char **vtnames,
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

            len = cstr_slpit_chr(sqlc, len, 44, rowkeys, RDBAPI_SQL_KEYS_MAX + 1);
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

    snprintf_chkd_V1(buf, bufsz, "%.*s", np, sqlc);

    for (j = 0; j < 256; j++) {
        if (vtnames[j] && ! strcmp(buf, vtnames[j])) {
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


// UPSERT INTO xsdb.logentry (uid, entrykey, sid, filemd5) VALUES (1, 'fc8397137a6bc16d38f383c524460b87', 1, 'haha') ON DUPLICATE KEY UPDATE status=999;
// UPSERT INTO xsdb.logentry (uid, entrykey, sid, filemd5) VALUES (1, 'fc8397137a6bc16d38f383c524460b8a', 1, 'hahahaha') ON DUPLICATE KEY IGNORE;
// UPSERT INTO xsdb.logentry (uid, entrykey, sid, filemd5) VALUES (1, 'fc8397137a6bc16d38f383c524460b8b', 1, 'haha');
//
static int check_upsert_fields (RDBSQLStmt sqlstmt)
{
/*
    int i, j, fieldnamelen, offlen;
    const char *fieldname, *fieldvalue;

    int argc = 2;

    int *rowkeys = &sqlstmt->upsert.rowkeys[0];

    RDBCtx ctx = sqlstmt->ctx;

    for (i = 0; i < sqlstmt->upsert.numfields; i++) {
        const RDBFieldDes_t *fldes = NULL;

        fieldname = sqlstmt->upsert.fieldnames[i];
        fieldnamelen = cstr_length(fieldname, -1);

        fieldvalue = sqlstmt->upsert.fieldvalues[i];

        for (j = 0; j < sqlstmt->upsert.tabledes.nfields; j++) {
            fldes = &sqlstmt->upsert.tabledes.fielddes[j];

            if (! cstr_compare_len(fldes->fieldname, fldes->namelen, fieldname, fieldnamelen)) {
                // success
                sqlstmt->upsert.fieldindex[i] = j;

                if (fldes->rowkey) {
                    if (rowkeys[0] < RDBAPI_KEYS_MAXNUM) {
                        if (rowkeys[fldes->rowkey]) {
                            // error
                            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: duplicated field: '%.*s'", fieldnamelen, fieldname);
                            return 0;
                        }
                        rowkeys[fldes->rowkey] = i + 1;
                        rowkeys[0] += 1;
                    } else {
                        // error
                        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: too many field: '%.*s'", fieldnamelen, fieldname);
                        return 0;
                    }
                }

                // find ok here
                fldes = NULL;
                break;
            }
        }

        if (fldes) {
            // error
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: undefined field: '%.*s'", fieldnamelen, fieldname);
            return 0;
        }

        fldes = &sqlstmt->upsert.tabledes.fielddes[j];
        if (! fldes->rowkey) {
            // only non rowkey field can be updated!
            sqlstmt->upsert.argvNew[argc] = fieldname;
            sqlstmt->upsert.argvlenNew[argc] = fieldnamelen;

            if (fieldvalue[0] == 39) {
                sqlstmt->upsert.argvNew[argc + 1] = &fieldvalue[1];
                sqlstmt->upsert.argvlenNew[argc + 1] = cstr_length(fieldvalue, -1) - 2;
            } else {
                sqlstmt->upsert.argvNew[argc + 1] = fieldvalue;
                sqlstmt->upsert.argvlenNew[argc + 1] = cstr_length(fieldvalue, -1);
            }

            argc += 2;
        }
    }

    sqlstmt->upsert.rkpattern.maxsz = RDBAPI_SQL_PATTERN_SIZE;
    sqlstmt->upsert.rkpattern.length = 0;
    sqlstmt->upsert.rkpattern.str = RDBMemAlloc(sqlstmt->upsert.rkpattern.maxsz);

    sqlstmt->upsert.rkpattern.length = snprintf_chkd_V1(sqlstmt->upsert.rkpattern.str, sqlstmt->upsert.rkpattern.maxsz,
                "{%s::%s", sqlstmt->upsert.tablespace,  sqlstmt->upsert.tablename);

    for (i = 1; i <= sqlstmt->upsert.rowkeyid[0]; i++) {
        const char *fldval = NULL;

        j = rowkeys[i];
        if (j) {
            fldval = sqlstmt->upsert.fieldvalues[j-1];

            offlen = cstr_length(fldval, RDBAPI_SQL_PATTERN_SIZE);
            if (offlen == RDBAPI_SQL_PATTERN_SIZE) {
                // error
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: too long value: '%.*s'", offlen, sqlstmt->upsert.fieldvalues[j-1]);
                return 0;
            }

            if (fldval[0] == 39) {
                fldval++;
                offlen -= 2;
            }
        } else {
            // key field not given
            offlen = 1;
        }

        if (sqlstmt->upsert.rkpattern.length + offlen > sqlstmt->upsert.rkpattern.maxsz - 4) {
            sqlstmt->upsert.rkpattern.str = RDBMemRealloc(sqlstmt->upsert.rkpattern.str, sqlstmt->upsert.rkpattern.maxsz, sqlstmt->upsert.rkpattern.maxsz + RDBAPI_SQL_PATTERN_SIZE);
            if (! sqlstmt->upsert.rkpattern.str || sqlstmt->upsert.rkpattern.maxsz > RDBAPI_SQL_PATTERN_SIZE * 4) {
                // error
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: no memory for too many values");
                return 0;
            }
            sqlstmt->upsert.rkpattern.maxsz += RDBAPI_SQL_PATTERN_SIZE;
        }

        if (fldval) {
            offlen = snprintf_chkd_V1(sqlstmt->upsert.rkpattern.str + sqlstmt->upsert.rkpattern.length, sqlstmt->upsert.rkpattern.maxsz - sqlstmt->upsert.rkpattern.length,
                        ":%.*s", offlen, fldval);
        } else {
            offlen = snprintf_chkd_V1(sqlstmt->upsert.rkpattern.str + sqlstmt->upsert.rkpattern.length, sqlstmt->upsert.rkpattern.maxsz - sqlstmt->upsert.rkpattern.length,
                        ":*");
        }
        sqlstmt->upsert.rkpattern.length += offlen;
    }

    sqlstmt->upsert.rkpattern.length += snprintf_chkd_V1(sqlstmt->upsert.rkpattern.str + sqlstmt->upsert.rkpattern.length, sqlstmt->upsert.rkpattern.maxsz - sqlstmt->upsert.rkpattern.length, "}");

    if (sqlstmt->upsert.rowkeys[0] == sqlstmt->upsert.rowkeyid[0]) {
        sqlstmt->upsert.argvNew[0] = sqlstmt->upsert.hmset_cmd;
        sqlstmt->upsert.argvlenNew[0] = 5;
        sqlstmt->upsert.argcNew = argc;
    }
*/
    // success
    return 1;
}


static int check_upsert_updates (RDBSQLStmt sqlstmt)
{
/*
    int i, j, fieldnamelen, fieldvaluelen, ok;
    const char *fieldname, *fieldvalue;
    const RDBFieldDes_t *fldes;

    RDBCtx ctx = sqlstmt->ctx;

    for (i = 0; i < sqlstmt->upsert.numupdates; i++) {
        fieldname = sqlstmt->upsert.updatenames[i];
        fieldnamelen = cstr_length(fieldname, -1);

        fieldvalue = sqlstmt->upsert.updatevalues[i];
        fieldvaluelen = cstr_length(fieldvalue, -1);

        ok = -1;

        for (j = 0; j < sqlstmt->upsert.tabledes.nfields; j++) {
            fldes = &sqlstmt->upsert.tabledes.fielddes[j];

            if (! cstr_compare_len(fldes->fieldname, fldes->namelen, fieldname, fieldnamelen)) {
                // success
                ok = fldes->rowkey;
                sqlstmt->upsert.updateindex[i] = j;
                break;
            }
        }

        if (ok < 0) {
            // error
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: undefined update field: '%.*s'", fieldnamelen, fieldname);
            return 0;
        }
        if (ok > 0) {
            // error
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLError: update rowkey field: '%.*s'", fieldnamelen, fieldname);
            return 0;
        }

        sqlstmt->upsert.argvUpd[i*2 + 2] = fieldname;
        sqlstmt->upsert.argvUpd[i*2 + 3] = fieldvalue;

        sqlstmt->upsert.argvlenUpd[i*2 + 2] = fieldnamelen;
        sqlstmt->upsert.argvlenUpd[i*2 + 3] = fieldvaluelen;
    }

    // hmset rkey f1 v1 f2 v2 ...
    sqlstmt->upsert.argvUpd[0] = sqlstmt->upsert.hmset_cmd;
    sqlstmt->upsert.argvlenUpd[0] = 5;

    sqlstmt->upsert.argcUpd = sqlstmt->upsert.numupdates * 2 + 2;
*/
    // success
    return 1;
}


// UPSERT INTO xsdb.test(name,id) VALUES('foo', 123);
// UPSERT INTO xsdb.test(id, counter) VALUES(123, 0) ON DUPLICATE KEY UPDATE id=1,counter = counter + 1;
// UPSERT INTO xsdb.test(id, my_col) VALUES(123, 0) ON DUPLICATE KEY IGNORE;
//
// UPSERT INTO xsdb.connect(sid, connfd, host) VALUES(1,1,'127.0.0.1') ON DUPLICATE KEY UPDATE sid=666, addr='888';
// UPSERT INTO xsdb.connect(sid, connfd, host) VALUES(1,1,'127.0.0.1') ON DUPLICATE KEY UPDATE sid='888';
// TODO: UPSERT INTO db.tbl (...) SELECT ... FROM ...
// TODO: UPSERT INTO db.tbl SELECT * FROM ...
// TODO: UPSERT INTO db.tbl (f1, f2) SELECT f1,f2 FROM ...
//
// UPSERT INTO xsdb.connect (sid, connfd, host) VALUES(1,1, 'localhost');
// UPSERT INTO xsdb.connect (sid, connfd, host) SELECT * FROM xsdb.connect;
//
void SQLStmtParseUpsert (RDBCtx ctx, RDBSQLStmt sqlstmt)
{
    int i, np, len;

    char *startp, *endp;

    int numfields = 0;
    int numvalues = 0;
    int update = 0;

    // pass UPSERT_
    char *sqlc = cstr_Ltrim_whitespace(sqlstmt->sqloffset + 7);

    // pass INTO_
    sqlc = cstr_Ltrim_whitespace(sqlc + 5);

    // find '('
    startp = strchr(sqlc, 40);
    if (! startp) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: tablename not assigned. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
        return;
    }
    *startp++ = 0;

    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, -1));
    if (! parse_table(sqlc, sqlstmt->upsert.tablespace, sqlstmt->upsert.tablename)) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: illegal tablename. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
        return;
    }

    // fieldnames
    sqlc = cstr_Ltrim_whitespace(startp);
    endp = strchr(sqlc, 41);
    if (! endp) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: non closure char. errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
        return;
    }
    *endp++ = 0;

    len = cstr_Rtrim_whitespace(sqlc, cstr_length(sqlc, -1));
    numfields = cstr_slpit_chr(sqlc, len, 44, sqlstmt->upsert.fieldnames, RDBAPI_ARGV_MAXNUM + 1);
    if (numfields == 0) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: fields not found");
        return;
    }
    if (numfields > RDBAPI_ARGV_MAXNUM) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: too many fields");
        return;
    }
    for(i = 0; i < numfields; i++) {
        sqlstmt->upsert.fieldnameslen[i] = cstr_length(sqlstmt->upsert.fieldnames[i], RDB_KEY_NAME_MAXLEN + 1);
        if (sqlstmt->upsert.fieldnameslen[i] > RDB_KEY_NAME_MAXLEN) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: too long fieldname: %s", sqlstmt->upsert.fieldnames[i]);
            return;
        }
    }

    sqlstmt->upsert.numfields = numfields;

    // VALUES(...)
    sqlc = cstr_Ltrim_whitespace(endp);
    np = re_match("VALUES[\\s]*(", sqlc);
    if (np != 0) {
        if (re_match(RDBSQL_PATTERN_SELECT_FROM, sqlc) == 0) {
            if (RDBSQLStmtCreate(ctx, sqlc, -1, &sqlstmt->upsert.selectstmt) == RDBAPI_SUCCESS) {
                SQLStmtParseSelect(ctx, sqlstmt->upsert.selectstmt);

                if (sqlstmt->upsert.selectstmt->stmt == RDBSQL_INVALID) {
                    snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "SQLStmtError: UPSERT INTO... SELECT.... errat(%d): '%s'", (int)(sqlc - sqlstmt->sqlblock), sqlc);
                    return;
                }

                sqlstmt->upsert.upsertmode = RDBSQL_UPSERT_MODE_SELECT;

                // success
                sqlstmt->stmt = RDBSQL_UPSERT;
                return;
            }
        }

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
            sqlnext, nextlen, (const char **) ctx->env->valtypenames,
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

    sqlstmt = (RDBSQLStmt) RDBMemAlloc(sizeof(RDBSQLStmt_t) + sql_len + 1);

    sqlstmt->ctx = ctx;

    memcpy(sqlstmt->sqlblock, sql_block, sql_len);
    sqlstmt->sqlblocksize = (ub4) (sql_len + 1);

    sql_len = cstr_Rtrim_whitespace(sqlstmt->sqlblock, (int) sql_len);

    cstr_Rtrim_chr(sqlstmt->sqlblock, ';');

    sqlstmt->sqloffset = cstr_Ltrim_whitespace(sqlstmt->sqlblock);
    sqlstmt->offsetlen = cstr_Rtrim_whitespace(sqlstmt->sqloffset, cstr_length(sqlstmt->sqloffset, sql_len));

    // use pattern to match sqlstmt

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
    } else if (sqlstmt->stmt == RDBSQL_CREATE) {
        // TODO:
    }

    RDBMemFree(sqlstmt);
}


RDBSQLStmtType RDBSQLStmtGetType (RDBSQLStmt sqlstmt, char **parsedClause, int pretty)
{
    return sqlstmt->stmt;
}


void RDBSQLStmtPrint (RDBSQLStmt sqlstmt, FILE *fout)
{
    int j;
    int rowids[RDBAPI_KEYS_MAXNUM + 1] = {0};

    RDBCtx ctx = sqlstmt->ctx;

    static const char *rdbsql_exprs[] = {
        NULL
        ,"="
        ,"LLIKE"
        ,"RLIKE"
        ,"LIKE"
        ,"MATCH"
        ,"!="
        ,">"
        ,"<"
        ,">="
        ,"<="
        ,NULL
    };

    switch (sqlstmt->stmt) {
    case RDBSQL_SELECT:
        if (sqlstmt->select.numselect == -1) {
            fprintf(fout, "  SELECT *\n");
        } else {
            fprintf(fout, "  SELECT %.*s", sqlstmt->select.selectfieldslen[0], sqlstmt->select.selectfields[0]);
            for (j = 1; j < sqlstmt->select.numselect; j++) {
                fprintf(fout, ",\n      %.*s", sqlstmt->select.selectfieldslen[j], sqlstmt->select.selectfields[j]);
            }
            fprintf(fout, "\n");
        }
        fprintf(fout, "    FROM %s.%s", sqlstmt->select.tablespace, sqlstmt->select.tablename);

        if (sqlstmt->select.numwhere) {
            fprintf(fout, "\n    WHERE\n");

            fprintf(fout, "        %.*s %s %.*s",
                sqlstmt->select.fieldslen[0], sqlstmt->select.fields[0],
                rdbsql_exprs[ sqlstmt->select.fieldexprs[0] ],
                sqlstmt->select.fieldvalslen[0], sqlstmt->select.fieldvals[0]);

            for (j = 1; j < sqlstmt->select.numwhere; j++) {
                fprintf(fout, "\n      AND\n");

                fprintf(fout, "        %.*s %s %.*s",
                    sqlstmt->select.fieldslen[j], sqlstmt->select.fields[j],
                    rdbsql_exprs[ sqlstmt->select.fieldexprs[j] ],
                    sqlstmt->select.fieldvalslen[j], sqlstmt->select.fieldvals[j]);
            }
        }

        if (sqlstmt->select.offset) {
            fprintf(fout, "\n    OFFSET %"PRIu64, sqlstmt->select.offset);
        }

        if (sqlstmt->select.limit != -1) {
            fprintf(fout, "\n    LIMIT %"PRIu64, sqlstmt->select.limit);
        }

        fprintf(fout, "\n");
        break;

    case RDBSQL_UPSERT:
        fprintf(fout, "  UPSERT INTO %s.%s ", sqlstmt->upsert.tablespace, sqlstmt->upsert.tablename);

        if (sqlstmt->upsert.numfields) {
            fprintf(fout, "(\n      %.*s", sqlstmt->upsert.fieldnameslen[0], sqlstmt->upsert.fieldnames[0]);

            for (j = 1; j < sqlstmt->upsert.numfields; j++) {
                fprintf(fout, ",\n      %.*s", sqlstmt->upsert.fieldnameslen[j], sqlstmt->upsert.fieldnames[j]);
            }
        }

        if (*sqlstmt->upsert.fieldvalues) {
            fprintf(fout, "\n    VALUES (\n      %.*s", sqlstmt->upsert.fieldvalueslen[0], sqlstmt->upsert.fieldvalues[0]);

            for (j = 1; j < sqlstmt->upsert.numfields; j++) {
                fprintf(fout, ",\n      %.*s", sqlstmt->upsert.fieldvalueslen[j], sqlstmt->upsert.fieldvalues[j]);
            }
        }
        fprintf(fout, "\n    )");

        if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_INSERT) {
            fprintf(fout, "\n");
        } else if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_IGNORE) {
            fprintf(fout, " ON DUPLICATE KEY IGNORE;\n");
        } else if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_UPDATE) {
            fprintf(fout, " ON DUPLICATE KEY UPDATE\n");
            fprintf(fout, "    %.*s=%.*s",
                sqlstmt->upsert.updcolnameslen[0], sqlstmt->upsert.updcolnames[0],
                sqlstmt->upsert.updcolvalueslen[0], sqlstmt->upsert.updcolvalues[0]);

            for (j = 1; j < sqlstmt->upsert.updcols; j++) {
                fprintf(fout, ",\n    %.*s=%.*s",
                    sqlstmt->upsert.updcolnameslen[j], sqlstmt->upsert.updcolnames[j],
                    sqlstmt->upsert.updcolvalueslen[j], sqlstmt->upsert.updcolvalues[j]);
            }
            fprintf(fout, "\n");
        } else if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_SELECT) {
            fprintf(fout, "  (\n");
            RDBSQLStmtPrint(sqlstmt->upsert.selectstmt, fout);
            fprintf(fout, "  )\n");
        }
        break;

    case RDBSQL_CREATE:
        if (! sqlstmt->create.fail_on_exists) {
            fprintf(fout, "  CREATE TABLE IF NOT EXISTS %s.%s (\n", sqlstmt->create.tablespace, sqlstmt->create.tablename);
        } else {
            fprintf(fout, "  CREATE TABLE %s.%s (\n", sqlstmt->create.tablespace, sqlstmt->create.tablename);
        }

        for (j = 0; j < sqlstmt->create.numfields; j++) {
            const RDBFieldDes_t *fdes = &sqlstmt->create.fielddefs[j];

            if (fdes->rowkey) {
                rowids[fdes->rowkey] = j;
                rowids[0] += 1;
            }

            fprintf(fout, "    %-20.*s"
                " %s",
                fdes->namelen, fdes->fieldname,
                ctx->env->valtypenames[fdes->fieldtype]);

            if (fdes->length) {
                fprintf(fout, "(%d", fdes->length);

                if (fdes->dscale) {
                    fprintf(fout, ",%d)", fdes->dscale);
                } else {
                    fprintf(fout, ")");
                }
            }

            if (! fdes->nullable) {
                fprintf(fout, " NOT NULL");
            }

            if (*fdes->comment) {
                fprintf(fout, " COMMENT '%s'", fdes->comment);
            }

            fprintf(fout, ",\n");
        }

        fprintf(fout, "    ROWKEY (");
        for (j = 1; j <= rowids[0]; j++) {
            // colindex: 0-based
            int colindex = rowids[j];

            if (j == 1) {
                fprintf(fout, "%.*s",
                    sqlstmt->create.fielddefs[colindex].namelen,
                    sqlstmt->create.fielddefs[colindex].fieldname);
            } else {
                fprintf(fout, ", %.*s",
                    sqlstmt->create.fielddefs[colindex].namelen,
                    sqlstmt->create.fielddefs[colindex].fieldname);
            }            
        }
        fprintf(fout, ")");

        if (*sqlstmt->create.tablecomment) {
            fprintf(fout, "\n  ) COMMENT '%s'\n", sqlstmt->create.tablecomment);
        } else {
            fprintf(fout, "\n  )\n");
        }
        break;

    case RDBSQL_DROP_TABLE:
        fprintf(fout, "  DROP TABLE %s.%s\n", sqlstmt->droptable.tablespace, sqlstmt->droptable.tablename);
        break;

    case RDBSQL_DESC_TABLE:
        fprintf(fout, "  DESC %s.%s\n", sqlstmt->desctable.tablespace, sqlstmt->desctable.tablename);
        break;

    default:
        // TODO:
        break;
    }
}


RDBAPI_RESULT RDBSQLStmtExecute (RDBSQLStmt sqlstmt, RDBResultMap *outResultMap)
{
    RDBAPI_RESULT res;

    int keylen;
    char keybuf[RDB_KEY_VALUE_SIZE];

    RDBResultMap resultmap = NULL;

    RDBCtx ctx = sqlstmt->ctx;
    const char **vtnames = (const char **) ctx->env->valtypenames;

    *outResultMap = NULL;

    if (sqlstmt->stmt == RDBSQL_SELECT || sqlstmt->stmt == RDBSQL_DELETE) {
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
        /*
        // TODO:
        ub8 existedkeys = 0;
        ub8 updatedkeys = 0;

        int nodeindex = 0;

        // result cursor state
        RDBTableCursor_t *nodestates = RDBMemAlloc(sizeof(RDBTableCursor_t) * RDBEnvNumNodes(ctx->env));

        // @@@
        //??RDBResultMapNew(ctx, NULL, sqlstmt->stmt, NULL, NULL, 0, NULL, NULL, &resultMap);

        while (nodeindex < RDBEnvNumNodes(ctx->env)) {
            RDBEnvNode envnode = RDBEnvGetNode(ctx->env, nodeindex);

            // scan only on master node
            if (RDBEnvNodeGetMaster(envnode, NULL) == RDBAPI_TRUE) {
                redisReply *replyRows;

                RDBCtxNode ctxnode = RDBCtxGetNode(ctx, nodeindex);

                RDBTableCursor nodestate = &nodestates[nodeindex];
                if (nodestate->finished) {
                    // goto next node since this current node has finished
                    nodeindex++;
                    continue;
                }

               //?? res = RDBTableScanOnNode(ctxnode, nodestate, sqlstmt->upsert.rkpattern.str, sqlstmt->upsert.rkpattern.length, 200, &replyRows);

                if (res == RDBAPI_SUCCESS) {
                    // here we should add new rows
                    size_t i = 0;

                    for (; i != replyRows->elements; i++) {
                        redisReply * replyUpdate = NULL;
                        redisReply * reply = replyRows->element[i];

                        existedkeys++;

                        if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_UPDATE) {

                            // with ON DUPLICATE KEY UPDATE field1=value1, field2=value2, ...
                            sqlstmt->upsert.argvUpd[1] = reply->str;
                            sqlstmt->upsert.argvlenUpd[1] = reply->len;

                            // here we found existed key, then set its value use below command:
                            //   "hmset key field1 value1 field2 value2"
                            replyUpdate = RedisExecCommandArgv(ctx, sqlstmt->upsert.argcUpd, sqlstmt->upsert.argvUpd, sqlstmt->upsert.argvlenUpd);

                            if (! RedisCheckReplyStatus(replyUpdate, "OK", 2)) {
                                printf("(%s:%d) TODO: fail to update key: %.*s - (%s) \n", __FILE__, __LINE__, (int)reply->len, reply->str, ctx->errmsg);
                            } else {
                                updatedkeys++;
                            }
                        } else if (sqlstmt->upsert.upsertmode == RDBSQL_UPSERT_MODE_INSERT) {
                            // without ON DUPLICATE KEY ...
                            sqlstmt->upsert.argvNew[1] = reply->str;
                            sqlstmt->upsert.argvlenNew[1] = reply->len;

                            replyUpdate = RedisExecCommandArgv(ctx, sqlstmt->upsert.argcNew, sqlstmt->upsert.argvNew, sqlstmt->upsert.argvlenNew);

                            if (! RedisCheckReplyStatus(replyUpdate, "OK", 2)) {
                                printf("(%s:%d) TODO: fail to update key: %.*s - (%s) \n", __FILE__, __LINE__, (int)reply->len, reply->str, ctx->errmsg);
                            } else {
                                updatedkeys++;
                            }
                        }

                        RedisFreeReplyObject(&replyUpdate);
                    }

                    RedisFreeReplyObject(&replyRows);
                }
            } else {
                nodeindex++;
            }
        }

        RDBMemFree(nodestates);

        if (! existedkeys) {
            // no matter what ON DUPLICATE KEY ...
            if (sqlstmt->upsert.argcNew) {
                redisReply * replyNew;

                // rowkey must be full patternized
                sqlstmt->upsert.argvNew[1] = sqlstmt->upsert.rkpattern.str;
                sqlstmt->upsert.argvlenNew[1] = sqlstmt->upsert.rkpattern.length;

                replyNew = RedisExecCommandArgv(ctx, sqlstmt->upsert.argcNew, sqlstmt->upsert.argvNew, sqlstmt->upsert.argvlenNew);
                if (! RedisCheckReplyStatus(replyNew, "OK", 2)) {
                    printf("(%s:%d) TODO: fail to insert key: %.*s - (%s)\n",
                        __FILE__, __LINE__,
                        sqlstmt->upsert.rkpattern.length, sqlstmt->upsert.rkpattern.str,
                        ctx->errmsg);
                } else {
                    printf("upsert 1 new key OK: %.*s.\n", sqlstmt->upsert.rkpattern.length, sqlstmt->upsert.rkpattern.str);
                }

                RedisFreeReplyObject(&replyNew);
            } else {
                printf("(%s:%d) TODO: warning insert key not valid: %.*s\n", __FILE__, __LINE__, sqlstmt->upsert.rkpattern.length, sqlstmt->upsert.rkpattern.str);
            }
        } else {
            printf("upsert %"PRIu64" existed keys OK.\n", updatedkeys);
        }
*/
        *outResultMap = resultmap;
        return RDBAPI_SUCCESS;

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
                resultmap = ResultMapBuildDescTable(sqlstmt->create.tablespace, sqlstmt->create.tablename, vtnames, &tabledes);

                *outResultMap = resultmap;
                return RDBAPI_SUCCESS;
            }
        }
    } else if (sqlstmt->stmt == RDBSQL_DESC_TABLE) {
        RDBTableDes_t tabledes = {0};
        if (RDBTableDescribe(ctx, sqlstmt->desctable.tablespace, sqlstmt->desctable.tablename, &tabledes) != RDBAPI_SUCCESS) {
            return RDBAPI_ERROR;
        }

        resultmap = ResultMapBuildDescTable(sqlstmt->desctable.tablespace, sqlstmt->desctable.tablename, vtnames, &tabledes);
        *outResultMap = resultmap;
        return RDBAPI_SUCCESS;
    } else if (sqlstmt->stmt == RDBSQL_DROP_TABLE) {
        keylen = snprintf_chkd_V1(keybuf, sizeof(keybuf), "{%s::%s:%s}", RDB_SYSTEM_TABLE_PREFIX, sqlstmt->droptable.tablespace, sqlstmt->droptable.tablename);

        if (RedisDeleteKey(ctx, keybuf, keylen, NULL, 0) == RDBAPI_KEY_DELETED) {
            snprintf_chkd_V1(keybuf, sizeof(keybuf), "SUCCESS: table '%s.%s' drop okay.", sqlstmt->droptable.tablespace, sqlstmt->droptable.tablename);
        } else {
            snprintf_chkd_V1(keybuf, sizeof(keybuf), "FAILED: table '%s.%s' not found !", sqlstmt->droptable.tablespace, sqlstmt->droptable.tablename);
        }

        RDBResultMapCreate(keybuf, NULL, NULL, 0, &resultmap);

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

            RDBResultMapCreate("SUCCESS:", names, nameslen, 5, &resultmap);

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

        RDBResultMapCreate("DATABASES:", names, NULL, 1, &resultmap);

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

        RDBResultMapCreate("TABLES:", names, NULL, 1, &resultmap);

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
        fprintf(stdout, "# VERBOSE ON\n");
        RDBSQLStmtPrint(sqlstmt, stdout);
        fprintf(stdout, "  ;\n");
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

        RDBResultMapCreate(scriptfile, colnames, NULL, 1, &hResultMap);

        RDBBlob_t sqlblob;
        sqlblob.maxsz = sizeof(line) * 2;
        sqlblob.length = 0;
        sqlblob.str = RDBMemAlloc(sqlblob.maxsz);

        while ((len = cstr_readline(fp, line, sizeof(line) - 1)) != -1) {
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
