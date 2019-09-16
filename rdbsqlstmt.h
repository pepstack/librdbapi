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
 * rdbsqlstmt.h
 *   rdb sqlstmt
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#ifndef RDBSQLSTMT_H_INCLUDED
#define RDBSQLSTMT_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#include "rdbresultmap.h"


#define RDBSQL_PATTERN_SELECT_FROM     "SELECT[\\s]+"
#define RDBSQL_PATTERN_DELETE_FROM     "DELETE[\\s]+FROM[\\s]+"
#define RDBSQL_PATTERN_UPSERT_INTO     "UPSERT[\\s]+INTO[\\s]+"
#define RDBSQL_PATTERN_CREATE_TABLE    "CREATE[\\s]+TABLE[\\s]+"
#define RDBSQL_PATTERN_DESC_TABLE      "DESC[\\s]+"
#define RDBSQL_PATTERN_INFO_SECTION    "INFO[\\W]*"
#define RDBSQL_PATTERN_SHOW_DATABASES  "SHOW[\\s]+DATABASES[\\s]*$"
#define RDBSQL_PATTERN_SHOW_TABLES     "SHOW[\\s]+TABLES[\\s]+"
#define RDBSQL_PATTERN_DROP_TABLE      "DROP[\\s]+TABLE[\\s]+"

#define RDBSQL_COMMAND_VERBOSE_ON      "VERBOSE[\\s]+ON[\\s]*$"
#define RDBSQL_COMMAND_VERBOSE_OFF     "VERBOSE[\\s]+OFF[\\s]*$"
#define RDBSQL_COMMAND_DELIMITER       "DELIMITER[\\s]+"


/**
 * RDBSQLErr-101: invalid table name. (:123) - 'DESC ;'
 */
#define RDBSQL_ERR_FATAL_APP        0x0FF

#define RDBSQL_ERR_INVAL_SQL        0x101
#define RDBSQL_ERR_INVAL_TABLE      0x102
#define RDBSQL_ERR_INVAL_DBNAME     0x103
#define RDBSQL_ERR_ILLEGAL_CHAR     0x104
#define RDBSQL_ERR_TOO_LONG         0x105
#define RDBSQL_ERR_TOO_SHORT        0x106


#define RDBSQL_UPSERT_MODE_INSERT     0
#define RDBSQL_UPSERT_MODE_IGNORE     1
#define RDBSQL_UPSERT_MODE_UPDATE     2
#define RDBSQL_UPSERT_MODE_SELECT     3


#define RDBSQL_FUNC_NONE              0
#define RDBSQL_FUNC_COUNT             1


typedef struct _RDBSQLStmt_t
{
    RDBCtx ctx;

    RDBSQLStmtType stmt;

    // 0: none
    // 1: COUNT(*)
    int sqlfunc;

    union {
        // SELECT and DELETE
        struct SELECT {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];

            // WHERE fields
            int numwhere;
            char *fields[RDBAPI_ARGV_MAXNUM + 1];
            int fieldslen[RDBAPI_ARGV_MAXNUM + 1];
            RDBFilterExpr fieldexprs[RDBAPI_ARGV_MAXNUM + 1];
            char *fieldvals[RDBAPI_ARGV_MAXNUM + 1];
            int fieldvalslen[RDBAPI_ARGV_MAXNUM + 1];

            // SELECT fields
            int numselect;
            char *selectfields[RDBAPI_ARGV_MAXNUM + 1];
            int selectfieldslen[RDBAPI_ARGV_MAXNUM + 1];

            // OFFSET m
            ub8 offset;

            // LIMIT n
            ub8 limit;
        } select;

        // UPSERT INTO tablespace.tablename(fieldnames...) VALUES(fieldvalues...);
        // UPSERT INTO tablespace.tablename(fieldnames...) VALUES(fieldvalues...) ON DUPLICATE KEY IGNORE;
        // UPSERT INTO tablespace.tablename(fieldnames...) VALUES(fieldvalues...) ON DUPLICATE KEY UPDATE col1=val1, col2=val2,...;
        //?? UPSERT INTO tablespace.tablename(fieldnames...) SELECT * FROM ...
        //?? UPSERT INTO tablespace.tablename(fieldnames...) SELECT (fieldnames...) FROM ...            
        //?? UPSERT INTO tablespace.tablename SELECT * FROM ...
        struct UPSERT_INTO {
            char tablespace[RDB_KEY_NAME_MAXLEN + 1];
            char tablename[RDB_KEY_NAME_MAXLEN + 1];

            int numfields;
            char *fieldnames[RDBAPI_ARGV_MAXNUM + 1];
            int   fieldnameslen[RDBAPI_ARGV_MAXNUM + 1];
            char *fieldvalues[RDBAPI_ARGV_MAXNUM + 1];
            int   fieldvalueslen[RDBAPI_ARGV_MAXNUM + 1];

            int updcols;
            char *updcolnames[RDBAPI_ARGV_MAXNUM + 1];
            int   updcolnameslen[RDBAPI_ARGV_MAXNUM + 1];
            char *updcolvalues[RDBAPI_ARGV_MAXNUM + 1];
            int   updcolvalueslen[RDBAPI_ARGV_MAXNUM + 1];

            // RDBSQL_UPSERT_MODE_INSERT
            // RDBSQL_UPSERT_MODE_IGNORE
            // RDBSQL_UPSERT_MODE_UPDATE
            // RDBSQL_UPSERT_MODE_SELECT
            int upsertmode;

            int fields_by_select;

            RDBSQLStmt selectstmt;

            struct UPSERT_PREPARE {
                RDBTableDes_t tabledes;

                zstringbuf keypattern;

                int attfields;
                int dupkey;

                int fields[RDBAPI_ARGV_MAXNUM + 1];
                int rowids[RDBAPI_KEYS_MAXNUM + 1];               
            } prepare;
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

    // offset to sqlblock
    int offsetlen;
    char *sqloffset;

    // original input sql
    ub4 sqlblocksize;
    char sqlblock[0];
} RDBSQLStmt_t;


/**
 * https://stackoverflow.com/questions/2616906/how-do-i-output-coloured-text-to-a-linux-terminal
 * 
 *   printf("\033[31mThis text is red \033[0mThis text has default color\n");
 * 
 * The codes for foreground and background colours are:
 *
 *   colorname     foreground  background
 *       black        30         40
 *       red          31         41
 *       green        32         42
 *       yellow       33         43
 *       blue         34         44
 *       magenta      35         45
 *       cyan         36         46
 *       white        37         47
 *
 *       Additionally, you can use these:
 *
 *       reset             0  (everything back to normal)
 *       bold/bright       1  (often a brighter shade of the same colour)
 *       underline         4
 *       inverse           7  (swap foreground and background colours)
 *       bold/bright off  21
 *       underline off    24
 *       inverse off      27
 * 
 * "(rdbsqlstmt.c:1123) RDBSQLErr-0012: invalid database name. [:125] SHOW TABLES ..."
 *
 */

static int RDBSQLErrMesg (int err, int lineno, char *errbuf, size_t bufsz)
{
    int errlen = 0;

    switch (err) {
    case RDBSQL_ERR_FATAL_APP:
        errlen = snprintf_chkd_V1(errbuf, bufsz, "(rdbsqlstmt.c:%d) RDBSQLErr-%04d: fatal app error.", lineno, err);
        break;
    case RDBSQL_ERR_INVAL_SQL:
        errlen = snprintf_chkd_V1(errbuf, bufsz, "(rdbsqlstmt.c:%d) RDBSQLErr-%04d: invalid SQL clause.", lineno, err);
        break;
    case RDBSQL_ERR_INVAL_TABLE:
        errlen = snprintf_chkd_V1(errbuf, bufsz, "(rdbsqlstmt.c:%d) RDBSQLErr-%04d: invalid table name.", lineno, err);
        break;
    case RDBSQL_ERR_INVAL_DBNAME:
        errlen = snprintf_chkd_V1(errbuf, bufsz, "(rdbsqlstmt.c:%d) RDBSQLErr-%04d: invalid database name.", lineno, err);
        break;
    case RDBSQL_ERR_ILLEGAL_CHAR:
        errlen = snprintf_chkd_V1(errbuf, bufsz, "(rdbsqlstmt.c:%d) RDBSQLErr-%04d: illegal char at", lineno, err);
        break;
    case RDBSQL_ERR_TOO_SHORT:
        errlen = snprintf_chkd_V1(errbuf, bufsz, "(rdbsqlstmt.c:%d) RDBSQLErr-%04d: name or value too short", lineno, err);
        break;
    case RDBSQL_ERR_TOO_LONG:
        errlen = snprintf_chkd_V1(errbuf, bufsz, "(rdbsqlstmt.c:%d) RDBSQLErr-%04d: name or value too long", lineno, err);
        break;
    default:
        errlen = snprintf_chkd_V1(errbuf, bufsz, "(rdbsqlstmt.c:%d) RDBSQLErr-%04d: unspecified error.", lineno, err);
        break;
    }

    return errlen;
}


static void RDBSQLErrFormat (RDBSQLStmt sqlstmt, int errcode, char *errsqlc, int lineno)
{
    int offlen = RDBSQLErrMesg(errcode, lineno, sqlstmt->ctx->errmsg, sizeof(sqlstmt->ctx->errmsg));

    if (errsqlc >= sqlstmt->sqlblock && errsqlc < sqlstmt->sqlblock + sqlstmt->sqlblocksize) {
        snprintf_chkd_V1(sqlstmt->ctx->errmsg + offlen, sizeof(sqlstmt->ctx->errmsg) - offlen,
            " [:%d] %.*s", (int)(errsqlc - sqlstmt->sqlblock), cstr_length(errsqlc, sizeof(sqlstmt->ctx->errmsg) - offlen - 20), errsqlc);
    } else {
        snprintf_chkd_V1(sqlstmt->ctx->errmsg + offlen, sizeof(sqlstmt->ctx->errmsg) - offlen,
            " [SHOULD NEVER RUN TO THIS] - %s", cstr_length(errsqlc, sizeof(sqlstmt->ctx->errmsg) - offlen - 40), errsqlc);
    }        
}


#define RDBSQLStmtError(errcode, errsqlc)  RDBSQLErrFormat(sqlstmt, errcode, errsqlc, __LINE__)


/**
 * 
 * RDBSQLStmt Private Functions
 * 
 */
void SQLStmtParseSelect        (RDBCtx ctx, RDBSQLStmt sqlstmt);
void SQLStmtParseDelete        (RDBCtx ctx, RDBSQLStmt sqlstmt);
void SQLStmtParseUpsert        (RDBCtx ctx, RDBSQLStmt sqlstmt);
void SQLStmtParseCreate        (RDBCtx ctx, RDBSQLStmt sqlstmt);
void SQLStmtParseDesc          (RDBCtx ctx, RDBSQLStmt sqlstmt);
void SQLStmtParseInfo          (RDBCtx ctx, RDBSQLStmt sqlstmt);
void SQLStmtParseDrop          (RDBCtx ctx, RDBSQLStmt sqlstmt);
void SQLStmtParseShowDatabases (RDBCtx ctx, RDBSQLStmt sqlstmt);
void SQLStmtParseShowTables    (RDBCtx ctx, RDBSQLStmt sqlstmt);

#if defined(__cplusplus)
}
#endif

#endif /* RDBSQLSTMT_H_INCLUDED */
