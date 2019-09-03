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
 * rdbcommon.h
 *   rdb common types
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#ifndef RDBCOMMON_H_INCLUDED
#define RDBCOMMON_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#include "rdbapi.h"

#if defined(__WINDOWS__)
# define NO_QFORKIMPL
# include <Win32_Interop/win32fixes.h>
#endif

#include "common/memapi.h"
#include "common/cstrutil.h"
#include "common/threadlock.h"

#include "common/red_black_tree.h"

#include "common/uthash/uthash.h"
#include "common/uthash/utarray.h"

#include "common/tpl/tpl.h"


typedef struct _RDBEnvNode_t * RDBEnvNodeMap;


/* Zero ended Ansi string */
typedef struct _RDBZString_t
{
    ub4 len;
    char str[0];
} RDBZString_t;


typedef struct _RDBProp_t
{
    char *name;
    char *value;

    char *_mapbuf;

    // makes this structure hashable
	UT_hash_handle hh;
} RDBProp_t, *RDBPropNode, *RDBPropMap;


static void RDBPropMapFree (RDBPropMap propmap)
{
    if (propmap) {
        RDBPropNode curnode, tmpnode;
        char *mapbuf = propmap->_mapbuf;

        HASH_ITER(hh, propmap, curnode, tmpnode) {
            HASH_DEL(propmap, curnode);
            RDBMemFree(curnode);
        }

        RDBMemFree(mapbuf);
    }
}


typedef struct _RDBNodeReplica_t
{
    int is_master;
    int master_nodeindex;
    int connected_slaves;
    int slaves[RDBAPI_SLAVES_MAXNUM];
} RDBNodeReplica_t;


typedef struct _RDBEnvNode_t
{
    // constants
    RDBEnv env;

    int index;

    int port;

    struct timeval ctxtimeo;
    struct timeval sotimeo;

    char host[RDB_HOSTADDR_MAXLEN + 1];
    char authpass[RDB_AUTHPASS_MAXLEN + 1];

    // host:port
    char key[RDB_HOSTADDR_MAXLEN + 12];

    RDBPropMap nodeinfo[MAX_NODEINFO_SECTIONS];

    // Replication
    RDBNodeReplica_t replica;

    // makes this structure hashable
	UT_hash_handle hh;
} RDBEnvNode_t;


typedef struct _RDBEnv_t
{
    // readonly check table for RDBValueType: 0 - bad; 1 - good
    char valtype_chk_table[256];
    char *valtypenames[256];
    char _valtypenamebuf[256];

    thread_lock_t thrlock;

    RDBEnvNodeMap nodemap;

    int maxclusternodes;
    int clusternodes;

    RDBEnvNode_t nodes[0];
} RDBEnv_t;


typedef struct _RDBCtxNode_t
{
    redisContext *redCtx;

    RDBCtx ctx;
    int index;

    int slot;
} RDBCtxNode_t;


typedef struct _RDBCtx_t
{
    RDBEnv env;

    RDBCtxNode_t *activenode;

    char errmsg[RDB_ERROR_MSG_LEN + 1];

    RDBCtxNode_t nodes[0];
} RDBCtx_t;


typedef struct _RDBAsynCtxNode_t
{
    /**
     * redisAsyncContext is NOT thread-safe
     */
    redisAsyncContext *redAsynCtx;

    RDBACtx actx;
    int index;

    int slot;
} RDBAsynCtxNode_t, RDBACtxNode_t;


/**
 * redis async context
 *   https://blog.csdn.net/l1902090/article/details/38583663
 */
typedef struct _RDBAsynCtx_t
{
    struct event_base * eb;

    RDBEnv env;

    RDBACtxNode_t *activenode;

    char errmsg[RDB_ERROR_MSG_LEN + 1];

    RDBACtxNode_t nodes[0];
} RDBAsynCtx_t, RDBACtx_t;


typedef struct _RDBNameReply_t
{
    // makes this structure hashable
    UT_hash_handle hh;

    char *name;

    // reply object
    redisReply *value;

    char *subval;
    int sublen;

    int namelen;
    char namebuf[0];
} RDBNameReply_t, *RDBNameReply, *RDBNameReplyMap;


typedef struct _RDBResultMap_t
{
    RDBCtx ctxh;

    RDBTableFilter filter;

    RDBSQLStmtType sqlstmt;

    char delimiter;

    // store reply rows
    red_black_tree_t  rbtree;

    // constants: rowkeyid[0] is count for keys
    int rowkeyid[RDBAPI_SQL_KEYS_MAX + 1];

    int kplen;
    char *keypattern;

    // count for result fields
    int resultfields;

    // which fields to fetch
    char *fetchfields[RDBAPI_ARGV_MAXNUM + 1];

    // length of fieldname
    int fieldnamelens[RDBAPI_ARGV_MAXNUM + 1];

    // for: DESC $table
    ub8 table_timestamp;
    char table_datetime[24];
    char table_comment[RDB_KEY_VALUE_SIZE];

    int numfields;
    RDBFieldDes_t fielddes[0];
} RDBResultMap_t;


typedef struct _RDBResultFilter_t
{
    RDBCtx ctx;

    RDBTableFilter filter;

    RDBSQLStmtType sqlstmt;

    // constants: rowkeyid[0] is count for keys
    int rowkeyid[RDBAPI_SQL_KEYS_MAX + 1];

    int kplen;
    char *keypattern;

    // count for result fields
    int resultfields;

    // which fields to fetch
    char *fetchfields[RDBAPI_ARGV_MAXNUM + 1];

    // length of fieldname
    int fieldnamelens[RDBAPI_ARGV_MAXNUM + 1];

    int numfields;
    RDBFieldDes_t fielddes[0];
} RDBResultFilter_t, *RDBResultFilter;


typedef struct _RDBResultRow_t
{
    // rowkeys value
    redisReply *replykey;

    // field values including rowkeys
    RDBFieldsMap fieldmap;
} RDBResultRow_t;


typedef struct _RDBTableCursor_t
{
    ub8 cursor;
    ub8 offset;

    ub4 nodeindex;
    int finished;
} RDBTableCursor_t, * RDBTableCursor;


static redisReply * RDBStringReplyCreate (const char *str, size_t len)
{
    redisReply * reply = (redisReply *) RDBMemAlloc(sizeof(*reply));
    reply->type = REDIS_REPLY_STRING;

    if (len == (size_t)(-1)) {
        len = str? strlen(str) : 0;
    }

    reply->str = RDBMemAlloc(len + 1);
    reply->len = (int) len;

    if (str && len) {
        memcpy(reply->str, str, len);
    }

    return reply;
}


static redisReply * RDBIntegerReplyCreate (sb8 val)
{
    redisReply * reply = (redisReply *) RDBMemAlloc(sizeof(*reply));

    reply->type = REDIS_REPLY_INTEGER;
    reply->integer = val;

    return reply;
}


static redisReply * RDBArrayReplyCreate (size_t elements)
{
    redisReply * replyArr = (redisReply *) RDBMemAlloc(sizeof(*replyArr));
    replyArr->type = REDIS_REPLY_ARRAY;

    replyArr->element = (redisReply **) RDBMemAlloc(sizeof(redisReply *) * elements);
    replyArr->elements = elements;

    return replyArr;
}

//DEL??
void RDBResultRowFree (RDBResultRow rowdata);

//DEL??
void RDBResultMapNew (RDBCtx ctx, RDBTableFilter filter, RDBSQLStmtType sqlstmt, const char *tablespace, const char *tablename, int numfields, const RDBFieldDes_t *fielddes, ub1 *resultfields, RDBResultMap *phResultMap);

RDBResultFilter RDBResultFilterNew (RDBCtx ctx, RDBTableFilter filter, RDBSQLStmtType stmt, int numfields, const RDBFieldDes_t *fielddes, ub1 resultfields[]);

void RDBResultFilterFree (RDBResultFilter filter);

int RDBBuildRowkeyPattern (const char * tablespace, const char * tablename,
    const RDBFieldDes_t *fielddes, int numfields,
    int rowkeyid[RDBAPI_KEYS_MAXNUM + 1],
    char **outRowkeyPattern);


int RDBFinishRowkeyPattern (const RDBFieldDes_t *tabledes, int nfielddes, const int rowkeyid[RDBAPI_KEYS_MAXNUM + 1], char **pattern);

int RDBNodeInfoQuery (RDBCtxNode ctxnode, RDBNodeInfoSection section, const char *propname, char propvalue[RDBAPI_PROP_MAXSIZE]);

RDBAPI_RESULT RDBTableScanOnNode (RDBCtxNode ctxnode, RDBTableCursor nodestate, const RDBBlob_t *patternblob, ub4 maxlimit, redisReply **outReply);

#if defined(__cplusplus)
}
#endif

#endif /* RDBCOMMON_H_INCLUDED */
