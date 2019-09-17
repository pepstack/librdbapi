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

# define LOG4C_ENABLED  1
# include "../../win32/liblog4c/src/log4c-wrapper.h"

# ifdef _DEBUG
#   pragma comment(lib, "../../../win32/liblog4c/makefiles/msvc/target/x64/Debug/liblog4c.lib")
# else
#   pragma comment(lib, "../../../win32/liblog4c/makefiles/msvc/target/x64/Release/liblog4c.lib")
# endif
#endif

#include "common/memapi.h"
#include "common/cstrut.h"
#include "common/threadlock.h"

#include "common/uthash/uthash.h"
#include "common/uthash/utarray.h"

#include "common/tpl/tpl.h"
#include "common/tiny-regex-c/re.h"


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
    RDBZString valtypetable[256];

    char _exprstr[128];
    char *filterexprs[filterexprs_count_max];

    // 0: OFF; 1: ON (default)
    ub1 verbose;

    // delimiter
    char delimiter;

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


typedef struct _RDBTableCursor_t
{
    ub8 cursor;
    ub8 offset;

    ub4 nodeindex;
    int finished;
} RDBTableCursor_t, * RDBTableCursor;


#define RDBFieldDesTplFmt  "S(siiiiiis)"

typedef struct _RDBFieldDesTpl_t
{
    char *fieldname;
    int namelen;

    int fieldtype;

    int length;
    int dscale;

    int rowkey;
    int nullable;

    char *comment;
} RDBFieldDesTpl_t, *RDBFieldDesTpl;

// 0-success
int RDBFieldDesPack (RDBFieldDes_t * infieldes, int numfields, tpl_bin *outbin);

// 1-success
int RDBFieldDesUnpack (const void *addrin, ub4 sizein, RDBFieldDes_t * outfieldes, int numfields);

// 1-success
int RDBFieldDesCheckSet (const RDBZString valtypetable[256], const RDBFieldDes_t * fielddes, int nfields, int rowkeyid[RDBAPI_KEYS_MAXNUM + 1], char *errmsg, size_t msgsz);

int RDBNodeInfoQuery (RDBCtxNode ctxnode, RDBNodeInfoSection section, const char *propname, char propvalue[RDBAPI_PROP_MAXSIZE]);

int RDBTableDesFieldIndex (const RDBTableDes_t *tabledes, const char *fieldname, int fieldnamelen);

RDBAPI_RESULT RDBTableScanOnNode (RDBCtxNode ctxnode, RDBTableCursor nodestate, const char *pattern, size_t patternlen, ub8 maxlimit, redisReply **outReply);

#if defined(__cplusplus)
}
#endif

#endif /* RDBCOMMON_H_INCLUDED */
