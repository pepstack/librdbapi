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
 * rdbtypes.h
 *   rdb types
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#ifndef RDBTYPES_H_INCLUDED
#define RDBTYPES_H_INCLUDED

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

#include "common/red_black_tree.h"

#include "common/uthash/uthash.h"
#include "common/uthash/utarray.h"


typedef struct _RDBEnvNode_t * RDBEnvNodeMap;


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


#define RDBCTX_ERRMSG_ZERO(ctxp)   *(ctxp->errmsg) = '\0'
#define RDBCTX_ERRMSG_DONE(ctxp)   ctxp->errmsg[RDB_ERROR_MSG_LEN] = '\0'


typedef struct _RDBResultMap_t
{
    RDBCtx ctxh;

    RDBTableFilter filter;

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

    int numfields;
    RDBFieldDes_t fielddes[0];
} RDBResultMap_t;


typedef struct _RDBResultRow_t
{
    // rowkeys value
    redisReply *replykey;

    // field values including rowkeys
    RDBFieldsMap fieldmap;
} RDBResultRow_t;


RDBAPI_RESULT RDBResultMapNew (RDBTableFilter filter, int numfields, const RDBFieldDes_t *fielddes, ub1 *resultfields, RDBResultMap *phResultMap);


int RDBBuildRowkeyPattern (const char * tablespace, const char * tablename,
    const RDBFieldDes_t *fielddes, int numfields,
    int rowkeyid[RDBAPI_KEYS_MAXNUM + 1],
    char **outRowkeyPattern);


int RDBFinishRowkeyPattern (const RDBFieldDes_t *tabledes, int nfielddes, const int rowkeyid[RDBAPI_KEYS_MAXNUM + 1], char **pattern);


#if defined(__cplusplus)
}
#endif

#endif /* RDBTYPES_H_INCLUDED */
