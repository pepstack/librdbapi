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
 * rdbapi.h
 *   redis cluster (DB) client api
 *
 * @author: master@pepstack.com
 *
 *   https://intermediate-and-advanced-software-carpentry.readthedocs.io/en/latest/c++-wrapping.html
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#ifndef RDBAPI_H_INCLUDED
#define RDBAPI_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#define RDBAPI_VERSION  "1.0.0"

#include "unitypes.h"

#include <hiredis/hiredis.h>
#include <hiredis/async.h>

#ifndef RDB_SYSTEM_TABLE_PREFIX
# define RDB_SYSTEM_TABLE_PREFIX  "redisdb"
#endif

#ifndef RDB_TABLE_DELIMITER_CHAR
# define RDB_TABLE_DELIMITER_CHAR  ((char) '|')
#endif

#ifndef RDB_CLUSTER_NODES_MAX
# define RDB_CLUSTER_NODES_MAX     255
#endif

#ifndef RDB_CLUSTER_CTX_TIMEOUT
# define RDB_CLUSTER_CTX_TIMEOUT   0     // never timeout
#endif

#ifndef RDB_CLUSTER_SOTIMEO_MS
# define RDB_CLUSTER_SOTIMEO_MS    12000 // 12 seconds
#endif

#ifndef RDB_TABLE_LIMIT_MAX
# define RDB_TABLE_LIMIT_MAX       10000
#endif

#ifndef RDB_TABLE_LIMIT_MIN
# define RDB_TABLE_LIMIT_MIN       10
#endif

#ifndef RDB_PRINT_LINE_INDENT
# define RDB_PRINT_LINE_INDENT     2
#endif


/**********************************************************************
 *
 * RDB API Result Types
 *
 *********************************************************************/

typedef int    RDBAPI_RESULT;
typedef void   RDBAPI_VOID;
typedef void * RDBAPI_PVOID;

/**
 * RDB API BOOL
 */
typedef int RDBAPI_BOOL;

#define RDBAPI_TRUE     1
#define RDBAPI_FALSE    0

/**
 * REDIS_REPLY_STRING=1
 * REDIS_REPLY_ARRAY=2
 * REDIS_REPLY_INTEGER=3
 * REDIS_REPLY_NIL=4
 * REDIS_REPLY_STATUS=5
 * REDIS_REPLY_ERROR=6
 */

/**********************************************************************
 *
 * SNPI error codes
 *
 *********************************************************************/

#define RDBAPI_SUCCESS         0
#define RDBAPI_NOERROR         RDBAPI_SUCCESS

#define RDBAPI_CONTINUE        1

#define RDBAPI_ERROR         (-1)

#define RDBAPI_ERR_NOKEY     (-2)
#define RDBAPI_ERR_RETVAL    (-3)
#define RDBAPI_ERR_TYPE      (-4)
#define RDBAPI_ERR_APP       (-5)
#define RDBAPI_ERR_REDIS     (-6)
#define RDBAPI_ERR_BADARG    (-7)
#define RDBAPI_ERR_NOMEM     (-10)
#define RDBAPI_ERR_EXISTED   (-11)
#define RDBAPI_ERR_NOBUF     (-12)
#define RDBAPI_ERR_NODES     (-13)
#define RDBAPI_ERR_REJECTED  (-14)
#define RDBAPI_ERR_NOFIELD   (-15)
#define RDBAPI_ERR_RDBSQL    (-16)


/**********************************************************************
 *
 * RDBAPI Constants
 *
 *********************************************************************/
#define RDBZSTRING_LEN_MAX         536870911
#define RDBZSTRING_SIZE_MAX        (RDBZSTRING_LEN_MAX + 1)

#define RDBAPI_ARGV_MAXNUM         252
#define RDBAPI_KEYS_MAXNUM         10

#define RDBAPI_SLAVES_MAXNUM       9

#define RDBAPI_PROP_MAXSIZE        256

#define RDBAPI_KEY_PERSIST        (-1)

#define RDBAPI_KEY_DELETED         1
#define RDBAPI_KEY_NOTFOUND        0

#define RDBAPI_SQL_PATTERN_SIZE    256
#define RDBAPI_SQL_KEYS_MAX        RDBAPI_KEYS_MAXNUM
#define RDBAPI_SQL_FIELDS_MAX      RDBAPI_ARGV_MAXNUM

/**********************************************************************
 *
 * RDB Constants
 *
 *********************************************************************/
#define RDB_HOSTADDR_MAXLEN            48
#define RDB_AUTHPASS_MAXLEN            32

#define RDB_PORT_NUMB_MIN              1024
#define RDB_PORT_NUMB_MAX              65535

#define RDB_ERROR_MSG_LEN              255

#define RDB_KEY_NAME_MAXLEN            32
#define RDB_KEY_VALUE_SIZE             256

#define RDB_ROWKEY_MAX_SIZE            1024

#define RDB_ERROR_OFFSET               ((ub8)(-1))

#define RDB_FIELD_LENGTH_MAX           16777216

#define RDB_FIELD_FLT_LENGTH_MAX       38
#define RDB_FIELD_FLT_SCALE_MIN        (-84)
#define RDB_FIELD_FLT_SCALE_MAX        127

// max length for sqlstmt clause
#define RDB_SQLSTMT_SQLBLOCK_MAXLEN    4000

// max length for where sub-clause
#define RDB_SQLSTMT_WHERE_MAXLEN       1000

/**********************************************************************
 *
 * RDB Objects
 *
 *********************************************************************/
#define RDBZSTR(zstr)               ((char *)(zstr))
#define RDBCZSTR(zstr)              ((const char *)(zstr))
#define RDBZSTRLEN(zstr)            ((int)RDBZStringLen(zstr))


typedef void * RDBZString;

typedef struct _RDBEnv_t         * RDBEnv;
typedef struct _RDBEnvNode_t     * RDBEnvNode;

typedef struct _RDBCtx_t         * RDBCtx;
typedef struct _RDBCtxNode_t     * RDBCtxNode;

typedef struct _RDBACtx_t        * RDBACtx;
typedef struct _RDBACtxNode_t    * RDBACtxNode;

typedef struct _RDBThreadCtx_t   * RDBThreadCtx;

typedef struct _RDBSQLStmt_t     * RDBSQLStmt;

typedef struct _RDBTableFilter_t * RDBTableFilter;

typedef struct _RDBResultMap_t   * RDBResultMap;
typedef struct _RDBCell_t        * RDBCell;
typedef struct _RDBRow_t         * RDBRow;
typedef struct _RDBRowIter_t     * RDBRowIter;


typedef struct _RDBBinary_t
{
    void *addr;
    uint32_t sz;
} RDBBinary_t, *RDBBinary;


typedef enum
{
    // DO NOT CHANGE !
    RDBFIL_IGNORE       = 0    // *
    ,RDBFIL_EQUAL       = 1    // a = b
    ,RDBFIL_LEFT_LIKE   = 2    // a like 'left%'
    ,RDBFIL_RIGHT_LIKE  = 3    // a like '%right'
    ,RDBFIL_LIKE        = 4    // a like '%mid%' or 'left%' or '%right'
    ,RDBFIL_MATCH       = 5    // a regmatch(pattern)
    ,RDBFIL_NOT_EQUAL   = 6    // a != b
    ,RDBFIL_GREAT_THAN  = 7    // a > b
    ,RDBFIL_LESS_THAN   = 8    // a < b
    ,RDBFIL_GREAT_EQUAL = 9    // a >= b
    ,RDBFIL_LESS_EQUAL  = 10   // a <= b
    ,filterexprs_count_max = 11
} RDBFilterExpr;


typedef enum
{
    // DO NOT CHANGE !
    RDBVT_ERROR = 0           // type is error
    ,RDBVT_SB2   = 'j'        // 16-bit signed int: SB2
    ,RDBVT_UB2   = 'v'        // 16-bit unsigned int: UB2
    ,RDBVT_SB4   = 'i'        // 32-bit signed int: SB4
    ,RDBVT_UB4   = 'u'        // 32-bit unsigned int: UB4
    ,RDBVT_UB4X  = 'x'        // 32-bit unsigned int (hex encoded): UB4X
    ,RDBVT_SB8   = 'I'        // 64-bit signed int: SB8
    ,RDBVT_UB8   = 'U'        // 64-bit unsigned int: UB8
    ,RDBVT_UB8X  = 'X'        // 64-bit unsigned int (hex encoded): UB8X
    ,RDBVT_CHAR  = 'c'        // character (signed char): CHAR
    ,RDBVT_BYTE  = 'b'        // byte (unsigned char): BYTE
    ,RDBVT_STR   = 's'        // utf8 encoded ascii string: STR
    ,RDBVT_FLT64 = 'f'        // 64-bit double precision: FLT64
    ,RDBVT_BLOB  = 'B'        // binary with variable size: BLOB
    ,RDBVT_DEC   = 'D'        // decimal(precision, scale): DEC
    ,RDBVT_SET   = 'S'        // redis set (sadd, smembers)
} RDBValueType;


typedef enum
{
    RDB_CELLTYPE_INVALID     = 0
    ,RDB_CELLTYPE_ZSTRING    = 1
    ,RDB_CELLTYPE_INTEGER    = 2
    ,RDB_CELLTYPE_DOUBLE     = 3
    ,RDB_CELLTYPE_BINARY     = 4  // tpl_bin
    ,RDB_CELLTYPE_REPLY      = 5  // reply string
    ,RDB_CELLTYPE_RESULTMAP  = 6  // nested result map
} RDBCellType;


// DO NOT CHANGE BELOW!
typedef enum
{
    NODEINFO_SERVER        = 0
    ,NODEINFO_CLIENTS      = 1
    ,NODEINFO_MEMORY       = 2
    ,NODEINFO_PERSISTENCE  = 3
    ,NODEINFO_STATS        = 4
    ,NODEINFO_REPLICATION  = 5
    ,NODEINFO_CPU          = 6
    ,NODEINFO_CLUSTER      = 7
    ,NODEINFO_KEYSPACE     = 8
    ,MAX_NODEINFO_SECTIONS = 9
} RDBNodeInfoSection;


typedef enum
{
    RDBSQL_INVALID = 0
    ,RDBSQL_SELECT  = 1
    ,RDBSQL_DELETE  = 2
    ,RDBSQL_UPSERT  = 3
    ,RDBSQL_CREATE  = 4
    ,RDBSQL_DESC_TABLE = 5
    ,RDBSQL_DROP_TABLE = 6
    ,RDBSQL_INFO_SECTION = 7
    ,RDBSQL_SHOW_DATABASES = 8
    ,RDBSQL_SHOW_TABLES = 9
    ,RDBENV_COMMAND_START = 10
    ,RDBENV_COMMAND_VERBOSE_ON = RDBENV_COMMAND_START
    ,RDBENV_COMMAND_VERBOSE_OFF = RDBENV_COMMAND_START + 1
    ,RDBENV_COMMAND_DELIMITER = RDBENV_COMMAND_START + 2
} RDBSQLStmtType;


typedef struct _RDBSheetStyle_t
{
    union {
        struct {
            ub1 minwidth;
            ub1 maxwidth;
            ub1 align;
            char delimiter;
        };

        ub1 style[4];
    };
} RDBSheetStyle_t;


typedef struct _RDBFieldDes_t
{
    char fieldname[RDB_KEY_NAME_MAXLEN + 1];
    int namelen;

    RDBValueType fieldtype;

    int length;
    int dscale;

    int rowkey;
    int nullable;

    char comment[RDB_KEY_VALUE_SIZE];
} RDBFieldDes_t;


typedef struct _RDBTableDes_t
{
    char table_rowkey[256];

    ub8 table_timestamp;

    char table_comment[RDB_KEY_VALUE_SIZE];

    /**
     * 1-based field index for table
     * rowid[0] is number of keys
     */
    int rowkeyid[RDBAPI_KEYS_MAXNUM + 1];

    int nfields;
    RDBFieldDes_t fielddes[RDBAPI_ARGV_MAXNUM];
} RDBTableDes_t;


typedef struct _RDBBlob_t
{
    struct {
        void *blob;
        ub8 blobsz;
    };

    struct {
        void *addr;
        ub4 usedsz;  /* size in bytes used */
        ub4 addrsz;  /* total size in bytes: used + unused */
    };

    struct {
        char *str;
        ub4 length;  /* length of string NOT including end char: '\0' */
        ub4 maxsz;   /* size in bytes for string. maxsz might >> len */
    };
} RDBBlob_t;


/**********************************************************************
 *
 * pulbic helper API
 *
 *********************************************************************/

#define RDBAPI_TIMESPEC_SEC    0
#define RDBAPI_TIMESPEC_MSEC   1

extern ub8 RDBCurrentTime (int spec, char *timestr);

extern void * RDBMemAlloc (size_t sizeb);
extern void * RDBMemRealloc (void *oldp, size_t oldsizeb, size_t newsizeb);
extern void RDBMemFree (void *addr);

extern RDBBinary RDBBinaryNew (const void *addr, ub4 sz);
extern void RDBBinaryFree (RDBBinary bin);

extern RDBZString RDBZStringNew (const char *str, ub4 len);
extern void RDBZStringFree (RDBZString zs);
extern ub4 RDBZStringLen (RDBZString zs);
extern int RDBZStringCmp (RDBZString zs, const char *str, ub4 len);


/**********************************************************************
 *
 * RDBThreadCtx API
 *
 *********************************************************************/

extern RDBThreadCtx RDBThreadCtxCreate (ub8 keyExpireMS, ssize_t keyBufSize, ssize_t valBufSize);

extern RDBCtx RDBThreadCtxAttach (RDBThreadCtx thrctx, RDBCtx ctx);

extern RDBCtx RDBThreadCtxGetConn (RDBThreadCtx thrctx);

extern ub8 RDBThreadCtxGetExpire (RDBThreadCtx thrctx);

extern char * RDBThreadCtxKeyBuf (RDBThreadCtx thrctx, ssize_t *bufSizeOut);

extern char * RDBThreadCtxValBuf (RDBThreadCtx thrctx, ssize_t *bufSizeOut);

extern void RDBThreadCtxDestroy (RDBThreadCtx thrctx);


/**********************************************************************
 *
 * RDBEnv API
 *
 *********************************************************************/

/**
 * RDBEnvCreate
 *  cluster - string for redis cluster nodes ("test@127.0.0.1:7001-7009") or
 *            pathfile to cfgfile ("file:///path/to/redplus.cfg").
 *  ctxtimeout - timeout in seconds for connection context
 *  sotimeo_ms - timeout in milliseconds for SO_SNDTIMEO and SO_RCVTIMEO
 */
extern RDBAPI_RESULT RDBEnvCreate (const char *cluster, int ctxtimeout, int sotimeo_ms, RDBEnv *outenv);

extern void RDBEnvDestroy (RDBEnv env);

extern int RDBEnvNumNodes (RDBEnv env);

extern int RDBEnvNumMasterNodes (RDBEnv env);

extern RDBEnvNode RDBEnvGetNode (RDBEnv env, int nodeindex);

extern RDBEnvNode RDBEnvFindNode (RDBEnv env, const char *host, ub4 port);

extern int RDBEnvNodeIndex (RDBEnvNode envnode);

extern const char * RDBEnvNodeHost (RDBEnvNode envnode);

extern ub4 RDBEnvNodePort (RDBEnvNode envnode);

extern ub4 RDBEnvNodeTimeout (RDBEnvNode envnode);

extern ub4 RDBEnvNodeSotimeo (RDBEnvNode envnode);

extern const char * RDBEnvNodeHostPort (RDBEnvNode envnode);

extern RDBAPI_BOOL RDBEnvNodeGetMaster (RDBEnvNode envnode, int *masterindex);

extern int RDBEnvNodeGetSlaves (RDBEnvNode envnode, int slaveindex[RDBAPI_SLAVES_MAXNUM]);


/**********************************************************************
 *
 * RDBCtx RDBCtxNode API
 *
 *********************************************************************/

extern RDBAPI_RESULT RDBCtxCreate (RDBEnv env, RDBCtx *outctx);

extern void RDBCtxFree (RDBCtx ctx);

extern RDBEnv RDBCtxGetEnv (RDBCtx ctx);

extern const char * RDBCtxErrMsg (RDBCtx ctx);

extern RDBCtxNode RDBCtxGetNode (RDBCtx ctx, int nodeindex);

extern RDBCtxNode RDBCtxGetActiveNode (RDBCtx ctx, const char *hostp, ub4 port);

extern RDBAPI_BOOL RDBCtxNodeIsOpen (RDBCtxNode ctxnode);

extern RDBAPI_RESULT RDBCtxNodeOpen (RDBCtxNode ctxnode);

extern void RDBCtxNodeClose (RDBCtxNode ctxnode);

extern int RDBCtxNodeIndex (RDBCtxNode ctxnode);

extern RDBCtx RDBCtxNodeGetCtx (RDBCtxNode ctxnode);

extern redisContext * RDBCtxNodeGetRedisContext (RDBCtxNode ctxnode);

extern RDBEnvNode RDBCtxNodeGetEnvNode (RDBCtxNode ctxnode);

extern RDBAPI_RESULT RDBCtxCheckInfo (RDBCtx ctx, RDBNodeInfoSection section, int whichnode);

extern void RDBCtxPrintInfo (RDBCtx ctx, int nodeindex);


/**********************************************************************
 *
 * Redis helper API
 *
 *********************************************************************/
extern RDBAPI_BOOL RedisCheckReplyStatus (redisReply *reply, const char *status, int stlen);

#define RedisIsReplyStatusOK(reply)    RedisCheckReplyStatus(reply, "OK", 2)

extern void RedisFreeReplyObject (redisReply **pReply);

extern void RedisFreeReplyObjects (redisReply **replys, int numReplys);

// exec A redis Command on any an active node
extern redisReply * RedisExecCommand (RDBCtx ctx, const char *command, RDBCtxNode *whichnode);

// exec A redis Command on any an active node
extern redisReply * RedisExecCommandArgv (RDBCtx ctx, int argc, const char **argv, const size_t *argvlen);


// exec A redis Command on all nodes, such as for: keys, scan
extern int RedisExecCommandOnAllNodes (RDBCtx ctx, char *command, redisReply **replys, RDBCtxNode *replyNodes);

// exec A redis Command on given RDBCtxNode
extern RDBAPI_RESULT RedisExecArgvOnNode (RDBCtxNode ctxnode, int argc, const char *argv[], const size_t *argvlen, redisReply **outReply);

// exec A redis Command on given RDBCtxNode
extern RDBAPI_RESULT RedisExecCommandArgvOnNode (RDBCtxNode ctxnode, char *command, redisReply **outReply);


typedef void (* redisReplyWatchCallback) (int type, void *data, size_t datalen, void *arg);

extern void RedisReplyObjectWatch (redisReply *reply, redisReplyWatchCallback wcb, void *wcbarg);

extern void RedisReplyObjectPrint (redisReply *reply, RDBCtxNode ctxnode);



/**
 * Redis wrapper synchronized API
 */

// set key's expiration time in ms:
//   expire_ms = 0  : ignored
//   expire_ms = -1 : never expired
//   expire_ms > 0  : timeout by ms
extern RDBAPI_RESULT RedisExpireKey (RDBCtx ctx, const char *key, size_t keylen, sb8 expire_ms);

extern RDBAPI_RESULT RedisSetKey (RDBCtx ctx, const char *key, size_t keylen, const char *value, size_t valuelen, sb8 expire_ms);

extern RDBAPI_RESULT RedisHMSet (RDBCtx ctx, const char *key, const char * fields[], const char *values[], const size_t *valueslen, sb8 expire_ms);
extern RDBAPI_RESULT RedisHMSetLen (RDBCtx ctx, const char *key, size_t keylen, const char * fields[], const size_t *fieldslen, const char *values[], const size_t *valueslen, sb8 expire_ms);

extern RDBAPI_RESULT RedisHMGet (RDBCtx ctx, const char * key, const char * fields[], redisReply **outReply);
extern RDBAPI_RESULT RedisHMGetLen (RDBCtx ctx, const char * key, size_t keylen, const char * fields[], const size_t *fieldslen, redisReply **outReply);

// success:
//   RDBAPI_KEY_DELETED
//   RDBAPI_KEY_NOTFOUND
extern int RedisDeleteKey (RDBCtx ctx, const char * key, size_t keylen, const char * fields[], int numfields);

// 0: not existed
// 1: existed
// < 0: error
extern int RedisExistsKey (RDBCtx ctx, const char * key, size_t keylen);

extern RDBAPI_RESULT RedisClusterKeyslot (RDBCtx ctx, const char *key, sb8 *slot);

extern RDBAPI_RESULT RedisClusterCheck (RDBCtx ctx);


/**
 * transaction API
 *   http://www.cnblogs.com/redcreen/articles/1955516.html
 *   high level functions
 */
// redis command: multi
extern RDBAPI_RESULT RedisTransStart (RDBCtx ctx);

// redis command: exec
extern RDBAPI_RESULT RedisTransCommit (RDBCtx ctx, redisReply **outReply);

// redis command: discard
extern void RedisTransDiscard (RDBCtx ctx);


/**
 *   watch 命令会监视给定的 key(s),当 exec 时候, 如果监视的 key 从调用 watch
 *     后发生过变化，则整个事务会失败。也可以调用 watch 多次监视多个 key.
 *     这样就可以对指定的 key 加乐观锁.
 *   注意 watch 的 key 是对整个连接有效的, 事务也一样. 如果连接断开, 监视和事务
 *   都会被自动清除. exec, discard, unwatch 命令都会清除连接中的所有监视.
 *
 *  [thread]> command        # result
 *  [1]> set uuid 100      # uuid=100
 *  [2]> watch uuid        # 设置快照点
 *  [2]> get uuid          # 100 stored in val
 *  [1]> set uuid 200      # uuid=200
 *  [2]> multi             # 事务开始
 *  [2]> set uuid $val + 500   # QUEUED  [2] 期望此时 uuid=600
 *  [2]> exec              # 提交事务, 失败! 因为设置快照点之后, uuid 被其他线程改变了
 *
 * 事务不支持跨 slot:
 *   set shang 100
 *   set hai 200
 *   watch shang hai
 *    (error) CROSSSLOT Keys in request don't hash to the same slot
 *
 * 强制多个 key 存储在同一个 slot 中 {...}:
 *   set {db}shang 100
 *   set {db}hai 200
 *   watch {db}shang {db}hai
 */
extern RDBAPI_RESULT RedisWatchKey (RDBCtx ctx, const char *key);

extern RDBAPI_RESULT RedisWatchKeys (RDBCtx ctx, const char *keys[], int numkeys);

extern void RedisUnwatch (RDBCtx ctx);


// 生成唯一 ID. 可以一次生成多个 (count > 1), genid 返回生成的(最大的那个)id.
// reset = 1, 表示当 genid 达到系统最大值 (=9223372036854775807) 后，是否从 1 开始.
// genid 的范围: [1, 9223372036854775807]
//
extern RDBAPI_RESULT RedisGenerateId (RDBCtx ctx, const char *generator, int count, sb8 *genid, int reset);

// hincrby key field increment
extern RDBAPI_RESULT RedisIncrIntegerField (RDBCtx ctx, const char *key, const char *field, sb8 increment, sb8 *retval, int reset);

// hincrbyfloat key field increment
extern RDBAPI_RESULT RedisIncrFloatField (RDBCtx ctx, const char *key, const char *field, double increment, double *retval, int reset);


/**********************************************************************
 *
 * RDB Table API
 *   low level functions
 *
 * specification:
 *   {tablespace::tablename:rowid1:rowid2,...}
 *
 *
 *********************************************************************/
extern RDBAPI_RESULT RDBTableScanFirst (RDBCtx ctx, RDBSQLStmt sqlstmt, RDBResultMap  *outresultmap);
extern ub8 RDBTableScanNext (RDBResultMap hResultMap, ub8 offset, ub8 limit);
extern RDBAPI_RESULT RDBTableCreate (RDBCtx ctx, const char *tablespace, const char *tablename, const char *tablecomment, int numfields, RDBFieldDes_t *fielddes);
extern RDBAPI_RESULT RDBTableDescribe (RDBCtx ctx, const char *tablespace, const char *tablename, RDBTableDes_t *tabledes);


/**********************************************************************
 *
 * RDBSQLStmt API
 *
 *********************************************************************/

extern RDBAPI_RESULT RDBSQLStmtCreate (RDBCtx ctx, const char *sql_block, size_t sql_len, RDBSQLStmt *outsqlstmt);
extern void RDBSQLStmtFree (RDBSQLStmt sqlstmt);
extern RDBSQLStmtType RDBSQLStmtGetSql (RDBSQLStmt sqlstmt, int indent, RDBZString *outsql);
extern RDBAPI_RESULT RDBSQLStmtExecute (RDBSQLStmt sqlstmt, RDBResultMap *outResultMap);

extern RDBAPI_RESULT RDBCtxExecuteSql (RDBCtx ctx, RDBZString sqlstr, RDBResultMap *outResultMap);
extern RDBAPI_RESULT RDBCtxExecuteFile (RDBCtx ctx, const char *sqlfile, RDBResultMap *outResultMap);


/**********************************************************************
 *
 * RDBResultMap API
 *
 *********************************************************************/
extern RDBAPI_RESULT RDBResultMapCreate (const char *title, const char *names[], const int *namelen, size_t numcols, int numrowidcols, RDBResultMap *outresultmap);
extern void RDBResultMapDestroy (RDBResultMap resultmap);
extern RDBAPI_RESULT RDBResultMapInsertRow (RDBResultMap resultmap, RDBRow row);
extern RDBRow RDBResultMapFindRow (RDBResultMap resultmap, const char *rowkey, int keylen);
extern void RDBResultMapDeleteOne (RDBResultMap resultmap, RDBRow row);
extern void RDBResultMapDeleteAll (RDBResultMap resultmap);
extern void RDBResultMapDeleteAllOnCluster (RDBResultMap resultmap);

extern RDBZString RDBResultMapTitle (RDBResultMap resultmap);

extern int RDBResultMapColHeads (RDBResultMap resultmap);
extern RDBZString RDBResultMapColHeadName (RDBResultMap resultmap, int colindex);
extern RDBRowIter RDBResultMapFirstRow (RDBResultMap resultmap);
extern RDBRowIter RDBResultMapNextRow (RDBRowIter rowiter);
extern RDBRow RDBRowIterGetRow (RDBRowIter iter);
extern ub4 RDBResultMapRows (RDBResultMap resultmap);
extern void RDBResultMapPrint (RDBCtx ctx, RDBResultMap resultmap, FILE *fout);


extern ub8 RDBResultMapGetOffset (RDBResultMap hResultMap);



/**************************************
 *
 * RDBRow API
 *
 *************************************/

extern RDBAPI_RESULT RDBRowNew (RDBResultMap resultmap, const char *key, int keylen, RDBRow *outrow);
extern void RDBRowFree (RDBRow row);
extern const char * RDBRowGetKey (RDBRow row, int *keylen);
extern int RDBRowCells (RDBRow row);
extern RDBCell RDBRowCell (RDBRow row, int colindex);


/**************************************
 *
 * RDBCell API
 *
 *************************************/

extern RDBCell RDBCellSetValue (RDBCell cell, RDBCellType type, void *value);
extern RDBCellType RDBCellGetValue (RDBCell cell, void **outvalue);

extern RDBCell RDBCellSetString (RDBCell cell, const char *str, int len);
extern RDBCell RDBCellSetInteger (RDBCell cell, sb8 val);
extern RDBCell RDBCellSetDouble (RDBCell cell, double val);
extern RDBCell RDBCellSetBinary (RDBCell cell, const void *addr, ub4 sz);
extern RDBCell RDBCellSetReply (RDBCell cell, redisReply *replyString);
extern RDBCell RDBCellSetResult (RDBCell cell, RDBResultMap resultmap);

extern RDBZString RDBCellGetString (RDBCell cell);
extern sb8 RDBCellGetInteger (RDBCell cell);
extern double RDBCellGetDouble (RDBCell cell);
extern RDBBinary RDBCellGetBinary (RDBCell cell);
extern redisReply* RDBCellGetReply (RDBCell cell);
extern RDBResultMap RDBCellGetResult (RDBCell cell);

extern void RDBCellClean (RDBCell cell);
extern void RDBCellPrint (RDBCell cell, FILE *fout, int colwidth);

#if defined(__cplusplus)
}
#endif

#endif /* RDBAPI_H_INCLUDED */
