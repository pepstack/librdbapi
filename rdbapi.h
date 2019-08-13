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
# define RDB_TABLE_DELIMITER_CHAR  '|'
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

#define RDB_ERROR_MSG_LEN              127

#define RDB_KEY_NAME_MAXLEN            32
#define RDB_KEY_VALUE_SIZE             256

#define RDB_ROWKEY_MAX_SIZE            1024

#define RDB_ERROR_OFFSET               ((ub8)(-1))

#define RDB_FIELD_LENGTH_MAX           16777216

#define RDB_FIELD_FLT_LENGTH_MAX       38
#define RDB_FIELD_FLT_SCALE_MIN        (-84)
#define RDB_FIELD_FLT_SCALE_MAX        127

#define RDB_SQL_TOTAL_LEN_MAX          4000
#define RDB_SQL_WHERE_LEN_MAX          1000

/**********************************************************************
 *
 * RDB Objects
 *
 *********************************************************************/

typedef struct _RDBEnv_t         * RDBEnv;
typedef struct _RDBEnvNode_t     * RDBEnvNode;

typedef struct _RDBCtx_t         * RDBCtx;
typedef struct _RDBCtxNode_t     * RDBCtxNode;

typedef struct _RDBACtx_t        * RDBACtx;
typedef struct _RDBACtxNode_t    * RDBACtxNode;

typedef struct _RDBTableFilter_t * RDBTableFilter;
typedef struct _RDBResultMap_t   * RDBResultMap;
typedef struct _RDBResultRow_t   * RDBResultRow;

typedef struct _RDBFieldDef_t    * RDBFieldDef;
typedef struct _RDBNameReply_t   * RDBFieldsMap;

typedef struct _RDBThreadCtx_t   * RDBThreadCtx;

typedef struct _RDBSQLParser_t   * RDBSQLParser;


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
} RDBValueType;


// DO NOT CHANGE BELOW!
typedef enum
{
    NODEINFO_SERVER = 0
    ,NODEINFO_CLIENTS
    ,NODEINFO_MEMORY
    ,NODEINFO_PERSISTENCE
    ,NODEINFO_STATS
    ,NODEINFO_REPLICATION
    ,NODEINFO_CPU
    ,NODEINFO_CLUSTER
    ,NODEINFO_KEYSPACE
    ,MAX_NODEINFO_SECTIONS
} RDBNodeInfoSection;


typedef enum
{
    RDBSQL_INVALID = 0
    ,RDBSQL_SELECT  = 1
    ,RDBSQL_DELETE  = 2
    ,RDBSQL_UPDATE  = 3
    ,RDBSQL_CREATE  = 4
    ,RDBSQL_DESC_TABLE = 5
    ,RDBSQL_DROP_TABLE = 6
    ,RDBSQL_INFO_SECTION = 7
} RDBSQLStmt;


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
    char table_datetime[20];
    char table_comment[RDB_KEY_VALUE_SIZE];

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
 * time API
 *
 *********************************************************************/

#define RDBAPI_TIMESPEC_SEC    0
#define RDBAPI_TIMESPEC_MSEC   1

extern ub8 RDBCurrentTime (int spec, char *timestr);

extern void * RDBMemAlloc (size_t sizeb);

extern void RDBMemFree (void *addr);

extern int RDBExprValues (RDBValueType vt, const char *val1, int len1, RDBFilterExpr expr, const char *val2, int len2);


/**********************************************************************
 *
 * RDBThreadCtx API
 *
 *********************************************************************/

extern RDBThreadCtx RDBThreadCtxCreate (ub8 key_expire_ms, ssize_t keybuf_size_max, ssize_t valbuf_size_max);

extern RDBCtx RDBThreadCtxAttach (RDBThreadCtx thrctx, RDBCtx ctx);

extern RDBCtx RDBThreadCtxGetConn (RDBThreadCtx thrctx, ub8 * key_expire_ms);

extern char * RDBThreadCtxGetKBuf (RDBThreadCtx thrctx, size_t *keybuf_cb);

extern char * RDBThreadCtxGetVBuf (RDBThreadCtx thrctx, size_t *valbuf_cb);

extern void RDBThreadCtxFree (RDBThreadCtx thrctx);


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

extern void RDBEnvSetNode (RDBEnvNode node, const char *host, ub4 port, ub4 ctxtimeout, ub4 sotimeo_ms, const char *authpass);

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

extern RDBAPI_RESULT RDBCtxNodeCheckInfo (RDBCtxNode ctxnode, RDBNodeInfoSection section);

extern RDBAPI_RESULT RDBCtxCheckInfo (RDBCtx ctx, RDBNodeInfoSection section);

extern void RDBCtxPrintInfo (RDBCtx ctx, int nodeindex);

extern int RDBCtxNodeInfoProp (RDBCtxNode ctxnode, RDBNodeInfoSection section, const char *propname, char propvalue[RDBAPI_PROP_MAXSIZE]);


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
extern redisReply * RedisExecCommand (RDBCtx ctx, int argc, const char **argv, const size_t *argvlen);

// exec A redis Command on all nodes, such as for: keys, scan
extern int RedisExecCommandOnAllNodes (RDBCtx ctx, char *command, redisReply **replys, RDBCtxNode *replyNodes);

// exec A redis Command on given RDBCtxNode
extern RDBAPI_RESULT RedisExecArgvOnNode (RDBCtxNode ctxnode, int argc, const char *argv[], const size_t *argvlen, redisReply **outReply);

// exec A redis Command on given RDBCtxNode
extern RDBAPI_RESULT RedisExecCommandOnNode (RDBCtxNode ctxnode, char *command, redisReply **outReply);


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
extern RDBAPI_RESULT RedisExpireKey (RDBCtx ctx, const char *key, sb8 expire_ms);

extern RDBAPI_RESULT RedisSetKey (RDBCtx ctx, const char *key, const char *value, size_t valuelen, sb8 expire_ms);

extern RDBAPI_RESULT RedisHMSet (RDBCtx ctx, const char *key, const char * fields[], const char *values[], const size_t *valueslen, sb8 expire_ms);

extern RDBAPI_RESULT RedisHMSetLen (RDBCtx ctx, const char *key, size_t keylen, const char * fields[], const size_t *fieldslen, const char *values[], const size_t *valueslen, sb8 expire_ms);

extern RDBAPI_RESULT RedisHMGet (RDBCtx ctx, const char * key, const char * fields[], redisReply **outReply);

// success:
//   RDBAPI_KEY_DELETED
//   RDBAPI_KEY_NOTFOUND
extern int RedisDeleteKey (RDBCtx ctx, const char * key, const char * fields[], int numfields);

// 0: not existed
// 1: existed
// < 0: error
extern int RedisExistsKey (RDBCtx ctx, const char * key);

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
 *   watch �������Ӹ����� key(s),�� exec ʱ��, ������ӵ� key �ӵ��� watch
 *     �������仯�������������ʧ�ܡ�Ҳ���Ե��� watch ��μ��Ӷ�� key.
 *     �����Ϳ��Զ�ָ���� key ���ֹ���.
 *   ע�� watch �� key �Ƕ�����������Ч��, ����Ҳһ��. ������ӶϿ�, ���Ӻ�����
 *   ���ᱻ�Զ����. exec, discard, unwatch �������������е����м���.
 *
 *  [thread]> command        # result
 *  [1]> set uuid 100      # uuid=100
 *  [2]> watch uuid        # ���ÿ��յ�
 *  [2]> get uuid          # 100 stored in val
 *  [1]> set uuid 200      # uuid=200
 *  [2]> multi             # ����ʼ
 *  [2]> set uuid $val + 500   # QUEUED  [2] ������ʱ uuid=600
 *  [2]> exec              # �ύ����, ʧ��! ��Ϊ���ÿ��յ�֮��, uuid �������̸߳ı���
 *
 * ����֧�ֿ� slot:
 *   set shang 100
 *   set hai 200
 *   watch shang hai
 *    (error) CROSSSLOT Keys in request don't hash to the same slot
 *
 * ǿ�ƶ�� key �洢��ͬһ�� slot �� {...}:
 *   set {db}shang 100
 *   set {db}hai 200
 *   watch {db}shang {db}hai
 */
extern RDBAPI_RESULT RedisWatchKey (RDBCtx ctx, const char *key);

extern RDBAPI_RESULT RedisWatchKeys (RDBCtx ctx, const char *keys[], int numkeys);

extern void RedisUnwatch (RDBCtx ctx);


// ����Ψһ ID. ����һ�����ɶ�� (count > 1), genid �������ɵ�(�����Ǹ�)id.
// reset = 1, ��ʾ�� genid �ﵽϵͳ���ֵ (=9223372036854775807) ���Ƿ�� 1 ��ʼ.
// genid �ķ�Χ: [1, 9223372036854775807]
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
 * example:
 *
 *   {xsdb::logentry:$sid:$uid}
 *
 *   RDBTableScanFirst(ctx, "xsdb", "logentry", 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, &replyMap);
 *   RDBTableScanNext(ctx, replyMap);
 *
 *********************************************************************/
extern RDBAPI_RESULT RDBTableScanFirst (RDBCtx ctx,
    RDBSQLStmt sqlstmt,
    const char *tablespace,
    const char *tablename,
    int filter_numkeys,
    const char *filter_keys[],
    RDBFilterExpr filter_keyexprs[],
    const char *filter_keyvals[],
    int filter_numfields,
    const char *filter_fields[],
    RDBFilterExpr filter_fieldexprs[],
    const char *filter_fieldvals[],
    const char *groupby[],    // Not Supported Now!
    const char *orderby[],    // Not Supported Now!
    int result_filedcount,
    const char *result_fieldnames[],
    RDBResultMap *phResultMap);


extern ub8 RDBTableScanNext (RDBResultMap hResultMap, ub8 offset, ub4 limit);

extern RDBAPI_RESULT RDBTableCreate (RDBCtx ctx, const char *tablespace, const char *tablename, const char *tablecomment, int numfields, RDBFieldDes_t fielddes[]);

extern RDBAPI_RESULT RDBTableDescribe (RDBCtx ctx, const char *tablespace, const char *tablename, RDBTableDes_t *tabledes);

extern int RDBTableFindField (const RDBFieldDes_t fields[RDBAPI_ARGV_MAXNUM], int numfields, const char *fieldname, int fieldnamelen);

extern RDBFieldsMap RDBTableFetchFields (RDBCtx ctx, RDBFieldsMap fields, const char *fieldnames[], int fieldnamelens[], const char *rowkey);

extern redisReply * RDBFieldsMapGetField (RDBFieldsMap fields, const char *fieldname, int fieldnamelen);

extern void RDBFieldsMapFree (RDBFieldsMap fields);


/**********************************************************************
 *
 * RDBResultMap API
 *
 *********************************************************************/
extern void RDBResultMapFree (RDBResultMap hResultMap);

extern void RDBResultMapClean (RDBResultMap hResultMap);

extern ub8 RDBResultMapSize (RDBResultMap hResultMap);

extern RDBAPI_RESULT RDBResultMapInsert (RDBResultMap hResultMap, redisReply *reply);

extern void RDBResultMapDelete (RDBResultMap hResultMap, redisReply *reply);

extern RDBAPI_BOOL RDBResultMapExist (RDBResultMap hResultMap, redisReply *reply);

extern void RDBResultMapTraverse (RDBResultMap hResultMap, void (onRowNodeCallback)(void *, void *), void *arg);

extern ub8 RDBResultMapGetOffset (RDBResultMap hResultMap);

extern char RDBResultMapSetDelimiter (RDBResultMap hResultMap, char delimiter);

extern void RDBResultMapPrintOut (RDBResultMap hResultMap, RDBAPI_BOOL withHead);


/**********************************************************************
 *
 * RDBSQLParser API
 *
 *********************************************************************/

extern RDBAPI_RESULT RDBSQLParserNew (RDBCtx ctx, const char *sql, size_t sqlen, RDBSQLParser *outSqlParser);

extern void RDBSQLParserFree (RDBSQLParser sqlParser);

extern ub8 RDBSQLExecute (RDBCtx ctx, RDBSQLParser sqlParser, RDBResultMap *outResultMap);

extern RDBSQLStmt RDBSQLParserGetStmt (RDBSQLParser sqlParser, char **parsedClause, int pretty);


#if defined(__cplusplus)
}
#endif

#endif /* RDBAPI_H_INCLUDED */
