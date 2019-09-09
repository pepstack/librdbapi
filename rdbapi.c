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
 * rdbapi.c
 *
 * Reference:
 *    https://github.com/redis/hiredis
 *    https://redislabs.com/lp/hiredis/
 *    https://github.com/aclisp/hiredispool
 *    https://github.com/eyjian/r3c/blob/master/r3c.cpp
 *    http://www.mamicode.com/info-detail-501902.html
 *
 * Async:
 *    https://blog.csdn.net/l1902090/article/details/38583663
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#include "rdbcommon.h"

#include <time.h>

#if defined(__WINDOWS__)

# include <sys/timeb.h>

# pragma comment(lib, "hiredis.lib")
# pragma comment(lib, "Win32_Interop.lib")

/**
 * win32fixes.c
 *  Modified by Henry Rawas (henryr@schakra.com)
 *  - make it compatible with Visual Studio builds
 *  - added wstrtod to handle INF, NAN
 *  - added gettimeofday routine
 *  - modified rename to retry after failure
 */
# include <stdlib.h>
# include <string.h>
# include <ctype.h>
# include <locale.h>

# if _MSC_VER < 1800
#   define isnan _isnan
#   define isfinite _finite
#   define isinf(x) (!_finite(x))
# else
#   include <math.h>
# endif

static _locale_t clocale = NULL;

double wstrtod(const char *nptr, char **eptr)
    {
        double d;
        char *leptr;
        if (clocale == NULL) {
            clocale = _create_locale(LC_ALL, "C");
        }
        d = _strtod_l(nptr, &leptr, clocale);
        /* if 0, check if input was inf */
        if (d == 0 && nptr == leptr) {
            int neg = 0;
            while (isspace(*nptr)) {
                nptr++;
            }
            if (*nptr == '+') {
                nptr++;
            } else if (*nptr == '-') {
                nptr++;
                neg = 1;
            }

            if (_strnicmp("INF", nptr, 3) == 0) {
                if (eptr != NULL) {
                    if ((_strnicmp("INFINITE", nptr, 8) == 0) || (_strnicmp("INFINITY", nptr, 8) == 0)) {
                        *eptr = (char*) (nptr + 8);
                    } else {
                        *eptr = (char*) (nptr + 3);
                    }
                }
                if (neg == 1) {
                    return -HUGE_VAL;
                } else {
                    return HUGE_VAL;
                }
            } else if (_strnicmp("NAN", nptr, 3) == 0) {
                if (eptr != NULL) {
                    *eptr = (char*) (nptr + 3);
                }
                /* create a NaN : 0 * infinity*/
                d = HUGE_VAL;
                return d * 0;
            }
        }
        if (eptr != NULL) {
            *eptr = leptr;
        }
        return d;
    }

#endif /* __WINDOWS__ */


/**********************************************************************
 *
 * RDBThreadCtx API
 *
 *********************************************************************/

typedef struct _RDBThreadCtx_t
{
    /* redis connection attached */
    RDBCtx ctx;

    /**
     * default timeout for key
     *   =  0: Not set (ignored)
     *   = -1: Persist
     *   >  0: timeout in ms (pexpire)
     */
    ub8 key_expire_ms;

    ssize_t valbuf_size;
    char *valbuf;

    ssize_t keybuf_size;
    char keybuf[0];
} RDBThreadCtx_t;


RDBThreadCtx RDBThreadCtxCreate (ub8 keyExpireMS, ssize_t keyBufSize, ssize_t valBufSize)
{
    RDBThreadCtx thrctx = (RDBThreadCtx) RDBMemAlloc(sizeof(RDBThreadCtx_t) + (valBufSize + valBufSize));

    thrctx->key_expire_ms = keyExpireMS;

    thrctx->keybuf_size = keyBufSize;
    thrctx->valbuf_size = valBufSize;

    thrctx->valbuf = & thrctx->keybuf[valBufSize];

    return thrctx;
}


RDBCtx RDBThreadCtxAttach (RDBThreadCtx thrctx, RDBCtx ctx)
{
    RDBCtx oldctx = thrctx->ctx;
    thrctx->ctx = ctx;
    return oldctx;
}


RDBCtx RDBThreadCtxGetConn (RDBThreadCtx thrctx)
{
    return thrctx->ctx;
}


ub8 RDBThreadCtxGetExpire (RDBThreadCtx thrctx)
{
    return thrctx->key_expire_ms;
}


char * RDBThreadCtxKeyBuf (RDBThreadCtx thrctx, ssize_t *bufSizeOut)
{
    if (bufSizeOut) {
        *bufSizeOut = thrctx->keybuf_size;
    }
    return thrctx->keybuf;
}


char * RDBThreadCtxValBuf (RDBThreadCtx thrctx, ssize_t *bufSizeOut)
{
    if (bufSizeOut) {
        *bufSizeOut = thrctx->valbuf_size;
    }
    return thrctx->valbuf;
}


void RDBThreadCtxDestroy (RDBThreadCtx thrctx)
{
    RDBCtx ctx = RDBThreadCtxAttach(thrctx, NULL);
    if (ctx) {
        RDBCtxFree(ctx);
    }

    RDBMemFree(thrctx);
}


/**********************************************************************
 *
 * Redis helper API
 *
 *********************************************************************/

ub8 RDBCurrentTime (int spec, char *timestr)
{
#ifdef __WINDOWS__
    struct timeb tb;

    ftime(&tb);

    if (spec == RDBAPI_TIMESPEC_SEC) {
        if (timestr) {
            struct tm *p = localtime(&tb.time);
            snprintf_chkd_V2(-1, timestr, 20, "%04d-%02d-%02d %02d:%02d:%02d",
                    1900 + p->tm_year,
                    1 + p->tm_mon,
                    p->tm_mday,
                    p->tm_hour,
                    p->tm_min,
                    p->tm_sec);
            timestr[19] = 0;
        }

        return tb.time;
    } else if (spec == RDBAPI_TIMESPEC_MSEC) {
        if (timestr) {
            struct tm *p = localtime(&tb.time);
            snprintf_chkd_V2(-1, timestr, 24, "%04d-%02d-%02d %02d:%02d:%02d.%03d",
                1900 + p->tm_year,
                1 + p->tm_mon,
                p->tm_mday,
                p->tm_hour,
                p->tm_min,
                p->tm_sec,
                (int) (tb.millitm));
            timestr[23] = 0;
        }
        return tb.time*1000 + tb.millitm;
    }
#else
    struct timeval tv;

    if (spec == RDBAPI_TIMESPEC_SEC) {
        if (gettimeofday(&tv, NULL) == 0) {
            if (timestr) {
                struct tm *p = localtime(&tv.tv_sec);
                snprintf_chkd_V2(-1, timestr, 20, "%04d-%02d-%02d %02d:%02d:%02d",
                    1900 + p->tm_year,
                    1 + p->tm_mon,
                    p->tm_mday,
                    p->tm_hour,
                    p->tm_min,
                    p->tm_sec);
            }
            return tv.tv_sec;
        }
    } else if (spec == RDBAPI_TIMESPEC_MSEC) {
        if (gettimeofday(&tv, NULL) == 0) {
            if (timestr) {
                struct tm *p = localtime(&tv.tv_sec);
                snprintf_chkd_V2(-1, timestr, 24, "%04d-%02d-%02d %02d:%02d:%02d.%03d",
                    1900 + p->tm_year,
                    1 + p->tm_mon,
                    p->tm_mday,
                    p->tm_hour,
                    p->tm_min,
                    p->tm_sec,
                    (int) (tv.tv_usec/1000));
            }
            return (tv.tv_sec * 1000 + tv.tv_usec / 1000);
        }
    }

#endif

    // invalid arg
    return RDBAPI_ERROR;
}


void * RDBMemAlloc (size_t sizeb)
{
    return mem_alloc_zero(1, sizeb);
}


void * RDBMemRealloc (void *oldp, size_t oldsizeb, size_t newsizeb)
{
    void *newp = mem_realloc(oldp, newsizeb);

    if (oldsizeb != (size_t)(-1) && newsizeb > oldsizeb) {
        bzero((char*)newp + oldsizeb, newsizeb - oldsizeb);
    }

    return newp;
}


void RDBMemFree (void *addr)
{
    mem_free(addr);
}


RDBBinary RDBBinaryNew (const void *addr, ub4 sz)
{
    RDBBinary bin;

    bin = RDBMemAlloc(sizeof(RDBBinary_t));
    if (bin) {
        bin->addr = RDBMemAlloc(sz);
        if (bin->addr) {
            memcpy(bin->addr, addr, sz);
            bin->sz = sz;
            return bin;
        } else {
            RDBMemFree(bin);
            return NULL;
        }
    }

    return NULL;
}


void RDBBinaryFree (RDBBinary bin)
{
    if (bin) {
        RDBMemFree(bin->addr);
        RDBMemFree(bin);
    }
}


/**********************************************************************
 *
 * RDBZString API
 *
 *********************************************************************/

RDBZString RDBZStringNew (const char *str, ub4 len)
{
    RDBZString_t *pzs;

    if (len == (ub4)(-1)) {
        len = (ub4)cstr_length(str, RDBZSTRING_LEN_MAX);
    }

    pzs = (RDBZString_t *) RDBMemAlloc(sizeof(*pzs) + len + 1);
    if (! pzs) {
        fprintf(stderr, "(%s:%d) out of memory.\n", __FILE__, __LINE__);
        exit(EXIT_FAILURE);
    }
    pzs->len = len;

    if (str) {
        memcpy(pzs->str, str, len);
    }

    return pzs;
}


void RDBZStringFree (RDBZString zs)
{
    RDBMemFree(zs);
}


ub4 RDBZStringLen (RDBZString zs)
{
    return (zs? zs->len : 0);
}


char * RDBZStringAddr (RDBZString zs)
{
    return (zs? zs->str : NULL);
}


RDBAPI_BOOL RedisCheckReplyStatus (redisReply *reply, const char *status, int stlen)
{
    if (reply && reply->type == REDIS_REPLY_STATUS &&
        ! cstr_notequal_len(reply->str, reply->len, status, stlen) ) {
        // true: 1
        return RDBAPI_TRUE;
    } else {
        // false: 0
        return RDBAPI_FALSE;
    }
}


void RedisFreeReplyObject (redisReply **pReply)
{
    redisReply *reply = *pReply;
    if (reply) {
        *pReply = NULL;

        freeReplyObject(reply);
    }
}


void RedisFreeReplyObjects (redisReply **replys, int numReplys)
{
    if (numReplys <= 1) {
        RedisFreeReplyObject(replys);
    } else {
        while (numReplys-- > 0) {
            redisReply *reply = replys[numReplys];
            if (reply) {
                replys[numReplys] = NULL;
                freeReplyObject(reply);
            }
        }
    }
}


/**
 *  RedisConnExecCommand/RedisExecCommand
 *
 *   https://github.com/redis/hiredis
 *   
 *   The return value of redisCommand holds a reply when the command was
 *   successfully executed. When an error occurs, the return value is NULL
 *   and the err field in the context will be set (see section on Errors).
 *
 *   Once an error is returned the context cannot be reused and you should
 *   set up a new connection.
 *
 * https://stackoverflow.com/questions/10041120/hiredis-c-socket
 */
redisReply * RedisExecCommand (RDBCtx ctx, int argc, const char **argv, const size_t *argvlen)
{
    redisReply * reply = NULL;

    *ctx->errmsg = 0;

    RDBCtxNode anode = RDBCtxGetActiveNode(ctx, NULL, 0);

    redisContext * redCtx = RDBCtxNodeGetRedisContext(anode);
    if (! redCtx) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "Active node not found");
        return NULL;
    }

    reply = (redisReply *) redisCommandArgv(redCtx, argc, argv, argvlen);

    if (! reply) {
        // failed to exec command
        if (redCtx->err == REDIS_ERR_IO) {
            /* hiredis/read.h
             *
             * When an error occurs, the err flag in a context is set to hold the type of
             * error that occurred. REDIS_ERR_IO means there was an I/O error and you
             * should use the "errno" variable to find out what is wrong.
             * For other values, the "errstr" field will hold a description.
             *
             * Add timeo_data can close this error!!
             */
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "redisCommandArgv REDIS_ERR_IO(errno=%d): %s", errno, strerror(errno));
        } else {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "redisCommandArgv RedisContext Error(%d): %.*s", redCtx->err, cstr_length(redCtx->errstr, RDB_ERROR_MSG_LEN), redCtx->errstr);
        }

        RDBCtxNodeClose(anode);
        return reply;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        // 'MOVED 7142 127.0.0.1:7002'
        // if remote call, 127.0.0.1 ?
        if (cstr_startwith(reply->str, reply->len, "MOVED ", 6)) {
            char * start = &(reply->str[6]);
            char * end = strchr(start, 32);

            if (end) {
                start = end;
                ++start;
                *end = 0;

                end = strchr(start, ':');

                if (end) {
                    *end++ = 0;

                    /**
                     * start => host
                     * end => port
                     */
                    redCtx = RDBCtxNodeGetRedisContext(RDBCtxGetActiveNode(ctx, start, (ub4) atoi(end)));
                    if (redCtx) {
                        RedisFreeReplyObject(&reply);
                        return RedisExecCommand(ctx, argc, argv, argvlen);
                    }
                }
            }
        } else if (cstr_startwith(reply->str, reply->len, "NOAUTH ", 7)) {
            /* 'NOAUTH Authentication required.' */
            RDBEnvNode enode = RDBCtxNodeGetEnvNode(anode);

            const char *cmds[] = { "auth", enode->authpass };

            RedisFreeReplyObject(&reply);

            reply = RedisExecCommand(ctx, sizeof(cmds)/sizeof(cmds[0]), cmds, 0);

            if (RedisIsReplyStatusOK(reply)) {
                // authentication success
                RedisFreeReplyObject(&reply);

                // do command
                return RedisExecCommand(ctx, argc, argv, argvlen);
            }

            if (reply) {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "AUTH failed(%d): %s", reply->type, reply->str);
                RedisFreeReplyObject(&reply);
            }

            return NULL;
        }

        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "REDIS_REPLY_ERROR: %s", reply->str);

        RedisFreeReplyObject(&reply);
        return NULL;
    }

    return reply;
}


RDBAPI_RESULT RedisExecArgvOnNode (RDBCtxNode ctxnode, int argc, const char *argv[], const size_t *argvlen, redisReply **outReply)
{
    *outReply = NULL;

    *ctxnode->ctx->errmsg = 0;

    if (! RDBCtxNodeIsOpen(ctxnode)) {
        RDBCtxNodeOpen(ctxnode);
    }

    if (RDBCtxNodeIsOpen(ctxnode)) {
        ctxnode->ctx->activenode = ctxnode;

        redisReply *reply = RedisExecCommand(ctxnode->ctx, argc, argv, argvlen);
        if (reply) {
            *outReply = reply;
            return RDBAPI_SUCCESS;
        }
    } else {
        snprintf_chkd_V1(ctxnode->ctx->errmsg, sizeof(ctxnode->ctx->errmsg), "RDBAPI_ERROR: No active node");
    }

    return RDBAPI_ERROR;
}


RDBAPI_RESULT RedisExecCommandOnNode (RDBCtxNode ctxnode, char *command, redisReply **outReply)
{
    const char *argv[256];
    char *p;

    int reply_id = 0;
    int argc = 0;
    int i = 0;

    RDBCtx ctx = ctxnode->ctx;

    *outReply = NULL;

    *ctx->errmsg = 0;

    if (! command || ! *command) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: null command");
        return RDBAPI_ERR_BADARG;
    }

    if (*command == 32) {
        argv[i] = strchr(command, 32);
    } else {
        argv[i] = command;
    }

    if (! argv[i]) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: bad command");
        return RDBAPI_ERR_BADARG;
    }

    while (i < 256 && argv[i] && (p = strchr(argv[i++], 32)) != 0) {
        *p++ = 0;

        while (*p == 32) {
            p++;
        }

        argv[i] = p;
    }

    argc = i;

    return RedisExecArgvOnNode(ctxnode, argc, argv, NULL, outReply);
}


int RedisExecCommandOnAllNodes (RDBCtx ctx, char *command, redisReply **replys, RDBCtxNode *replyNodes)
{
    const char *argv[256];
    char *p;

    int ok_nodes = 0;
    int argc = 0;
    int i = 0;

    int numnodes = ctx->env->clusternodes;

    *ctx->errmsg = 0;

    if (! replys) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "REDISAPI_EARG: bad replys");
        return RDBAPI_ERR_BADARG;
    }

    bzero(replys, sizeof(replys[0]) * numnodes);

    if (replyNodes) {
        bzero(replyNodes, sizeof(replyNodes[0]) * numnodes);
    }

    if (! command || ! *command) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "REDISAPI_EARG: bad command");
        return RDBAPI_ERR_BADARG;
    }

    if (*command == 32) {
        argv[i] = strchr(command, 32);
    } else {
        argv[i] = command;
    }

    if (! argv[i]) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "REDISAPI_EARG: bad command");
        return RDBAPI_ERR_BADARG;
    }

    while (i < 256 && argv[i] && (p = strchr(argv[i++], 32)) != 0) {
        *p++ = 0;

        while (*p == 32) {
            p++;
        }

        argv[i] = p;
    }

    argc = i;

    // Commands such as keys, scan cannot cross nodes.
    for (i = 0; i < numnodes; i++) {
        redisReply *reply = NULL;
        RDBCtxNode ctxnode = RDBCtxGetNode(ctx, i);

        if (! RDBCtxNodeIsOpen(ctxnode)) {
            RDBCtxNodeOpen(ctxnode);
        }

        if (replyNodes) {
            replyNodes[i] = ctxnode;
        }

        if (RedisExecArgvOnNode(ctxnode, argc, argv, NULL, &reply) == RDBAPI_SUCCESS) {
            replys[i] = reply;
            ok_nodes++;
        }
    }

    return ok_nodes;
}


static void redisReplyWatchCallbackPrint (int type, void *data, size_t datalen, void *wcbarg)
{
    switch (type) {
    case REDIS_REPLY_INTEGER:
        printf("(integer)  \"%"PRId64"\"\n", (int64_t) data);
        break;

    case REDIS_REPLY_ERROR:
        printf("(error)    \"%s\"\n", (char *) data);
        break;

    case REDIS_REPLY_STRING:
        if (wcbarg) {
            printf("[%"PRIu64"] (string)   \"%s\"\n", (size_t) wcbarg, (char *) data);
        } else {
            printf("(string)   \"%s\"\n", (char *) data);
        }
        break;

    case REDIS_REPLY_ARRAY:
        if (datalen > 0) {
            size_t i;

            printf("(array) elements(%"PRIu64"):\n", datalen);

            for (i = 0; i < datalen; i++) {
                RedisReplyObjectWatch(((redisReply **) data)[i], redisReplyWatchCallbackPrint, (void *) (i+1));
            }
        } else {
            printf("(array) empty list.\n");
        }
        break;

    case REDIS_REPLY_STATUS:
        printf("(status)   \"%s\"\n", (char *) data);
        break;

    case REDIS_REPLY_NIL:
        printf("(null)\n");
        break;
    }
}


void RedisReplyObjectWatch (redisReply *reply, redisReplyWatchCallback wcb, void *wcbarg)
{
    if (reply) {
        switch (reply->type) {
        case REDIS_REPLY_INTEGER:
            wcb(REDIS_REPLY_INTEGER, (void *) reply->integer, sizeof reply->integer, wcbarg);
            break;

        case REDIS_REPLY_ERROR:
        case REDIS_REPLY_STRING:
            wcb(reply->type, (void *) reply->str, reply->len, wcbarg);
            break;

        case REDIS_REPLY_ARRAY:
            wcb(REDIS_REPLY_ARRAY, (void *) reply->element, reply->elements, wcbarg);
            break;

        case REDIS_REPLY_STATUS:
            wcb(REDIS_REPLY_STATUS, (void *) reply->str, reply->len, wcbarg);
            break;

        case REDIS_REPLY_NIL:
            wcb(REDIS_REPLY_NIL, (void *) NULL, 0, wcbarg);
            break;
        }
    }
}


void RedisReplyObjectPrint (redisReply *reply, RDBCtxNode ctxnode)
{
    if (ctxnode) {
        // printf("\033[1;36m* redis node(%s)>\033[0m\n", RDBCtxNodeGetEnvNode(ctxnode)->key);

        printf("* redis node(%s) >\n", RDBCtxNodeGetEnvNode(ctxnode)->key);
    }

    RedisReplyObjectWatch(reply, redisReplyWatchCallbackPrint, (void*) 0);
}


char * RedisReplyDetachString (redisReply *reply, int *len)
{
    char *str = NULL;

    if (reply->type == REDIS_REPLY_STRING || reply->type == REDIS_REPLY_ERROR || reply->type == REDIS_REPLY_STATUS) {
        str = reply->str;

        if (len) {
            *len = (int) reply->len;
        }

        reply->str = NULL;
        reply->len = 0;
    }

    return str;
}


/**
 * Redis wrapper synchronized API
 */

// set key's expiration time in ms:
//   expire_ms = 0  : ignored
//   expire_ms = -1 : never expired
//   expire_ms > 0  : timeout by ms
RDBAPI_RESULT RedisExpireKey (RDBCtx ctx, const char *key, size_t keylen, sb8 expire_ms)
{
    const char *argv[3];
    size_t argl[3];
    char val[22];

    redisReply *reply = NULL;

    *ctx->errmsg = 0;

    if (expire_ms == 0) {
        // ignored
        RDBAPI_BOOL ok = RedisExistsKey(ctx, key, keylen);

        if (ok == RDBAPI_TRUE) {
            // key is ok
            return RDBAPI_SUCCESS;
        } else {
            // key not found
            return RDBAPI_ERR_NOKEY;
        }
    }

    if (expire_ms == RDBAPI_KEY_PERSIST) {
        // never expired
        argv[0] = "persist";
        argv[1] = key;

        argl[0] = 7;
        argl[1] = (keylen == -1? strlen(key) : keylen);

        reply = RedisExecCommand(ctx, 2, argv, argl);
        if (! reply) {
            // error
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERROR: RedisExecCommand failed");
            return RDBAPI_ERROR;
        }

        if (reply->type == REDIS_REPLY_INTEGER) {
            if (reply->integer == 0 || reply->integer == 1) {
                // success
                RedisFreeReplyObject(&reply);
                return RDBAPI_SUCCESS;
            } else {
                // bad reply integer
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_RETVAL: reply integer(%"PRId64")", (sb8) reply->integer);

                RedisFreeReplyObject(&reply);
                return RDBAPI_ERR_RETVAL;
            }
        }

        // bad reply type
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_TYPE: reply type(%d)", reply->type);
        RedisFreeReplyObject(&reply);

        return RDBAPI_ERR_TYPE;
    }

    if (expire_ms % 1000 == 0 || expire_ms > 60000) {
        // expire key seconds
        argv[0] = "expire";
        argv[1] = key;
        argv[2] = val;

        argl[0] = 6;
        argl[1] = (keylen == -1? strlen(key) : keylen);

        argl[2] = snprintf_chkd_V1(val, sizeof(val), "%"PRId64"", expire_ms / 1000);
    } else {
        // pexpire key ms
        argv[0] = "pexpire";
        argv[1] = key;
        argv[2] = val;

        argl[0] = 7;
        argl[1] = (keylen == -1? strlen(key) : keylen);

        argl[2] = snprintf_chkd_V1(val, sizeof(val), "%"PRId64"", expire_ms);
    }

    val[21] = 0;

    reply = RedisExecCommand(ctx, 3, argv, argl);
    if (! reply) {
        return RDBAPI_ERROR;
    }

    if (reply->type == REDIS_REPLY_INTEGER) {
        if (reply->integer == 1) {
            // success
            RedisFreeReplyObject(&reply);
            return RDBAPI_SUCCESS;
        } else if (reply->integer == 0) {
            // key not found
            RedisFreeReplyObject(&reply);
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_NOKEY: not found key(%s)", key);
            return RDBAPI_ERR_NOKEY;
        } else {
            // bad reply integer
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_RETVAL: reply integer(%"PRId64")", (sb8) reply->integer);
            RedisFreeReplyObject(&reply);
            return RDBAPI_ERR_RETVAL;
        }
    }

    // bad reply type
    snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_TYPE: reply type(%d)", reply->type);
    RedisFreeReplyObject(&reply);
    return RDBAPI_ERR_TYPE;
}


RDBAPI_RESULT RedisSetKey (RDBCtx ctx, const char *key, size_t keylen, const char *value, size_t valuelen, sb8 expire_ms)
{
    redisReply *reply;

    const char *argv[5];
    size_t argl[5];

    char str[22] = {0};

    int argc = 3;

    argv[0] = "set";
    argl[0] = 3;

    argv[1] = key;
    argl[1] = (keylen == -1 ? strlen(key) : keylen);

    argv[2] = value;
    argl[2] = valuelen;

    if (expire_ms > 0) {
        if (expire_ms % 1000 == 0 || expire_ms > 60000) {
            argv[3] = "EX";
            argl[3] = 2;

            argl[4] = snprintf_chkd_V1(str, sizeof(str), "%"PRId64"", expire_ms / 1000);
            argv[4] = str;

            argc = 5;
        } else {
            argv[3] = "PX";
            argl[3] = 2;

            argl[4] = snprintf_chkd_V1(str, sizeof(str), "%"PRId64"", expire_ms);
            argv[4] = str;

            argc = 5;
        }
    }

    reply = RedisExecCommand(ctx, argc, argv, argl);

    if (RedisIsReplyStatusOK(reply)) {
        RedisFreeReplyObject(&reply);
        return RDBAPI_SUCCESS;
    }

    RedisFreeReplyObject(&reply);
    return RDBAPI_ERROR;
}


RDBAPI_RESULT RedisHMSet (RDBCtx ctx, const char *key, const char * fields[], const char *values[], const size_t *valueslen, sb8 expire_ms)
{
    redisReply *reply;

    int i, argc;

    const char *argv[RDBAPI_ARGV_MAXNUM * 2 + 4];
    size_t argvlen[RDBAPI_ARGV_MAXNUM * 2 + 4];

    const char *fld, *val;

    i = 0;

    argv[0] = "hmset";
    argvlen[0] = 5;

    argv[1] = key;
    argvlen[1] = strlen(key);

    argc = 2;

    while ((fld = fields[i]) != 0) {
        val = values[i];

        // fld = 2, 4, 6, ...
        argv[i*2 + 2] = fld;
        argvlen[i*2 + 2] = strlen(fld);

        // val  = 3, 5, 7, ...
        argv[i*2 + 3] = val;
        argvlen[i*2 + 3] = (valueslen? valueslen[i] : strlen(val));

        argc += 2;

        if (i++ == RDBAPI_ARGV_MAXNUM) {
            break;
        }
    }

    if (i > RDBAPI_ARGV_MAXNUM) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: too many fields.");
        return RDBAPI_ERR_BADARG;
    }

    reply = RedisExecCommand(ctx, argc, argv, argvlen);
    if (! reply) {
        return RDBAPI_ERROR;
    }

    if (RedisIsReplyStatusOK(reply)) {
        // success
        RedisFreeReplyObject(&reply);

        // set time out for key
        return RedisExpireKey(ctx, key, -1, expire_ms);
    } else {
        // bad reply type
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_TYPE: reply type(%d)", reply->type);

        RedisFreeReplyObject(&reply);
        return RDBAPI_ERR_TYPE;
    }
}


RDBAPI_RESULT RedisHMSetLen (RDBCtx ctx, const char *key, size_t keylen, const char * fields[], const size_t *fieldslen, const char *values[], const size_t *valueslen, sb8 expire_ms)
{
    redisReply *reply;

    int i, argc;

    const char *argv[RDBAPI_ARGV_MAXNUM * 2 + 4];
    size_t argvlen[RDBAPI_ARGV_MAXNUM * 2 + 4];

    const char *fld, *val;

    i = 0;

    argv[0] = "hmset";
    argvlen[0] = 5;

    argv[1] = key;
    argvlen[1] = keylen;

    argc = 2;

    while ((fld = fields[i]) != 0) {
        val = values[i];

        // fld = 2, 4, 6, ...
        argv[i*2 + 2] = fld;
        argvlen[i*2 + 2] = fieldslen[i];

        // val  = 3, 5, 7, ...
        argv[i*2 + 3] = val;
        argvlen[i*2 + 3] = valueslen[i];

        argc += 2;

        if (i++ == RDBAPI_ARGV_MAXNUM) {
            break;
        }
    }

    if (i > RDBAPI_ARGV_MAXNUM) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: too many fields.");
        return RDBAPI_ERR_BADARG;
    }

    reply = RedisExecCommand(ctx, argc, argv, argvlen);
    if (! reply) {
        return RDBAPI_ERROR;
    }

    if (RedisIsReplyStatusOK(reply)) {
        // success
        RedisFreeReplyObject(&reply);

        // set time out for key
        return RedisExpireKey(ctx, key, keylen, expire_ms);
    } else {
        // bad reply type
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_TYPE: reply type(%d)", reply->type);

        RedisFreeReplyObject(&reply);
        return RDBAPI_ERR_TYPE;
    }
}

/**
 * redis command:
 *  redis > hmget key host port clientid
 *
 *  redisReply *reply;
 *
 *  const char * fields[] = {
 *      "host",
 *      "port",
 *      "clientid",
 *      0  // must be 0 as the last
 *  };
 *
 *  int ret = RedisHMGet(redisconn, key, fields, &reply);
 *
 *  if (ret == RDBAPI_SUCCESS) {
 *      printf("%s=%s %s=%s %s=%s\n",
 *          fields[0],
 *          reply->element[0]->str ? reply->element[0]->str : "(nil)",
 *          fields[1],
 *          reply->element[1]->str ? reply->element[1]->str : "(nil)",
 *          fields[2],
 *          reply->element[2]->str ? reply->element[2]->str : "(nil)"
 *      );
 *
 *      RedisFreeReplyObject(&reply);
 *  }
 */
RDBAPI_RESULT RedisHMGet (RDBCtx ctx, const char * key, const char * fields[], redisReply **outReply)
{
    return RedisHMGetLen(ctx, key, -1, fields, NULL, outReply);
}


RDBAPI_RESULT RedisHMGetLen (RDBCtx ctx, const char * key, size_t keylen, const char * fields[], const size_t *fieldslen, redisReply **outReply)
{
    redisReply *reply = NULL;

    if (fields && fields[0]) {
        int i, argc = 0;

        const char *argv[RDBAPI_ARGV_MAXNUM + 4] = {0};
        size_t argvlen[RDBAPI_ARGV_MAXNUM + 4] = {0};

        const char *fld;

        argv[0] = "hmget";
        argvlen[0] = 5;

        argv[1] = key;
        argvlen[1] = (keylen == -1)? strlen(key) : keylen;

        i = 0;
        argc = 2;
        while ((fld = fields[i]) != 0) {
            argv[2 + i] = fld;
            argvlen[2 + i] = (fieldslen? fieldslen[i] : strlen(fld));
            argc++;

            if (i++ == RDBAPI_ARGV_MAXNUM) {
                break;
            }
        }

        if (i > RDBAPI_ARGV_MAXNUM) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: too many fields.");
            return RDBAPI_ERR_BADARG;
        }

        reply = RedisExecCommand(ctx, argc, argv, argvlen);
        if (! reply) {
            ctx->errmsg[0] = 0;
            return RDBAPI_ERROR;
        }

        if (reply->type != REDIS_REPLY_ARRAY) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_TYPE: reply type(%d)", reply->type);

            RedisFreeReplyObject(&reply);
            return RDBAPI_ERR_TYPE;
        }

        if (reply->elements != argc - 2) {
            // bad reply elements
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_APP: reply elements(%"PRIu64"). required(%d).", reply->elements, argc);
            RedisFreeReplyObject(&reply);
            return RDBAPI_ERR_APP;
        }
    } else {
        const char *argv[2];
        size_t argl[2];

        argv[0] = "hgetall";
        argv[1] = key;

        argl[0] = 7;
        argl[1] = (keylen == -1)? strlen(key) : keylen;

        reply = RedisExecCommand(ctx, 2, argv, argl);
        if (! reply) {
            ctx->errmsg[0] = 0;
            return RDBAPI_ERROR;
        }

        if (reply->type != REDIS_REPLY_ARRAY) {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_TYPE: reply type(%d)", reply->type);

            RedisFreeReplyObject(&reply);
            return RDBAPI_ERR_TYPE;
        }
    }

    // all is ok
    *outReply = reply;
    return RDBAPI_SUCCESS;
}


// 1: del key ok
// 0: key not found
int RedisDeleteKey (RDBCtx ctx, const char * key, size_t keylen, const char * fields[], int numfields)
{
    redisReply *reply = NULL;

    if (numfields > RDBAPI_ARGV_MAXNUM) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: too many fields(%d)", numfields);
        return RDBAPI_ERR_BADARG;
    }

    if (numfields < 0) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: invalid numfields(%d)", numfields);
        return RDBAPI_ERR_BADARG;
    }

    if (! fields || ! numfields) {
        const char *argv[] = { "del", key };
        size_t argvlen[] = { 3, (keylen == -1? strlen(key) : keylen) };

        numfields = 1;

        reply = RedisExecCommand(ctx, 2, argv, argvlen);
    } else {
        int i, argc;

        const char *fld;

        const char *argv[RDBAPI_ARGV_MAXNUM + 4];
        size_t argvlen[RDBAPI_ARGV_MAXNUM + 4];

        argv[0] = "hdel";
        argvlen[0] = 4;

        argv[1] = key;
        argvlen[1] = (keylen == -1? strlen(key) : keylen);

        for (i = 0; i < numfields; i++) {
            fld = fields[i];
            argv[i + 2] = fld;
            argvlen[i + 2] = strlen(fld);
        }

        argc = i + 2;

        reply = RedisExecCommand(ctx, 2, argv, argvlen);
    }

    if (! reply) {
        return RDBAPI_ERROR;
    }

    if (reply->type != REDIS_REPLY_INTEGER) {
        // bad reply type
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_TYPE: reply type(%d)", reply->type);

        RedisFreeReplyObject(&reply);
        return RDBAPI_ERR_TYPE;
    }

    if ((int) reply->integer == numfields) {
        RedisFreeReplyObject(&reply);
        return numfields;
    } else if (reply->integer == 0) {
        RedisFreeReplyObject(&reply);
        return RDBAPI_KEY_NOTFOUND;
    } else {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_RETVAL: reply integer(%d)", (int)reply->integer);
        RedisFreeReplyObject(&reply);
        return RDBAPI_ERR_RETVAL;
    }
}


int RedisExistsKey (RDBCtx ctx, const char * key, size_t keylen)
{
    int ret;
    redisReply *reply = NULL;

    const char *argv[] = { "exists", key };
    size_t argvlen[] = { 6, (keylen == -1? strlen(key) : keylen) };

    *ctx->errmsg = 0;

    reply = RedisExecCommand(ctx, 2, argv, argvlen);

    if (! reply) {
        return RDBAPI_ERROR;
    }

    if (reply->type != REDIS_REPLY_INTEGER) {
        // bad reply type: REDIS_REPLY_INTEGER expected
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_TYPE: reply type(%d)", reply->type);

        RedisFreeReplyObject(&reply);
        return RDBAPI_ERR_TYPE;
    }

    // 1: key exists
    // 0: key not exists
    ret = (int) reply->integer;

    RedisFreeReplyObject(&reply);
    return ret;
}


RDBAPI_RESULT RedisClusterKeyslot (RDBCtx ctx, const char *key, sb8 *slot)
{
    redisReply *reply = NULL;

    const char *argv[3] = { "cluster", "keyslot", key };
    size_t argvlen[3] = { 7, 7, strlen(key) };

    reply = RedisExecCommand(ctx, 3, argv, argvlen);
    if (! reply) {
        return RDBAPI_ERROR;
    }

    if (reply->type != REDIS_REPLY_INTEGER) {
        // bad reply type: REDIS_REPLY_INTEGER required
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_TYPE: reply type(%d)", reply->type);
        RedisFreeReplyObject(&reply);
        return RDBAPI_ERR_TYPE;
    }

    *slot = (sb8) reply->integer;

    RedisFreeReplyObject(&reply);
    return RDBAPI_SUCCESS;
}


// redis command: multi
RDBAPI_RESULT RedisTransStart (RDBCtx ctx)
{
    redisReply *reply;
    const char *cmds[] = { "multi", '\0' };

    reply = RedisExecCommand(ctx, 1, cmds, 0);

    if (RedisIsReplyStatusOK(reply)) {
        RedisFreeReplyObject(&reply);
        return RDBAPI_SUCCESS;
    }

    RedisFreeReplyObject(&reply);
    return RDBAPI_ERROR;
}


// redis command: exec
RDBAPI_RESULT RedisTransCommit (RDBCtx ctx, redisReply **outReply)
{
    redisReply *reply;
    const char *cmds[] = { "exec", '\0' };

    reply = RedisExecCommand(ctx, 1, cmds, 0);

    if (RedisIsReplyStatusOK(reply)) {
        *outReply = reply;
        return RDBAPI_SUCCESS;
    }

    RedisFreeReplyObject(&reply);
    return RDBAPI_ERROR;
}


// redis command: discard
void RedisTransDiscard (RDBCtx ctx)
{
    redisReply *reply;
    const char *cmds[] = { "discard", '\0' };

    reply = RedisExecCommand(ctx, 1, cmds, 0);

    RedisFreeReplyObject(&reply);
}


RDBAPI_RESULT RedisWatchKey (RDBCtx ctx, const char *key)
{
    redisReply *reply;
    const char *cmds[] = { "watch", key, 0 };

    reply = RedisExecCommand(ctx, 2, cmds, 0);

    if (RedisIsReplyStatusOK(reply)) {
        RedisFreeReplyObject(&reply);
        return RDBAPI_SUCCESS;
    }

    RedisFreeReplyObject(&reply);
    return RDBAPI_ERROR;
}


RDBAPI_RESULT RedisWatchKeys (RDBCtx ctx, const char *keys[], int numkeys)
{
    redisReply *reply;
    const char *argv[RDBAPI_ARGV_MAXNUM + 4];
    int argc = 0;

    while (argc < RDBAPI_ARGV_MAXNUM && keys[argc]) {
        argv[argc + 1] = keys[argc];
        argc++;
    }

    if (argc == 0 || argc >= RDBAPI_ARGV_MAXNUM) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_BADARG: error numkeys(%d)", numkeys);
        return RDBAPI_ERR_BADARG;
    }

    if (argc == 1) {
        return RedisWatchKey(ctx, keys[0]);
    }

    argv[0] = "watch";
    argc++;

    reply = RedisExecCommand(ctx, argc, argv, 0);

    if (RedisIsReplyStatusOK(reply)) {
        RedisFreeReplyObject(&reply);
        return RDBAPI_SUCCESS;
    }

    RedisFreeReplyObject(&reply);
    return RDBAPI_ERROR;
}


void RedisUnwatch (RDBCtx ctx)
{
    redisReply *reply;

    const char *cmds[] = { "unwatch", 0 };

    reply = RedisExecCommand(ctx, 1, cmds, 0);

    RedisFreeReplyObject(&reply);
}


RDBAPI_RESULT RedisGenerateId (RDBCtx ctx, const char *generator, int count, sb8 *genid, int reset)
{
    redisReply *reply;

    if (count > 1) {
        // redis:port > INCRBY generator count
        char bycount[22];

        snprintf_chkd_V1(bycount, sizeof(bycount), "%d", count);
        bycount[21] = 0;

        const char *cmds[] = { "incrby", generator, bycount };

        reply = RedisExecCommand(ctx, 3, cmds, 0);
    } else {
        // redis:port > INCR generator
        const char *cmds[] = { "incr", generator };

        reply = RedisExecCommand(ctx, 2, cmds, 0);
    }

    if (reply) {
        if (reply->type == REDIS_REPLY_INTEGER) {
            *genid = (sb8) reply->integer;

            RedisFreeReplyObject(&reply);
            return RDBAPI_SUCCESS;
        }
    } else if (reset) {
        // reset id generator
        const char * cmds[] = { "set", generator, "0" };

        reply = RedisExecCommand(ctx, 3, cmds, 0);

        if (RedisIsReplyStatusOK(reply)) {
            // reset ok
            RedisFreeReplyObject(&reply);

            // get id again without reset
            return RedisGenerateId(ctx, generator, count, genid, 0);
        }
    }

    RedisFreeReplyObject(&reply);
    return RDBAPI_ERROR;
}


RDBAPI_RESULT RedisIncrIntegerField (RDBCtx ctx, const char *key, const char *field, sb8 increment, sb8 *retval, int reset)
{
    redisReply *reply;

    // redis:port > HINCRBY key field increment
    char incrbuf[22];

    snprintf_chkd_V1(incrbuf, sizeof(incrbuf), "%"PRId64, increment);

    const char *cmds[] = {"hincrby", key, field, incrbuf};

    reply = RedisExecCommand(ctx, 4, cmds, 0);

    if (reply) {
        if (reply->type == REDIS_REPLY_INTEGER) {
            *retval = (sb8) reply->integer;

            RedisFreeReplyObject(&reply);
            return RDBAPI_SUCCESS;
        }
    } else if (reset) {
        // reset id generator
        const char * cmds[] = {"hset", key, field, "0"};

        reply = RedisExecCommand(ctx, 4, cmds, 0);

        if (reply->type == REDIS_REPLY_INTEGER) {
            if (    reply->integer == 0     // existed field
                ||  reply->integer == 1     // new field
                ) {
                RedisFreeReplyObject(&reply);

                // reset ok. get id again without reset
                return RedisIncrIntegerField(ctx, key, field, increment, retval, 0);
            }
        }
    }

    RedisFreeReplyObject(&reply);
    return RDBAPI_ERROR;
}


RDBAPI_RESULT RedisIncrFloatField (RDBCtx ctx, const char *key, const char *field, double increment, double *retval, int reset)
{
    redisReply *reply;

    // redis:port > HINCRBY key field increment
    char incrbuf[42];

    snprintf_chkd_V1(incrbuf, sizeof(incrbuf), "%e", increment);

    const char *cmds[] = {"hincrbyfloat", key, field, incrbuf};

    reply = RedisExecCommand(ctx, 4, cmds, 0);

    if (reply) {
        if (reply->type == REDIS_REPLY_STRING) {
            char *endpos = 0;
            errno = 0;

            *retval = strtod(reply->str, &endpos);

            if (endpos == reply->str || errno == ERANGE) {
                RedisFreeReplyObject(&reply);
                return RDBAPI_ERROR;
            }

            RedisFreeReplyObject(&reply);
            return RDBAPI_SUCCESS;
        } else if (reply->type == REDIS_REPLY_INTEGER) {
            *retval = (double) reply->integer;

            RedisFreeReplyObject(&reply);
            return RDBAPI_SUCCESS;
        }
    } else if (reset) {
        const char * cmds[] = {"hset", key, field, "0"};

        reply = RedisExecCommand(ctx, 4, cmds, 0);

        if (reply->type == REDIS_REPLY_INTEGER) {
            if (    reply->integer == 0     // existed field
                ||  reply->integer == 1     // new field
                ) {
                RedisFreeReplyObject(&reply);

                // reset ok. get id again without reset
                return RedisIncrFloatField(ctx, key, field, increment, retval, 0);
            }
        }
    }

    RedisFreeReplyObject(&reply);
    return RDBAPI_ERROR;
}


RDBAPI_RESULT RedisClusterCheck (RDBCtx ctx)
{
    RDBAPI_RESULT result;

    char propval[RDBAPI_PROP_MAXSIZE];

    int nodeindex, numnodes;

    numnodes = RDBEnvNumNodes(ctx->env);
    if (numnodes == 0) {
        snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "nodes not found");
        return RDBAPI_ERROR;
    }

    result = RDBCtxCheckInfo(ctx, NODEINFO_CLUSTER, 0);
    if (result != RDBAPI_SUCCESS) {
        return RDBAPI_ERROR;
    }

    result = RDBCtxCheckInfo(ctx, NODEINFO_REPLICATION, 0);
    if (result != RDBAPI_SUCCESS) {
        return RDBAPI_ERROR;
    }

    threadlock_lock(&ctx->env->thrlock);

    for (nodeindex = 0; nodeindex < numnodes; nodeindex++) {
        int len;
        RDBCtxNode ctxnode = RDBCtxGetNode(ctx, nodeindex);
        RDBEnvNode envnode = RDBCtxNodeGetEnvNode(ctxnode);

        bzero(&envnode->replica, sizeof(envnode->replica));

        len = RDBNodeInfoQuery(ctxnode, NODEINFO_CLUSTER, "cluster_enabled", propval);

        // check cluster_enabled
        if (len != 1 || propval[0] != '1') {
            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERROR: cluster_enabled=%s", propval);
            threadlock_unlock(&ctx->env->thrlock);
            return RDBAPI_ERROR;
        }        
    
        len = RDBNodeInfoQuery(ctxnode, NODEINFO_REPLICATION, "role", propval);
        if (len > 0) {
            if (cstr_startwith(propval, len, "master", 6)) {
                envnode->replica.is_master = 1;

                len = RDBNodeInfoQuery(ctxnode, NODEINFO_REPLICATION, "connected_slaves", propval);
                if (len == 1) {
                    int slaveid;
                    char slave[10] = {0};

                    envnode->replica.connected_slaves = atoi(propval);

                    for (slaveid = 0; slaveid < envnode->replica.connected_slaves; slaveid++) {
                        RDBEnvNode slavenode = NULL;

                        snprintf_chkd_V1(slave, sizeof(slave), "slave%d", slaveid);

                        len = RDBNodeInfoQuery(ctxnode, NODEINFO_REPLICATION, slave, propval);
                        if (len > 0) {
                            // ip=192.168.39.111,port=7006,state=online,offset=50091828,lag=1
                            char *ip;
                            char *port;
                            char *state;

                            ip = strstr(propval, "ip=");
                            port = strstr(propval, ",port=");
                            state = strstr(propval, ",state=");

                            if (ip && port && state) {
                                ip += 3;

                                *port = 0;
                                port += 6;

                                *state = 0;
                                state += 7;

                                slavenode = RDBEnvFindNode(ctx->env, ip, (ub4) atoi(port));
                            }
                        }

                        if (! slavenode) {
                            snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERROR: slave%d node not found", slaveid);
                            threadlock_unlock(&ctx->env->thrlock);
                            return RDBAPI_ERROR;
                        }

                        // set nodeindex for slave
                        envnode->replica.slaves[slaveid] = slavenode->index;
                    }
                }
            } else if (cstr_startwith(propval, len, "slave", 5)) {
                RDBEnvNode masterenode = NULL;

                envnode->replica.is_master = 0;

                //role:slave
                //master_host:192.168.39.111
                //master_port:7001
                len = RDBNodeInfoQuery(ctxnode, NODEINFO_REPLICATION, "master_port", propval);
                if (len > 0) {
                    len = RDBNodeInfoQuery(ctxnode, NODEINFO_REPLICATION, "master_host", propval + 20);
                    if (len > 0) {
                        masterenode = RDBEnvFindNode(ctx->env, propval + 20, (ub4) atoi(propval));
                    }
                }

                if (! masterenode) {
                    snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERROR: master node not found");
                    threadlock_unlock(&ctx->env->thrlock);
                    return RDBAPI_ERROR;
                }

                envnode->replica.master_nodeindex = masterenode->index;
            } else {
                snprintf_chkd_V1(ctx->errmsg, sizeof(ctx->errmsg), "RDBAPI_ERR_APP: role prop not found: %s", propval);
                threadlock_unlock(&ctx->env->thrlock);
                return RDBAPI_ERR_APP;
            }
        }
    }

    threadlock_unlock(&ctx->env->thrlock);
    return RDBAPI_SUCCESS;
}
