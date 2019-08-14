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
 * rdbctx.c
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#include "rdbapi.h"
#include "rdbtypes.h"


RDBAPI_RESULT RDBCtxCreate (RDBEnv env, RDBCtx *outctx)
{
    int i = 0;

    RDBCtx ctx = (RDBCtx_t *) RDBMemAlloc(sizeof(RDBCtx_t) + env->clusternodes * sizeof(RDBCtxNode_t));

    ctx->env = env;
    ctx->activenode = NULL;

    for (; i < env->clusternodes; i++) {
        ctx->nodes[i].ctx = ctx;
        ctx->nodes[i].index = i;
        ctx->nodes[i].slot = -1;
    }

    if (RedisClusterCheck(ctx) != RDBAPI_SUCCESS) {
        printf("RedisClusterCheck failed: %s\n", ctx->errmsg);
        RDBMemFree(ctx);
        return RDBAPI_ERROR;
    }

    *outctx = ctx;
    return RDBAPI_SUCCESS;
}


void RDBCtxFree (RDBCtx ctx)
{
    int i = 0;
    int numnodes = RDBEnvNumNodes(RDBCtxGetEnv(ctx));

    for (; i < numnodes; i++) {
        RDBCtxNode node = RDBCtxGetNode(ctx, i);
        RDBCtxNodeClose(node);
    }

    RDBMemFree(ctx);    
}


RDBEnv RDBCtxGetEnv (RDBCtx ctx)
{
    if (ctx) {
        return ctx->env;
    } else {
        return NULL;
    }
}


const char * RDBCtxErrMsg (RDBCtx ctx)
{
    return ctx->errmsg;
}


RDBCtxNode RDBCtxGetNode (RDBCtx ctx, int nodeindex)
{
    int numnodes = RDBEnvNumNodes(RDBCtxGetEnv(ctx));

    if (nodeindex < 0 || nodeindex >= numnodes) {
        return NULL;
    } else {
        return (&ctx->nodes[nodeindex]);
    }
}


RDBCtxNode RDBCtxGetActiveNode (RDBCtx ctx, const char *hostp, ub4 port)
{
    int index;
    int numnodes = RDBEnvNumNodes(RDBCtxGetEnv(ctx));

    if (! hostp) {
        if (ctx->activenode && ctx->activenode->redCtx) {
            return ctx->activenode;
        }

        ctx->activenode = NULL;

        for (index = 0; index < numnodes; index++) {
            if (ctx->nodes[index].redCtx) {
                ctx->activenode = &(ctx->nodes[index]);
                return ctx->activenode;
            }
        }

        // all nodes are deactive, create connect and return at once
        for (index = 0; index < numnodes; index++) {
            RDBCtxNode node = RDBCtxGetNode(ctx, index);

            if (RDBCtxNodeOpen(node) == RDBAPI_SUCCESS) {
                return RDBCtxGetActiveNode(ctx, NULL, (ub4) -1);
            }
        }
    } else {
        RDBEnvNode envnode = RDBEnvFindNode(RDBCtxGetEnv(ctx), hostp, port);
        if (! envnode) {
            return NULL;
        }

        index = RDBEnvNodeIndex(envnode);

        if (ctx->nodes[index].redCtx) {
            ctx->activenode = &(ctx->nodes[index]);
            return ctx->activenode;
        }

        if (RDBCtxNodeOpen(&ctx->nodes[index]) == RDBAPI_SUCCESS) {
            return ctx->activenode;
        }
    }

    return NULL;
}


RDBAPI_BOOL RDBCtxNodeIsOpen (RDBCtxNode ctxnode)
{
    return ((ctxnode && ctxnode->redCtx) ? RDBAPI_TRUE : RDBAPI_FALSE);
}


RDBAPI_RESULT RDBCtxNodeOpen (RDBCtxNode ctxnode)
{
    redisContext * redCtx = NULL;

    RDBCtx ctx = RDBCtxNodeGetCtx(ctxnode);
    RDBEnvNode envnode = RDBCtxNodeGetEnvNode(ctxnode);

    RDBCtxNodeClose(ctxnode);

    if (envnode->ctxtimeo.tv_sec) {
        redCtx = redisConnectWithTimeout(envnode->host, envnode->port, envnode->ctxtimeo);
    } else {
        redCtx = redisConnect(envnode->host, envnode->port);
    }

    if (! redCtx) {
        // redis connect failed
        return RDBAPI_ERROR;
    }

    if (envnode->sotimeo.tv_sec || envnode->sotimeo.tv_usec) {
        if (redisSetTimeout(redCtx, envnode->sotimeo) == REDIS_ERR) {
            // set timeout failed
            redisFree(redCtx);
            return RDBAPI_ERROR;
        }
    }

    ctxnode->redCtx = redCtx;
    ctx->activenode = ctxnode;

    return RDBAPI_SUCCESS;
}


void RDBCtxNodeClose (RDBCtxNode ctxnode)
{
    RDBCtx ctx = RDBCtxNodeGetCtx(ctxnode);

    if (ctxnode->redCtx) {
        redisContext * redCtx = ctxnode->redCtx;
        ctxnode->redCtx = NULL;

        redisFree(redCtx);
    }

    if (ctx->activenode == ctxnode) {
        ctx->activenode = NULL;
    }

    ctxnode->slot = -1;
}


int RDBCtxNodeIndex (RDBCtxNode ctxnode)
{
    return ctxnode->index;
}


RDBCtx RDBCtxNodeGetCtx (RDBCtxNode ctxnode)
{
    return ctxnode->ctx;
}


redisContext * RDBCtxNodeGetRedisContext (RDBCtxNode ctxnode)
{
    return (ctxnode ? ctxnode->redCtx : NULL);
}


RDBEnvNode RDBCtxNodeGetEnvNode (RDBCtxNode ctxnode)
{
    return RDBEnvGetNode(ctxnode->ctx->env, ctxnode->index);
}


RDBAPI_RESULT RDBCtxNodeCheckInfo (RDBCtxNode ctxnode, RDBNodeInfoSection section)
{
    RDBAPI_RESULT result;
    redisReply *reply;

    RDBEnvNode envnode;
    RDBPropMap propmap;

    const char *sections[] = {
        "SERVER",        // 0
        "CLIENTS",       // 1
        "MEMORY",        // 2
        "PERSISTENCE",   // 3
        "STATS",         // 4
        "REPLICATION",   // 5
        "CPU",           // 6
        "CLUSTER",       // 7
        "KEYSPACE",      // 8
        0
    };

    size_t argvlen[] = {4, 0};
    const char *argv[] = {"info", 0};

    if ((int) section < (int) NODEINFO_SERVER || (int) section >= (int) MAX_NODEINFO_SECTIONS) {
        snprintf(ctxnode->ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_BADARG: invalid section(%d)", (int)section);
        return RDBAPI_ERR_BADARG;
    }

    envnode = RDBCtxNodeGetEnvNode(ctxnode);
    propmap = envnode->nodeinfo[section];

    envnode->nodeinfo[section] = NULL;

    argv[1] = sections[(int) section];
    argvlen[1] = strlen(argv[1]);

    // clear old info map
    RDBPropMapFree(propmap);
    propmap = NULL;

    result = RedisExecArgvOnNode(ctxnode, 2, argv, argvlen, &reply);
    if (result == RDBAPI_SUCCESS) {
        if (reply->type != REDIS_REPLY_STRING) {
            snprintf(ctxnode->ctx->errmsg, RDB_ERROR_MSG_LEN, "RDBAPI_ERR_TYPE: reply type(%d)", reply->type);
            RedisFreeReplyObject(&reply);
            return RDBAPI_ERR_TYPE;
        }

        char *mapbuf = (char *) RDBMemAlloc(reply->len + 1);
        memcpy(mapbuf, reply->str, reply->len);
        mapbuf[reply->len] = 0;

        char *p = strtok(mapbuf, "\r\n");
        while (p) {
            char *key = 0;
            char *val = 0;

            if (p[0] != '#') {
                key = p;
                val = strchr(p, ':');
            }

            p = strtok(NULL, "\r\n");

            if (key && val) {
                RDBPropNode propnode = (RDBPropNode) RDBMemAlloc(sizeof(RDBProp_t));

                // skip ':'
                *val++ = 0;

                propnode->name = key;
                propnode->value = val;
                propnode->_mapbuf = mapbuf;

                HASH_ADD_STR(propmap, name, propnode);
            }
        }

        if (! propmap) {
            // no map created
            RDBMemFree(mapbuf);
        }
    }

    envnode->nodeinfo[section] = propmap;

    RedisFreeReplyObject(&reply);
    return result;
}


RDBAPI_RESULT RDBCtxCheckInfo (RDBCtx ctx, RDBNodeInfoSection section)
{
    if (section < NODEINFO_SERVER || section > MAX_NODEINFO_SECTIONS) {
        snprintf(ctx->errmsg, RDB_ERROR_MSG_LEN, "(%s:%d) RDBAPI_ERR_BADARG: invalid section(%d)", __FILE__, __LINE__, (int) section);
        return RDBAPI_ERR_BADARG;
    } else {
        RDBCtxNode ctxnode;
        int nodeindex = RDBEnvNumNodes(ctx->env);

        while (nodeindex-- > 0) {
            ctxnode = RDBCtxGetNode(ctx, nodeindex);

            if (section == MAX_NODEINFO_SECTIONS) {
                RDBNodeInfoSection secid;

                for (secid = NODEINFO_SERVER; secid != MAX_NODEINFO_SECTIONS; secid++) {
                    if (RDBCtxNodeCheckInfo(ctxnode, secid) != RDBAPI_SUCCESS) {
                        return RDBAPI_ERROR;
                    }
                }
            } else {
                if (RDBCtxNodeCheckInfo(ctxnode, section) != RDBAPI_SUCCESS) {
                    return RDBAPI_ERROR;
                }
            }
        }

        return RDBAPI_SUCCESS;
    }
}


static void RDBCtxNodePrintInfo (RDBCtxNode ctxnode, const char *sections[])
{
    int section;

    printf("\n# Node[%d]\n", ctxnode->index);

    for (section = (int) NODEINFO_SERVER; section != (int) MAX_NODEINFO_SECTIONS; section++) {
        RDBEnvNode envnode = RDBCtxNodeGetEnvNode(ctxnode);
        RDBPropMap propmap = envnode->nodeinfo[section];

        RDBPropNode propnode;

        printf("[%d] %s:\n", ctxnode->index, sections[section]);
        for (propnode = propmap; propnode != NULL; propnode = propnode->hh.next) {
            printf("  %s:%s\n", propnode->name, propnode->value);
        }
    }
}


void RDBCtxPrintInfo (RDBCtx ctx, int nodeindex)
{
    RDBCtxNode ctxnode;

    int maxnodes = RDBEnvNumNodes(ctx->env);

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

    if (nodeindex >= 0 && nodeindex < maxnodes) {
        ctxnode = RDBCtxGetNode(ctx, nodeindex);
        RDBCtxNodePrintInfo(ctxnode, sections);
    } else {
        nodeindex = 0;
        for (; nodeindex < maxnodes; nodeindex++) {
            ctxnode = RDBCtxGetNode(ctx, nodeindex);
            RDBCtxNodePrintInfo(ctxnode, sections);
        }
    }
}


int RDBCtxNodeInfoProp (RDBCtxNode ctxnode, RDBNodeInfoSection section, const char *propname, char propvalue[RDBAPI_PROP_MAXSIZE])
{
    int len;

    RDBEnvNode envnode = RDBCtxNodeGetEnvNode(ctxnode);

    propvalue[0] = 0;
    propvalue[RDBAPI_PROP_MAXSIZE - 1] = 0;

    if (envnode) {
        RDBPropMap propmap = envnode->nodeinfo[section];
        if (propmap) {
            RDBPropNode propnode = NULL;

            HASH_FIND_STR(propmap, propname, propnode);

            if (propnode) {
                len = snprintf(propvalue, 255, "%s", propnode->value);
                propvalue[255] = 0;
                return len;
            }
        }
    }

    return RDBAPI_ERROR;
}
