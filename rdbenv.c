﻿/***********************************************************************
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
 * rdbenv.c
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#include "rdbfuncs.h"


/**
 * ahostport:
 *   - 127.0.0.1:7001-7009
 *   - 127.0.0.1:7001,127.0.0.1:7002-7005,...
 *   - 192.168.22.11:7001-7003,192.168.22.12:7001-7003,...
 *   - authpass@host:port,...
 * 
 * returns:
 *   number of nodes
 */
typedef struct _RDBNodeCfg_t
{
    ub4 port;

    ub4 ctxtimeout;
    ub4 sotimeo_ms;

    char host[RDB_HOSTADDR_MAXLEN + 1];
    char authpass[RDB_AUTHPASS_MAXLEN + 1];
} RDBNodeCfg_t, *RDBNodeCfg;


static int RDBParseClusterNodes (const char *hosts, ub4 ctxtimeout, ub4 sotimeo_ms, RDBNodeCfg nodecfgs[RDB_CLUSTER_NODES_MAX])
{
    char *nodenames[RDB_CLUSTER_NODES_MAX] = {0};
    int nodenameslen[RDB_CLUSTER_NODES_MAX] = {0};

    int nn = cstr_slpit_chr(hosts, cstr_length(hosts, -1), ',', nodenames, nodenameslen, RDB_CLUSTER_NODES_MAX);

    int nodeindex = 0;

    while (nodeindex < RDB_CLUSTER_NODES_MAX && nodenames[nodeindex]) {
        int flds = 0;

        // pass@host:port
        char *authpass = NULL;
        char *host = NULL;
        char *port = NULL;

        char *outs[2] = {0};

        char * nodestr = nodenames[nodeindex];
        int nodelen =  nodenameslen[nodeindex];

        if (nodestr[0] == nodestr[ nodelen - 1 ]) {
            if (nodestr[0] == '\'' || nodestr[0] == '"') {
                nodestr[ nodelen - 1 ] = 0;
                nodestr++;
            }
        }

        // with authpass = test@123
        //   1) 'test@123'@127.0.0.1:7001
        //   2) "test@123"@127.0.0.1:7001
        //   3) test@123@127.0.0.1:7001
        // without authpass
        //   127.0.0.1:7001

        if (nodestr[0] == '"') {
            char * p = strstr(nodestr, "\"@");
            if (!p) {
                printf("(%s:%d) node(%d) error: %s\n", __FILE__, __LINE__, nodeindex, nodestr);
                nodeindex = -1;
                goto exit_return;
            }

            *p++ = 0;
            *p++ = 0;

            outs[0] = strdup(&nodestr[1]);
            outs[1] = strdup(p);

            flds = 2;
        } else if (nodestr[0] == '\'') {
            char * p = strstr(nodestr, "'@");
            if (!p) {
                printf("(%s:%d) node(%d) error: %s\n", __FILE__, __LINE__, nodeindex, nodestr);
                nodeindex = -1;
                goto exit_return;
            }

            *p++ = 0;
            *p++ = 0;

            outs[0] = strdup(&nodestr[1]);
            outs[1] = strdup(p);

            flds = 2;
        } else {
            char * p = strrchr(nodestr, '@');
            if (p) {
                *p++ = 0;

                outs[0] = strdup(nodestr);
                outs[1] = strdup(p);

                flds = 2;
            } else {
                // no authpass
                outs[0] = strdup(nodestr);
                flds = 1;
            }
        }

        if (flds == 1) {
            // without authpass
            char *hpouts[2] = {0};

            // get ip:port
            if (cstr_slpit_chr(nodenames[nodeindex], nodelen, ':', hpouts, NULL, 2) == 2) {
                if (cstr_slpit_chr(outs[1], cstr_length(outs[1], -1), ':', hpouts, NULL, 2) == 2) {
                    host = strdup( cstr_LRtrim_chr( cstr_LRtrim_chr(hpouts[0], 32, NULL), '\'', NULL) );
                    port = strdup( cstr_LRtrim_chr( cstr_LRtrim_chr(hpouts[1], 32, NULL), '\'', NULL) );
                }

                free(hpouts[0]);
                free(hpouts[1]);
            }
        } else if (flds == 2) {
            // with authpass
            char *hpouts[2] = {0};

            authpass = strdup( cstr_LRtrim_chr(cstr_LRtrim_chr(outs[0], 32, NULL), '\'', NULL));

            if (cstr_slpit_chr(outs[1], cstr_length(outs[1], -1), ':', hpouts, NULL, 2) == 2) {
                host = strdup( cstr_LRtrim_chr(cstr_LRtrim_chr(hpouts[0], 32, NULL), '\'', NULL) );
                port = strdup( cstr_LRtrim_chr(cstr_LRtrim_chr(hpouts[1], 32, NULL), '\'', NULL) );
            }

            free(hpouts[0]);
            free(hpouts[1]);
        }

        while (flds-- > 0) {
            free(outs[flds]);
        }

        if (! host || ! port) {
            free(host);
            free(port);
            free(authpass);

            printf("(%s:%d) node(%d) error: %s\n", __FILE__, __LINE__, nodeindex, nodestr);
            nodeindex = -1;
            goto exit_return;
        }

        do {
            char *pnouts[2] = {0};

            if (cstr_slpit_chr(port, (int) strlen(port), '-', pnouts, NULL, 2) == 2) {
                int pno = atoi(pnouts[0]);
                int endpno = atoi(pnouts[1]);

                for (; pno <= endpno; pno++) {
                    RDBNodeCfg nodecfg = (RDBNodeCfg) RDBMemAlloc(sizeof(RDBNodeCfg_t));

                    snprintf_chkd_V1(nodecfg->host, sizeof(nodecfg->host), "%s", host);
                    snprintf_chkd_V1(nodecfg->authpass, sizeof(nodecfg->authpass), "%s", authpass);

                    nodecfg->port = (ub4) pno;
                    nodecfg->ctxtimeout = ctxtimeout;
                    nodecfg->sotimeo_ms = sotimeo_ms;

                    nodecfgs[nodeindex++] = nodecfg;
                }
            } else {
                RDBNodeCfg nodecfg = (RDBNodeCfg) RDBMemAlloc(sizeof(RDBNodeCfg_t));

                snprintf_chkd_V1(nodecfg->host, sizeof(nodecfg->host), "%s", host);
                snprintf_chkd_V1(nodecfg->authpass, sizeof(nodecfg->authpass), "%s", authpass);

                nodecfg->port = (ub4) atol(port);
                nodecfg->ctxtimeout = ctxtimeout;
                nodecfg->sotimeo_ms = sotimeo_ms;
                
                nodecfgs[nodeindex++] = nodecfg;
            }

            free(pnouts[0]);
            free(pnouts[1]);
        } while(0);

        free(host);
        free(port);
        free(authpass);
    }

exit_return:
    while (nn-- > 0) {
        free(nodenames[nn]);
    }

    return nodeindex;
}


static void RDBEnvSetNode (RDBEnvNode node, const char *host, ub4 port, ub4 ctxtimeout, ub4 sotimeoms, const char *authpass)
{
    int keylen;

    node->port = port;

    snprintf_chkd_V1(node->host, sizeof(node->host), "%.*s", (int) strnlen(host, RDB_HOSTADDR_MAXLEN), host);

    keylen = snprintf_chkd_V1(node->key, sizeof(node->key), "%s:%d", node->host, (int) node->port);

    if (authpass) {
        snprintf_chkd_V1(node->authpass, sizeof(node->authpass), "%.*s", (int) strnlen(authpass, RDB_AUTHPASS_MAXLEN), authpass);
    } else {
        *node->authpass = 0;
    }

    if (ctxtimeout) {
        node->ctxtimeo.tv_sec = ctxtimeout;
        node->ctxtimeo.tv_usec = 0;
    } else {
        node->ctxtimeo.tv_sec = 0;
        node->ctxtimeo.tv_usec = 0;
    }

    if (sotimeoms) {
        node->sotimeo.tv_sec = sotimeoms / 1000;
        node->sotimeo.tv_usec = (sotimeoms % 1000) * 1000;
    } else {
        memset(&node->sotimeo, 0, sizeof(node->sotimeo));
    }

    HASH_ADD_STR_LEN(node->env->nodemap, key, keylen, node);
}


static void RDBEnvInitInternal (RDBEnv env, RDBNodeCfg nodecfgs[])
{
    int i;

    for (i = 0; i < (int) env->clusternodes; i++) {
        env->nodes[i].env = env;            
        env->nodes[i].index = i;
        
        RDBEnvSetNode(&(env->nodes[i]), nodecfgs[i]->host, nodecfgs[i]->port, nodecfgs[i]->ctxtimeout, nodecfgs[i]->sotimeo_ms, nodecfgs[i]->authpass);
    }

    // initialize global readonly config
    //
    env->valtypetable[RDBVT_SB2]   = RDBZStringNew("SB2", 3);
    env->valtypetable[RDBVT_UB2]   = RDBZStringNew("UB2", 3);
    env->valtypetable[RDBVT_SB4]   = RDBZStringNew("SB4", 3);
    env->valtypetable[RDBVT_UB4]   = RDBZStringNew("UB4", 3);
    env->valtypetable[RDBVT_UB4X]  = RDBZStringNew("UB4X", 4);
    env->valtypetable[RDBVT_SB8]   = RDBZStringNew("SB8", 3);
    env->valtypetable[RDBVT_UB8]   = RDBZStringNew("UB8", 3);
    env->valtypetable[RDBVT_UB8X]  = RDBZStringNew("UB8X", 4);
    env->valtypetable[RDBVT_CHAR]  = RDBZStringNew("CHAR", 4);
    env->valtypetable[RDBVT_BYTE]  = RDBZStringNew("BYTE", 4);
    env->valtypetable[RDBVT_STR]   = RDBZStringNew("STR", 3);
    env->valtypetable[RDBVT_FLT64] = RDBZStringNew("FLT64", 5);
    env->valtypetable[RDBVT_BLOB]  = RDBZStringNew("BLOB", 4);
    env->valtypetable[RDBVT_DEC]   = RDBZStringNew("DEC", 3);

    env->valtypetable[RDBVT_DATE]   = RDBZStringNew("DATE", 4);
    env->valtypetable[RDBVT_TIME]  = RDBZStringNew("TIME", 4);
    env->valtypetable[RDBVT_STAMP]  = RDBZStringNew("STAMP", 5);

    env->valtypetable[RDBVT_SET]   = RDBZStringNew("SET", 3);

    // init rdb functions
    //
}


static size_t RDBEnvLoadCfgfile (const char *cfgfile, char **outCluster, int *ioCtxtimeout, int *ioSotimeo_ms)
{
    int numnodes = 0;

    int ctxtimeout = *ioCtxtimeout;
    int sotimeo_ms = *ioSotimeo_ms;

    RDBEnv env = NULL;

    FILE *fp;

    if ((fp = fopen(cfgfile, "r")) != NULL) {
        int n, len;

        char *line;
        char rdbuf[128];
        char *cols[10] = {0};

        int maxsz = 256;

        size_t nnlen = 0;

        char *cluster = RDBMemAlloc(maxsz);

        numnodes = 0;

        cstr_readline(fp, rdbuf, 127, 1);

        n = cstr_slpit_chr(rdbuf, cstr_length(rdbuf, 127), ',', cols, NULL, 3);

        if (n != 3) {
            printf("RDBAPI_ERROR: (%s:%d) fail on read config: %s\n", __FILE__, __LINE__, cfgfile);
            return RDBAPI_ERROR;
        }

        numnodes = atoi(cols[0]);

        ctxtimeout = atoi(cols[1]);
        sotimeo_ms = atoi(cols[2]);

        free(cols[0]);
        free(cols[1]);
        free(cols[2]);

        for (n = 0; n < numnodes; n++) {
            cstr_readline(fp, rdbuf, 127, 1);

            // test@192.168.39.111:7009
            line = cstr_trim_chr(rdbuf, 32);

            len = cstr_length(line, 255);
            if (len < 16 || len > 120) {
                printf("RDBAPI_ERROR: (%s:%d) bad node(%d) config: %s\n", __FILE__, __LINE__, n+1, line);
                return RDBAPI_ERROR;
            }

            if ( nnlen + len + 4 > maxsz ) {
                maxsz += 256;

                cluster = realloc(cluster, maxsz);
                if (! cluster) {
                    printf("RDBAPI_ERROR: (%s:%d) no memory to realloc size(%d).\n", __FILE__, __LINE__, maxsz);
                    exit(EXIT_FAILURE);
                }
            }
            
            if (! nnlen) {
                len = snprintf_chkd_V1(cluster, maxsz, "%.*s", len, line);
            } else {
                len = snprintf_chkd_V1(cluster + nnlen, maxsz, ",%.*s", len, line);
            }
            nnlen += len;
        }

        cluster[nnlen] = 0;

        fclose(fp);

        *outCluster = cluster;
        *ioCtxtimeout = ctxtimeout;
        *ioSotimeo_ms = sotimeo_ms;

        // success
        return nnlen;
    }

    printf("RDBAPI_ERROR: (%s:%d) fopen failed: %s\n", __FILE__, __LINE__, cfgfile);
    return 0;
}


RDBAPI_RESULT RDBEnvCreate (const char *cluster, int ctxtimeout, int sotimeo_ms, RDBEnv *outenv)
{
    if (sotimeo_ms == 0) {
        sotimeo_ms = RDB_CLUSTER_SOTIMEO_MS;
    }

    if (cstr_startwith(cluster, cstr_length(cluster, -1), "file://", 7)) {
        int ret;
        char *clustnodes = NULL;

        size_t nodeslen = RDBEnvLoadCfgfile(cluster + 7, &clustnodes, &ctxtimeout, &sotimeo_ms);
        if (! nodeslen) {
            return RDBAPI_ERROR;
        }

        ret = RDBEnvCreate(clustnodes, ctxtimeout, sotimeo_ms, outenv);

        free(clustnodes);

        return ret;
    } else {
        RDBEnv env;

        int numnodes;
        RDBNodeCfg nodecfgs[RDB_CLUSTER_NODES_MAX] = {0};

        numnodes = RDBParseClusterNodes(cluster, ctxtimeout, sotimeo_ms, nodecfgs);

        if (numnodes < 1) {
            printf("(%s:%d) invalid clusternodes: %s", __FILE__, __LINE__, cluster);
            return RDBAPI_ERR_NODES;
        }

        env = (RDBEnv_t *) RDBMemAlloc(sizeof(RDBEnv_t) + (int) numnodes * sizeof(RDBEnvNode_t));

        env->nodemap = NULL;
        env->clusternodes = numnodes;

        RDBEnvInitInternal(env, nodecfgs);

        while(numnodes-- > 0) {
            RDBMemFree(nodecfgs[numnodes]);
        }

        threadlock_init(&env->thrlock);

        // set env readonly attributes
        env->verbose = (ub1)1;
        env->delimiter = RDB_TABLE_DELIMITER_CHAR;

        snprintf_chkd_V1(env->_exprstr, sizeof(env->_exprstr), "%s", "=,LLIKE,RLIKE,LIKE,MATCH,!=,>,<,>=,<=");
        do {
            char *saveptr;
            int cnt = 0;
            char *ps = strtok_r(env->_exprstr, ",", &saveptr);
            while (ps && cnt++ < filterexprs_count_max) {
                env->filterexprs[cnt] = ps;
                ps = strtok_r(NULL, ",", &saveptr);
            }
        } while(0);

        *outenv = env;
        return RDBAPI_SUCCESS;
    }
}


void RDBEnvDestroy (RDBEnv env)
{
    int i, s;

    threadlock_lock(&env->thrlock);

    for (i = 0; i < env->clusternodes; i++) {
        RDBEnvNode envnode = RDBEnvGetNode(env, i);

        for (s = (int) NODEINFO_SERVER; s != (int) MAX_NODEINFO_SECTIONS; s++) {
            RDBPropMap propmap = envnode->nodeinfo[s];
            envnode->nodeinfo[s] = NULL;

            RDBPropMapFree(propmap);
        }
    }

    HASH_CLEAR(hh, env->nodemap);

    for (i = 0; i < sizeof(env->valtypetable)/sizeof(env->valtypetable[0]); i++) {
        RDBZStringFree(env->valtypetable[i]);
    }

    for (i = 0; i < sizeof(env->rdbfuncs)/sizeof(env->rdbfuncs[0]); i++) {
        if (env->rdbfuncs[i].name) {
            free((void*) env->rdbfuncs[i].name);
        }
    }

    threadlock_destroy(&env->thrlock);

    RDBMemFree((void*) env);
}


int RDBEnvNumNodes (RDBEnv env)
{
    return (int) env->clusternodes;
}


int RDBEnvNumMasterNodes (RDBEnv env)
{
    int masters = 0;
    int nodeindex = 0;

    for (; nodeindex < env->clusternodes; nodeindex++) {
        RDBEnvNode envnode = RDBEnvGetNode(env, nodeindex);

        if (envnode->replica.is_master) {
            masters++;
        }
    }

    return masters;
}


RDBEnvNode RDBEnvGetNode (RDBEnv env, int nodeindex)
{
    if (nodeindex < 0 || nodeindex >= env->clusternodes) {
        return NULL;
    } else {
        return &env->nodes[nodeindex];
    }
}


RDBEnvNode RDBEnvFindNode (RDBEnv env, const char *host, ub4 port)
{
    RDBEnvNode node = NULL;

    if (! port || port == (ub4) -1) {
        HASH_FIND_STR(env->nodemap, host, node);
    } else {
        char key[RDB_HOSTADDR_MAXLEN + 12];

        snprintf_chkd_V1(key, sizeof(key), "%s:%u", host, port);

        HASH_FIND_STR(env->nodemap, key, node);
    }

    return node;
}


int RDBEnvNodeIndex (RDBEnvNode envnode)
{
    return envnode->index;
}


const char * RDBEnvNodeHost (RDBEnvNode envnode)
{
    return envnode->host;
}


ub4 RDBEnvNodePort (RDBEnvNode envnode)
{
    return envnode->port;
}


ub4 RDBEnvNodeTimeout (RDBEnvNode envnode)
{
    return (ub4) envnode->ctxtimeo.tv_sec;
}


ub4 RDBEnvNodeSotimeo (RDBEnvNode envnode)
{
    return (ub4) (envnode->sotimeo.tv_sec * 1000 + envnode->sotimeo.tv_usec / 1000);
}


const char * RDBEnvNodeHostPort (RDBEnvNode envnode)
{
    return envnode->key;
}


RDBAPI_BOOL RDBEnvNodeGetMaster (RDBEnvNode envnode, int *masterindex)
{
    if (envnode->replica.is_master) {
        if (masterindex) {
            *masterindex = envnode->index;
        }
        return RDBAPI_TRUE;
    } else {
        if (masterindex) {
            *masterindex = envnode->replica.master_nodeindex;
        }
        return RDBAPI_FALSE;
    }
}


int RDBEnvNodeGetSlaves (RDBEnvNode envnode, int slaveindex[RDBAPI_SLAVES_MAXNUM])
{
    if (envnode->replica.is_master) {
        if (slaveindex) {
            int i = 0;
            for (i = 0; i < envnode->replica.connected_slaves; i++) {
                slaveindex[i] = envnode->replica.slaves[i];
            }
        }
        return envnode->replica.connected_slaves;
    } else {
        // no slaves for slave node
        return 0;
    }    
}
