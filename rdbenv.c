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
 * rdbenv.c
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#include "rdbapi.h"
#include "rdbtypes.h"


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

    int nn = cstr_slpit_chr(hosts, cstr_length(hosts, -1), ',', nodenames, RDB_CLUSTER_NODES_MAX);

    int nodeindex = 0;

    while (nodeindex < RDB_CLUSTER_NODES_MAX && nodenames[nodeindex]) {
        int flds = 0;

        // pass@host:port
        char *authpass = NULL;
        char *host = NULL;
        char *port = NULL;

        char *outs[2] = {0};

        char * nodestr = nodenames[nodeindex];

        if (nodestr[0] == nodestr[ strlen(nodestr) - 1 ]) {
            if (nodestr[0] == '\'' || nodestr[0] == '"') {
                nodestr[ strlen(nodestr) - 1 ] = 0;
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
            if (cstr_slpit_chr(nodenames[nodeindex], (int)strlen(nodenames[nodeindex]), ':', hpouts, 2) == 2) {
                if (cstr_slpit_chr(outs[1], (int)strlen(outs[1]), ':', hpouts, 2) == 2) {
                    host = strdup(cstr_LRtrim_chr(cstr_LRtrim_chr(hpouts[0], 32), '\''));
                    port = strdup(cstr_LRtrim_chr(cstr_LRtrim_chr(hpouts[1], 32), '\''));
                }

                free(hpouts[0]);
                free(hpouts[1]);
            }
        } else if (flds == 2) {
            // with authpass
            char *hpouts[2] = {0};

            authpass = strdup(cstr_LRtrim_chr(cstr_LRtrim_chr(outs[0], 32), '\''));

            if (cstr_slpit_chr(outs[1], (int)strlen(outs[1]), ':', hpouts, 2) == 2) {
                host = strdup(cstr_LRtrim_chr(cstr_LRtrim_chr(hpouts[0], 32), '\''));
                port = strdup(cstr_LRtrim_chr(cstr_LRtrim_chr(hpouts[1], 32), '\''));
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

            if (cstr_slpit_chr(port, (int) strlen(port), '-', pnouts, 2) == 2) {
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
    env->valtype_chk_table[RDBVT_SB2] = 1;
    env->valtype_chk_table[RDBVT_UB2] = 1;
    env->valtype_chk_table[RDBVT_SB4] = 1;
    env->valtype_chk_table[RDBVT_UB4] = 1;
    env->valtype_chk_table[RDBVT_UB4X] = 1;
    env->valtype_chk_table[RDBVT_SB8] = 1;
    env->valtype_chk_table[RDBVT_UB8] = 1;
    env->valtype_chk_table[RDBVT_UB8X] = 1;
    env->valtype_chk_table[RDBVT_CHAR] = 1;
    env->valtype_chk_table[RDBVT_BYTE] = 1;
    env->valtype_chk_table[RDBVT_STR] = 1;
    env->valtype_chk_table[RDBVT_FLT64] = 1;
    env->valtype_chk_table[RDBVT_BLOB] = 1;
    env->valtype_chk_table[RDBVT_DEC] = 1;

    i = 0;
    snprintf_V1(env->_valtypenamebuf + i, 6, "SB2");
    env->valtypenames[RDBVT_SB2] = &env->_valtypenamebuf[i];

    i += 8;
    snprintf_V1(env->_valtypenamebuf + i, 6, "UB2");
    env->valtypenames[RDBVT_UB2] = &env->_valtypenamebuf[i];

    i += 8;
    snprintf_V1(env->_valtypenamebuf + i, 6, "UB4");
    env->valtypenames[RDBVT_UB4] = &env->_valtypenamebuf[i];

    i += 8;
    snprintf_V1(env->_valtypenamebuf + i, 6, "UB4X");
    env->valtypenames[RDBVT_UB4X] = &env->_valtypenamebuf[i];

    i += 8;
    snprintf_V1(env->_valtypenamebuf + i, 6, "SB8");
    env->valtypenames[RDBVT_SB8] = &env->_valtypenamebuf[i];

    i += 8;
    snprintf_V1(env->_valtypenamebuf + i, 6, "UB8");
    env->valtypenames[RDBVT_UB8] = &env->_valtypenamebuf[i];

    i += 8;
    snprintf_V1(env->_valtypenamebuf + i, 6, "UB8X");
    env->valtypenames[RDBVT_UB8X] = &env->_valtypenamebuf[i];

    i += 8;
    snprintf_V1(env->_valtypenamebuf + i, 6, "CHAR");
    env->valtypenames[RDBVT_CHAR] = &env->_valtypenamebuf[i];

    i += 8;
    snprintf_V1(env->_valtypenamebuf + i, 6, "BYTE");
    env->valtypenames[RDBVT_BYTE] = &env->_valtypenamebuf[i];

    i += 8;
    snprintf_V1(env->_valtypenamebuf + i, 6, "STR");
    env->valtypenames[RDBVT_STR] = &env->_valtypenamebuf[i];

    i += 8;
    snprintf_V1(env->_valtypenamebuf + i, 6, "FLT64");
    env->valtypenames[RDBVT_FLT64] = &env->_valtypenamebuf[i];

    i += 8;
    snprintf_V1(env->_valtypenamebuf + i, 6, "BLOB");
    env->valtypenames[RDBVT_BLOB] = &env->_valtypenamebuf[i];

    i += 8;
    snprintf_V1(env->_valtypenamebuf + i, 6, "DEC");
    env->valtypenames[RDBVT_DEC] = &env->_valtypenamebuf[i];
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

        cstr_readline(fp, rdbuf, 127);
        line = cstr_trim_chr(rdbuf, 32);

        n = cstr_slpit_chr(line, (int) strnlen(line, 127), ',', cols, 3);

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
            cstr_readline(fp, rdbuf, 127);

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

        *outenv = env;
        return RDBAPI_SUCCESS;
    }
}


void RDBEnvDestroy (RDBEnv env)
{
    int section;
    int nodeindex;

    for (nodeindex = 0; nodeindex < env->clusternodes; nodeindex++) {
        RDBEnvNode envnode = RDBEnvGetNode(env, nodeindex);

        for (section = (int) NODEINFO_SERVER; section != (int) MAX_NODEINFO_SECTIONS; section++) {
            RDBPropMap propmap = envnode->nodeinfo[section];
            envnode->nodeinfo[section] = NULL;

            RDBPropMapFree(propmap);
        }
    }

    HASH_CLEAR(hh, env->nodemap);

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


void RDBEnvSetNode (RDBEnvNode node, const char *host, ub4 port, ub4 ctxtimeout, ub4 sotimeoms, const char *authpass)
{
    node->port = port;

    snprintf_chkd_V1(node->host, sizeof(node->host), "%.*s", (int) strnlen(host, RDB_HOSTADDR_MAXLEN), host);

    snprintf_chkd_V1(node->key, sizeof(node->key), "%s:%d", node->host, (int) node->port);

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

    HASH_ADD_STR(node->env->nodemap, key, node);
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
