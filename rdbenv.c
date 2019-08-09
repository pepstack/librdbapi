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


RDBAPI_RESULT RDBEnvCreate (ub4 flags, ub2 clusternodes, RDBEnv *outenv)
{
    if (clusternodes < 1 || clusternodes > RDB_CLUSTER_NODES_MAX) {
        // too many nodes for redis cluster
        return RDBAPI_ERR_NODES;
    }

    RDBEnv env = (RDBEnv_t *) RDBMemAlloc(sizeof(RDBEnv_t) + (int) clusternodes * sizeof(RDBEnvNode_t));

    env->flags = flags;
    env->clusternodes = clusternodes;
    env->nodemap = NULL;

    do {
        int i = 0;
        for (; i < (int) clusternodes; i++) {
            env->nodes[i].env = env;            
            env->nodes[i].index = i;
        }
    } while (0);

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

    *outenv = env;
    return RDBAPI_SUCCESS;
}


RDBAPI_RESULT RDBEnvCreateInit (const char *appcfg, RDBEnv *outenv)
{
    int numnodes = 0;

    int ctxtimeout = 0;
    int sotimeoms = 12000;

    char *nodenames[RDB_CLUSTER_NODES_MAX] = {0};

    FILE *fp;

    if ((fp = fopen(appcfg, "r")) != NULL) {
        int nn = 0;
        int nnlen = 0;

        numnodes = 0;

        while (!feof(fp) && nn < RDB_CLUSTER_NODES_MAX) {
            char rdbuf[256] = {0};

            fgets(rdbuf, 255, fp);

            if (! numnodes) {
                char *outs[20] = {0};

                int n = cstr_slpit_chr(rdbuf, (int) strnlen(rdbuf, 255), ',', outs, 3);
                if (n >= 1) {
                    numnodes = atoi(outs[0]);
                    free(outs[0]);
                }
                if (n >= 2) {
                    ctxtimeout = atoi(outs[1]);
                    free(outs[1]);
                }
                if (n == 3) {
                    sotimeoms = atoi(outs[2]);
                    free(outs[2]);
                }
            } else {
                nodenames[nn] = strdup(cstr_LRtrim_chr(cstr_LRtrim_chr(cstr_LRtrim_chr(rdbuf, '\r'), '\n'), 32));
                nnlen += (int) strlen(nodenames[nn]);
                nn++;
            }
        }

        fclose(fp);

        if (! nn || ! numnodes || nnlen + numnodes > 4095) {
            while(nn-- > 0) {
                free(nodenames[nn]);
            }
            return RDBAPI_ERROR;
        }

        do {
            int n = 1;
            char cluster[4096] = {0};

            strcat(cluster, nodenames[0]);

            while (n++ < nn) {
                strcat(cluster, ",");
                strcat(cluster, nodenames[nn]);
            }

            cluster[nnlen + numnodes] = 0;
        
            RDBEnv env;
            if (RDBEnvCreate(0, numnodes, &env) == RDBAPI_SUCCESS) {
                if (RDBEnvInitAllNodes(env, cluster, -1, ctxtimeout, sotimeoms) == RDBAPI_SUCCESS) {
                    *outenv = env;

                    while(nn-- > 0) {
                        free(nodenames[nn]);
                    }
                    return RDBAPI_SUCCESS;
                }

                RDBEnvDestroy(env);
            }
        } while(0);

        while(nn-- > 0) {
            free(nodenames[nn]);
        }
    }

    return RDBAPI_ERROR;
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

    snprintf(node->host, sizeof(node->host), "%.*s", (int) strnlen(host, RDB_HOSTADDR_MAXLEN), host);
    node->host[RDB_HOSTADDR_MAXLEN] = 0;

    snprintf(node->key, sizeof(node->key), "%s:%d", node->host, (int) node->port);
    node->key[sizeof(node->key) - 1] = 0;

    if (authpass) {
        snprintf(node->authpass, sizeof(node->authpass), "%.*s", (int) strnlen(authpass, RDB_AUTHPASS_MAXLEN), authpass);
        node->authpass[RDB_AUTHPASS_MAXLEN] = 0;
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

        snprintf(key, sizeof(key), "%s:%u", host, port);
        key[sizeof(key) - 1] = '\0';

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


/**
 * ahostport:
 *   - 127.0.0.1:7001-7009
 *   - 127.0.0.1:7001,127.0.0.1:7002-7005,...
 *   - 192.168.22.11:7001-7003,192.168.22.12:7001-7003,...
 *   - authpass@host:port,...
 */
static int RDBEnvNodeParseHosts (RDBEnv_t *env, const char *hosts, size_t hostslen, ub4 ctxtimeout, ub4 sotimeoms)
{
    char *nodenames[RDB_CLUSTER_NODES_MAX] = {0};
    int nn = cstr_slpit_chr(hosts, (int) hostslen, ',', nodenames, RDB_CLUSTER_NODES_MAX);

    int nodeindex = 0;

    while (nodeindex < RDB_CLUSTER_NODES_MAX && nodenames[nodeindex]) {
        // pass@host:port
        char *authpass = NULL;
        char *host = NULL;
        char *port = NULL;

        char *outs[2] = {0};

        int flds = cstr_slpit_chr(nodenames[nodeindex], (int)strlen(nodenames[nodeindex]), '@', outs, 2);

        if (flds == 1) {
            // no pass
            char *hpouts[2] = {0};

            if (cstr_slpit_chr(nodenames[nodeindex], (int)strlen(nodenames[nodeindex]), ':', hpouts, 2) == 2) {
                if (cstr_slpit_chr(outs[1], (int)strlen(outs[1]), ':', hpouts, 2) == 2) {
                    host = strdup(cstr_LRtrim_chr(cstr_LRtrim_chr(hpouts[0], 32), '\''));
                    port = strdup(cstr_LRtrim_chr(cstr_LRtrim_chr(hpouts[1], 32), '\''));
                }

                free(hpouts[0]);
                free(hpouts[1]);
            }
        } else if (flds == 2) {
            char *hpouts[2] = {0};

            authpass = strdup(cstr_LRtrim_chr(cstr_LRtrim_chr(outs[0], 32), '\''));

            if (cstr_slpit_chr(outs[1], (int)strlen(outs[1]), ':', hpouts, 2) == 2) {
                host = strdup(cstr_LRtrim_chr(cstr_LRtrim_chr(hpouts[0], 32), '\''));
                port = strdup(cstr_LRtrim_chr(cstr_LRtrim_chr(hpouts[1], 32), '\''));
            }

            free(hpouts[0]);
            free(hpouts[1]);
        }

        free(outs[0]);
        free(outs[1]);

        if (! host || !port) {
            free(host);
            free(port);
            free(authpass);

            break;
        }

        do {
            char *pnouts[2] = {0};

            if (cstr_slpit_chr(port, (int) strlen(port), '-', pnouts, 2) == 2) {
                int pno = atoi(pnouts[0]);
                int endpno = atoi(pnouts[1]);

                for (; pno <= endpno; pno++) {
                    RDBEnvSetNode(RDBEnvGetNode(env, nodeindex), host, (ub4) pno, ctxtimeout, sotimeoms, authpass);

                    nodeindex++;
                }
            } else {
                RDBEnvSetNode(RDBEnvGetNode(env, nodeindex), host, (ub4) atol(port), ctxtimeout, sotimeoms, authpass);

                nodeindex++;
            }

            free(pnouts[0]);
            free(pnouts[1]);
        } while(0);

        free(host);
        free(port);
        free(authpass);
    }

    while (nn-- > 0) {
        free(nodenames[nn]);
    }

    return nodeindex;
}


RDBAPI_RESULT RDBEnvInitAllNodes (RDBEnv env, const char *ahostport, size_t ahostportlen, ub4 ctxtimeout, ub4 sotimeoms)
{
    size_t ahplenmax = env->clusternodes * (RDB_HOSTADDR_MAXLEN + RDB_AUTHPASS_MAXLEN + 12);

    if (ahostportlen == (size_t) -1) {
        ahostportlen = strnlen(ahostport, ahplenmax);
    }

    if (ahostportlen > ahplenmax) {
        return RDBAPI_ERR_BADARG;
    }

    if (RDBEnvNodeParseHosts(env, ahostport, ahostportlen, ctxtimeout, sotimeoms) == env->clusternodes) {
        return RDBAPI_SUCCESS;
    } else {
        return RDBAPI_ERROR;
    }
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