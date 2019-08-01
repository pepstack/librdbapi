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

    *outenv = env;
    return RDBAPI_SUCCESS;
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

static char * strtrimchr (char *s, char c)
{
    return (*s==0)?s:(((*s!=c)?(((strtrimchr(s+1,c)-1)==s)?s:(*(strtrimchr(s+1,c)-1)=*s,*s=c,strtrimchr(s+1,c))):strtrimchr(s+1,c)));
}


static int port_str2no (char *port, int portnomin, int portnomax)
{
    int pno = 0;

    char *p = port;

    while (*p) {
        if (! isdigit(*p)) {
            return 0;
        }

        p++;

        if (p - port > 8) {
            return 0;
        }
    }

    if (p == port) {
        return 0;
    }

    pno = atoi(port);

    if (pno < portnomin || pno > portnomax) {
        return 0;
    }

    return pno;
}


static int _RDBEnvNodeParseHosts (RDBEnv_t *env, const char *hosts, size_t hostslen, ub4 ctxtimeout, ub4 sotimeoms)
{
    char *sbuf, *nbuf, *hp, *port, *endp;

    int pno, nodeindex = 0;

    sbuf = (char *) mem_alloc_zero(1, hostslen + 1);

    memcpy(sbuf, hosts, hostslen);

    nbuf = strtrimchr(sbuf, 32);

    hp = strtok(nbuf, ",");
    while (hp) {
        int startpno, endpno;

        port = strchr(hp, ':');
        if (! port) {
            free(sbuf);
            return 0;
        }

        *port++ = 0;

        endp = strchr(port, '-');
        if (endp) {
            *endp++ = 0;

            startpno = port_str2no(port, RDB_PORT_NUMB_MIN, RDB_PORT_NUMB_MAX);
            endpno = port_str2no(endp, RDB_PORT_NUMB_MIN, RDB_PORT_NUMB_MAX);
        } else {
            startpno = port_str2no(port, RDB_PORT_NUMB_MIN, RDB_PORT_NUMB_MAX);
            endpno = startpno;
        }

        if (startpno == 0 || endpno == 0 || startpno > endpno || endpno - startpno > env->clusternodes) {
            free(sbuf);
            return 0;
        }

        for (pno = startpno; pno <= endpno; pno++) {
            if (nodeindex < env->clusternodes) {
                char hostname[RDB_HOSTADDR_MAXLEN + 1];
                char authpass[RDB_AUTHPASS_MAXLEN + 1];

                char *at = strrchr(hp, '@');
                if (at) {
                    snprintf(hostname, sizeof(hostname), "%s", at + 1);
                    if (hp[0] == '\'' || hp[0] == '"') {
                        snprintf(authpass, sizeof(authpass), "%.*s", (int)(at - hp - 2), hp + 1);
                    } else {
                        snprintf(authpass, sizeof(authpass), "%.*s", (int)(at - hp), hp);
                    }
                } else {
                    snprintf(hostname, sizeof(hostname), "%s", hp);
                    authpass[0] = '\0';
                }

                hostname[RDB_HOSTADDR_MAXLEN] = 0;
                authpass[RDB_AUTHPASS_MAXLEN] = 0;

                RDBEnvSetNode(RDBEnvGetNode(env, nodeindex), hostname, pno, ctxtimeout, sotimeoms, authpass);
            } else {
                free(sbuf);
                return (-1);
            }

            nodeindex++;
        }

        if (nodeindex == env->clusternodes) {
            break;
        }
    }

    free(sbuf);
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

    if (_RDBEnvNodeParseHosts(env, ahostport, ahostportlen, ctxtimeout, sotimeoms) == env->clusternodes) {
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