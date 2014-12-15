/*
 * Copyright (c) 2008-2014, Chen Huaying <chying.ren@gmail.com>
 * All Rights Reserved.
 *
 * redis cluster client, wrapper of Cluster_Redis
 */


#include <iostream>
#include <vector>
#include <cstring>
#include <cstdlib>
#include "cluster_client.h"
#include "log.h"

#define REDIS_IP_SPLIT_CHAR  ";"
#define IP_ADDR_LEN          32

using std::cout;
using std::endl;

ClusterRedis *g_curr_cr;

int32_t ClusterClient::Init(const char *redis_master_ips)
{
    ip_list_unserailize(redis_master_ips);
    if (cluster_masters.size() == 0) {
        logg("ERROR", "Init masters %s failed!", redis_master_ips);
        return -1;
    }

    std::vector<pair<string ,int> >::iterator itr = cluster_masters.begin();
    for (; itr != cluster_masters.end(); ++itr) {
        logg("DEBUG", "master node[%s:%d]", itr->first.c_str(), itr->second);
        char addr[IP_ADDR_LEN] = {0};
        snprintf(addr, IP_ADDR_LEN, "%s:%d", itr->first.c_str(), itr->second);

        ClusterRedis *cr = new ClusterRedis;
        cr->Init(itr->first.c_str(), itr->second);
        clients.insert(pair<string, ClusterRedis *>(string(addr), cr));

        if (!g_curr_cr) g_curr_cr = cr;
    }

    return 0;
}

int32_t ClusterClient::Uninit()
{
    map<string, ClusterRedis *>::iterator itr = clients.begin();
    for (; itr != clients.end(); ++itr) {
        logg("DEBUG", "%s, %p", itr->first.c_str(), itr->second);
        itr->second->UnInit();
    }
    return 0;
}

void ClusterClient::show_clients()
{
    map<string, ClusterRedis *>::iterator itr = clients.begin();
    for (; itr != clients.end(); ++itr) {
        logg("DEBUG", "%s, %p", itr->first.c_str(), itr->second);
    }
}

void ClusterClient::ip_list_unserailize(const char *ip_list)
{
    if (NULL == ip_list) return;
    char *buf = new char[1024 * 1024];
    strcpy(buf, ip_list);
    char *tokenPtr = std::strtok(buf, REDIS_IP_SPLIT_CHAR);
    char *tmpPtr = NULL;
    char ip_buff[32 + 1];
    int32_t port = 0;
    pair<string, int> tmpPair; //(string(ip_buff),port);
    while (NULL != tokenPtr)
    {
		printf("ip list[%s]\n", tokenPtr);
		memset(ip_buff, 0, sizeof(ip_buff));
		port = 0;
		tmpPtr = std::strchr(tokenPtr, ':');
		strncpy(ip_buff, tokenPtr, tmpPtr - tokenPtr);
		port = std::atoi(tmpPtr + 1);
		tmpPair.first = string(ip_buff);
		tmpPair.second = port;
		cluster_masters.push_back(tmpPair);
		tokenPtr = strtok(NULL, REDIS_IP_SPLIT_CHAR);
    }

    delete[] buf;
}

RetInfo *ClusterClient::String_Set(const char *key,
        const char *value,
        int32_t expiration)
{
    cout << "String_set " << key << endl;
    RetInfo *ret = NULL;
    ClusterRedis *cr = NULL;
    ret = g_curr_cr->String_Set(key, value, expiration);

    if (ret->errorno == REDIS_ERROR_MOVE) {
        map<string, ClusterRedis *>::iterator itr;
        if ((itr = clients.find(ret->ip_port)) != clients.end()) {
            cout << "find " << ret->ip_port << " client" << endl;
            cr = itr->second;
            g_curr_cr = cr;
        } else {
            cout << "add new client" << endl;
            add_new_client(ret->ip_port);
#if 0
            char ip_buf[IP_ADDR_LEN] = {0};
            int32_t port = 0;
            char *ptr = NULL;

            // add new client
            if ((ptr = std::strchr(ret->ip_port, ':')) != NULL) {
                strncpy(ip_buf, ret->ip_port, ptr - ret->ip_port);
                port = std::atoi(ptr + 1);

                ClusterRedis *cr = new ClusterRedis;
                cr->Init(ip_buf, port);
                clients.insert(pair<string,
                               ClusterRedis *>(string(ret->ip_port), cr));
            }
#endif
        }
        String_Set(key, value, expiration);
    }

    return ret;
}

RetInfo *ClusterClient::String_Get(const char *key, string &value)
{
    cout << "String get" << endl;
    RetInfo *ret = NULL;
    ClusterRedis *cr = NULL;
    if (!g_curr_cr) {
        g_curr_cr = clients.begin()->second;
        if (g_curr_cr == NULL) return NULL;
    }
    ret = g_curr_cr->String_Get(key, value);

    if (ret != NULL && ret->errorno == REDIS_ERROR_MOVE) {
        map<string, ClusterRedis *>::iterator itr;
        if ((itr = clients.find(ret->ip_port)) != clients.end()) {
            cr = itr->second;
            g_curr_cr = cr;
        } else {
            add_new_client(ret->ip_port);
#if 0
            char ip_buf[IP_ADDR_LEN] = {0};
            int32_t port = 0;
            char *ptr = NULL;

            // add new client
            if ((ptr = std::strchr(ret->ip_port, ':')) != NULL) {
                strncpy(ip_buf, ret->ip_port, ptr - ret->ip_port);
                port = std::atoi(ptr + 1);

                ClusterRedis *cr = new ClusterRedis;
                cr->Init(ip_buf, port);
                clients.insert(pair<string,
                               ClusterRedis *>(string(ret->ip_port), cr));
            }
#endif
        }

        if ((ret = String_Get(key, value)) != NULL) {
            // the MOVED master node is down
            if (ret->errorno == REDIS_ERROR_DOWN) {
                // XXX:TODO connect to slave node
            }
        }
    }

    if (ret != NULL && ret->errorno == REDIS_ERROR_DOWN) {
        clients.erase(string(ret->ip_port));
        g_curr_cr->UnInit();
        g_curr_cr = NULL;
        String_Get(key, value);
    }

    return ret;
}

int32_t ClusterClient::add_new_client(const char *ip_addr)
{
    char ip_buf[IP_ADDR_LEN] = {0};
    int32_t port = 0;
    const char *ptr = NULL;

    // add new client
    if ((ptr = std::strchr(ip_addr, ':')) != NULL) {
        strncpy(ip_buf, ip_addr, ptr - ip_addr);
        port = std::atoi(ptr + 1);

        ClusterRedis *cr = new ClusterRedis;
        cr->Init(ip_buf, port);
        pair<map<string, ClusterRedis *>::iterator, bool> ret;
        ret = clients.insert(pair<string,
                       ClusterRedis *>(string(ip_addr), cr));
        if (ret.second == false) {
            logg("ERROR", "insert <%s, %p> failed, already existed!",
                 ret.first->first.c_str(), ret.first->second);
            return CLUSTER_ERR;
        }
    }

    return CLUSTER_OK;
}
