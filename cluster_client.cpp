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
        if (cr->Init(itr->first.c_str(), itr->second) < 0) continue;
        clients.insert(pair<string, ClusterRedis *>(string(addr), cr));

        if (!curr_cr_) curr_cr_ = cr;
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

void ClusterClient::ReleaseRetInfoInstance(RetInfo *ri)
{
    if (ri == NULL) return;
    curr_cr_->ReleaseRetInfoInstance(ri);
}

int32_t ClusterClient::String_Set(const char *key,
                                   const char *value,
                                   int32_t expiration)
{
    RetInfo *ret = NULL;
    int32_t res = 0;
    ClusterRedis *cr = NULL;
    if (!curr_cr_) {
        curr_cr_ = clients.begin()->second;
        if (curr_cr_ == NULL) return CLUSTER_ERR;
    }
    ret = curr_cr_->String_Set(key, value, expiration);

    if (ret != NULL && ret->errorno == REDIS_ERROR_MOVE) {
        map<string, ClusterRedis *>::iterator itr;
        if ((itr = clients.find(ret->ip_port)) != clients.end()) {
            cr = itr->second;
            curr_cr_ = cr;
        } else {
            add_new_client(ret->ip_port);
        }

        curr_cr_->ReleaseRetInfoInstance(ret);
        if ((res = String_Set(key, value, expiration)) == CLUSTER_ERR_REQ) {
            // the MOVED master node can't connect
            // XXX:TODO connect to slave node
            return CLUSTER_ERR;
        } else {
            return res;
        }
    }

    // cluster unvalide
    if (ret != NULL && ret->errorno == REDIS_ERROR_DOWN) {
        // just return
        curr_cr_->ReleaseRetInfoInstance(ret);
        return CLUSTER_ERR_DOWN;
    }

    // the current node connection unvalide
    if (ret == NULL || ret->errorno == REDIS_ERROR_REQ) {
        //(ret!=NULL && ret->errorno == REDIS_ERROR_REQ)) {
        char ip_port[32] = {0};
        snprintf(ip_port, IP_ADDR_LEN, "%s:%d",
                 curr_cr_->get_ip(), curr_cr_->get_port());
        clients.erase(string(ip_port));
        curr_cr_->UnInit();
        curr_cr_ = NULL;
        res = String_Set(key, value, expiration);
    } else {
        curr_cr_->ReleaseRetInfoInstance(ret);
        return ret->errorno;
    }

    curr_cr_->ReleaseRetInfoInstance(ret);
    return res;
}

RetInfo *ClusterClient::String_Get(const char *key, string &value)
{
    RetInfo *ret = NULL;
    ClusterRedis *cr = NULL;
    if (!curr_cr_) {
        curr_cr_ = clients.begin()->second;
        if (curr_cr_ == NULL) return NULL;
    }
    ret = curr_cr_->String_Get(key, value);

    if (ret != NULL && ret->errorno == REDIS_ERROR_MOVE) {
        map<string, ClusterRedis *>::iterator itr;
        if ((itr = clients.find(ret->ip_port)) != clients.end()) {
            cr = itr->second;
            curr_cr_ = cr;
        } else {
            add_new_client(ret->ip_port);
        }

        if ((ret = String_Get(key, value)) == NULL) {
            // the MOVED master node can't connect
            // XXX:TODO connect to slave node
            return ret;
        }
    }

    if (ret != NULL && ret->errorno == REDIS_ERROR_DOWN) {
        // just return
        return ret;
    }

    if (ret == NULL) {
        char ip_port[32] = {0};
        snprintf(ip_port, IP_ADDR_LEN, "%s:%d",
                 curr_cr_->get_ip(), curr_cr_->get_port());
        clients.erase(string(ip_port));
        curr_cr_->UnInit();
        curr_cr_ = NULL;
        ret = String_Get(key, value);
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
        curr_cr_ = cr;
        if (ret.second == false) {
            logg("ERROR", "insert <%s, %p> failed, already existed!",
                 ret.first->first.c_str(), ret.first->second);
            return CLUSTER_ERR;
        }
    }

    return CLUSTER_OK;
}
