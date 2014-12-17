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
#include "crc16.h"
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
        if (cr->Init(itr->first.c_str(), itr->second) < 0) { 
            cr->UnInit();
            delete cr;
            continue;
        }
        pair<map<string, ClusterRedis *>::iterator, bool> ret;
        ret = clients.insert(pair<string, ClusterRedis *>(string(addr), cr));
        if (ret.second == false) {
            logg("ERROR", "insert <%s, %p> failed, already existed!",
                 ret.first->first.c_str(), ret.first->second);
            cr->UnInit();
            delete cr;
            continue;
        }

        if (!curr_cr_) curr_cr_ = cr;
    }

    if (curr_cr_ == NULL) {
        logg("ERROR", "Init master nodes failed! Can't connect to none");
        return -1;
    }

    redisReply *reply = curr_cr_->redis_command("cluster slots");
    if (reply != NULL && reply->type != REDIS_REPLY_ERROR) {
        uint32_t i = 0;
        uint32_t j = 0;
        uint32_t k = 0;
        for (i = 0; i < reply->elements; ++i) {
            ClusterSlots slots;
            for (j = 0; j < reply->element[i]->elements; ++j) {
                if (reply->element[i]->element[j]->type
                    == REDIS_REPLY_INTEGER)
                {
                    if (j == 0)
                        slots.set_from(reply->element[i]->element[j]->integer);
                    if (j == 1)
                        slots.set_to(reply->element[i]->element[j]->integer);

                }
                if (reply->element[i]->element[j]->type
                        == REDIS_REPLY_ARRAY)
                {
                    pair<string, int32_t> ip_port;
                    for (k = 0; k < reply->element[i]->element[j]->elements; ++k) {
                        if (reply->element[i]->element[j]->element[k]->type
                                == REDIS_REPLY_STRING)
                        {
                            ip_port.first =
                                reply->element[i]->element[j]->element[k]->str;
                        }
                        if (reply->element[i]->element[j]->element[k]->type
                                == REDIS_REPLY_INTEGER)
                        {
                            ip_port.second = reply->element[i]->element[j]
                                             ->element[k]->integer;
                        }
                    }
                    slots.add_node_info(ip_port);
                }
            }
            slots_.push_back(slots);
        }
        freeReplyObject(reply);
    }

    return 0;
}

bool ClusterClient::startup()
{
    vector<ClusterSlots>::iterator itr = slots_.begin();
    for (; itr != slots_.end(); ++itr) {
        vector<pair<string, int32_t> >::iterator itr2 =itr->ip_ports_.begin();
        bool is_master = true;
        for (; itr2 != itr->ip_ports_.end(); ++itr2) {
            ClusterRedis *cr = new ClusterRedis;
            if (cr->Init(itr2->first.c_str(), itr2->second)) {
                if (is_master) is_master = false;
                continue;
            }
            itr->add_node(cr, is_master);
            is_master = false;
        }
    }
    return true;
}

int32_t ClusterClient::Uninit()
{
    map<string, ClusterRedis *>::iterator itr = clients.begin();
    for (; itr != clients.end(); ++itr) {
        logg("DEBUG", "%s, %p", itr->first.c_str(), itr->second);
        itr->second->UnInit();

        delete itr->second;
    }
    // XXX TODO: free memory of slots_ client
    return 0;
}

void ClusterClient::show_clients()
{
    map<string, ClusterRedis *>::iterator itr = clients.begin();
    for (; itr != clients.end(); ++itr) {
        logg("DEBUG", "%s, %p", itr->first.c_str(), itr->second);
    }
}

void ClusterClient::show_slots()
{
    vector<ClusterSlots>::iterator itr = slots_.begin();
    int32_t count = 0;
    for (; itr != slots_.end(); ++itr) {
        cout << ++count << "):" << endl;
        itr->show_slot();
    }
}

ClusterRedis *ClusterClient::get_slots_client(const char *key, bool is_write)
{
    ClusterRedis *client = NULL;
    if (key == NULL) return NULL;
    int32_t slot_id = keyHashSlot(key, (int)strlen(key));

    vector<ClusterSlots>::iterator itr = slots_.begin(); 
    for (; itr != slots_.end(); ++itr) {
        if (slot_id <= itr->get_to() && slot_id >= itr->get_from()) {
            break;
        }
    }

    if (itr != slots_.end()) {
        client = itr->get_client(is_write);
    }

    return client;
}

void ClusterClient::ip_list_unserailize(const char *ip_list)
{
    if (NULL == ip_list) return;
    char *buf = new char[1024 * 1024];
    strcpy(buf, ip_list);
    char *saveptr;
    char *tokenPtr = strtok_r(buf, REDIS_IP_SPLIT_CHAR, &saveptr);
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
        tokenPtr = strtok_r(NULL, REDIS_IP_SPLIT_CHAR, &saveptr);
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
        if (clients.empty()) return CLUSTER_ERR;
        curr_cr_ = clients.begin()->second;
        if (clients.empty() || curr_cr_ == NULL) {
            cout << "no current client" << endl;
            return CLUSTER_ERR;
        }
    }
    ret = curr_cr_->String_Set(key, value, expiration);

    if (ret != NULL && ret->errorno == REDIS_ERROR_MOVE) {
        map<string, ClusterRedis *>::iterator itr;
        if ((itr = clients.find(ret->ip_port)) != clients.end()) {
            cr = itr->second;
            curr_cr_ = cr;
        } else {
            if (add_new_client(ret->ip_port) < 0) {
                curr_cr_->ReleaseRetInfoInstance(ret);
                return CLUSTER_ERR;
            }
        }

        curr_cr_->ReleaseRetInfoInstance(ret);
        ret = curr_cr_->String_Set(key, value, expiration);
        if (ret == NULL || ret->errorno == REDIS_ERROR_REQ) {
            // the MOVED master node can't connect
            // XXX:TODO connect to slave node
            curr_cr_->ReleaseRetInfoInstance(ret);
            return CLUSTER_ERR_REQ;
        } else {
            curr_cr_->ReleaseRetInfoInstance(ret);
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
        char ip_port[32] = {0};
        snprintf(ip_port, IP_ADDR_LEN, "%s:%d",
                 curr_cr_->get_ip(), curr_cr_->get_port());
        clients.erase(string(ip_port));
        curr_cr_->ReleaseRetInfoInstance(ret);
        curr_cr_->UnInit();
        curr_cr_ = NULL;
        res = String_Set(key, value, expiration);
        return res;
    } else {
        res = ret->errorno;
        curr_cr_->ReleaseRetInfoInstance(ret);
        return res;
    }

    curr_cr_->ReleaseRetInfoInstance(ret);
    return res;
}

int32_t ClusterClient::String_Get(const char *key, string &value)
{
    RetInfo *ret = NULL;
    int32_t res = 0;
    ClusterRedis *cr = NULL;
    if (!curr_cr_) {
        if (clients.empty()) return CLUSTER_ERR;
        curr_cr_ = clients.begin()->second;
        if (clients.empty() || curr_cr_ == NULL) return CLUSTER_ERR;
    }
    ret = curr_cr_->String_Get(key, value);

    if (ret != NULL && ret->errorno == REDIS_ERROR_MOVE) {
        map<string, ClusterRedis *>::iterator itr;
        if ((itr = clients.find(ret->ip_port)) != clients.end()) {
            cr = itr->second;
            curr_cr_ = cr;
        } else {
            if (add_new_client(ret->ip_port) < 0) {
                curr_cr_->ReleaseRetInfoInstance(ret);
                return CLUSTER_ERR;
            }
        }

        curr_cr_->ReleaseRetInfoInstance(ret);
        ret = curr_cr_->String_Get(key, value);
        if (ret == NULL || ret->errorno == REDIS_ERROR_REQ) {
            // the MOVED master node can't connect
            // XXX:TODO connect to slave node
            curr_cr_->ReleaseRetInfoInstance(ret);
            return CLUSTER_ERR;
        } else {
            curr_cr_->ReleaseRetInfoInstance(ret);
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
        char ip_port[32] = {0};
        snprintf(ip_port, IP_ADDR_LEN, "%s:%d",
                 curr_cr_->get_ip(), curr_cr_->get_port());
        clients.erase(string(ip_port));
        curr_cr_->ReleaseRetInfoInstance(ret);
        curr_cr_->UnInit();
        curr_cr_ = NULL;
        res = String_Get(key, value);
        return res;
    } else {
        res = ret->errorno;
        curr_cr_->ReleaseRetInfoInstance(ret);
        return res;
    }

    curr_cr_->ReleaseRetInfoInstance(ret);
    return res;
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
        if (cr->Init(ip_buf, port)) {
            cr->UnInit();
            delete cr;
            return CLUSTER_ERR;
        }
        pair<map<string, ClusterRedis *>::iterator, bool> ret;
        ret = clients.insert(pair<string,
                       ClusterRedis *>(string(ip_addr), cr));
        if (ret.second == false) {
            logg("ERROR", "insert <%s, %p> failed, already existed!",
                 ret.first->first.c_str(), ret.first->second);
            cr->UnInit();
            delete cr;
            return CLUSTER_ERR;
        }
        curr_cr_ = cr;

        return CLUSTER_OK;
    }

    return CLUSTER_ERR;
}
