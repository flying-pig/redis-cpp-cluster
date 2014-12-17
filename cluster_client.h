/*
 * Copyright (c) 2008-2014, Chen Huaying <chying.ren@gmail.com>
 * All Rights Reserved.
 *
 * redis cluster client, wrapper of Cluster_Redis
 */

#ifndef CLUSTER_CLIENT_H_
#define CLUSTER_CLIENT_H_

#include <vector>
#include "Cluster_Redis.h"
#include "cluster_slots.h"

#define CLUSTER_OK          0
#define CLUSTER_ERR        -1
#define CLUSTER_ERR_DOWN    REDIS_ERROR_DOWN
#define CLUSTER_ERR_REQ     REDIS_ERROR_REQ

using std::pair;

class ClusterClient {

    public:
        explicit ClusterClient():curr_cr_(NULL) {};
        ~ClusterClient() {};
        int32_t Init(const char *redis_master_ips);
        int32_t Uninit();
        void show_clients();
        void show_slots();
        bool startup();
        int32_t String_Set(const char *key,
                const char *value,
                int32_t expiration);
        int32_t String_Get(const char *key, string &value);
        // notify: Set and Get interface must call this release func
        void ReleaseRetInfoInstance(RetInfo *ri);
        ClusterRedis *get_slots_client(const char *key, bool is_write);
        ClusterRedis *get_slots_client(const int32_t slot_id, bool is_write);

    private:
        map<string, ClusterRedis *> clients;
        std::vector<pair<string,int> > cluster_masters;
        ClusterRedis *curr_cr_;
        vector<ClusterSlots> slots_;

        // ip_list format: <ip:port>;<ip:port>;...
        // 192.168.0.1:6379;192.168.0.2:6379;...
        void ip_list_unserailize(const char *ip_list);
        // ip_addr format: <ip:port>
        // 192.168.0.1:6379
        int32_t add_new_client(const char *ip_addr);
        ClusterClient(const ClusterClient&);
        ClusterClient& operator=(const ClusterClient&);
};



#endif  /*CLUSTER_CLIENT_H_*/
