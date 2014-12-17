/*
 * Copyright (c) 2008-2014, Chen Huaying <chying.ren@gmail.com>
 * All Rights Reserved.
 *
 * redis cluster slots
 */

#include  <stdint.h>
#include  <vector>
#include  "Cluster_Redis.h"

using std::vector;
using std::pair;

#ifndef  CLUSTER_SLOTS_H_
#define  CLUSTER_SLOTS_H_

class RedisNodeGroup {
    public:
        explicit RedisNodeGroup() {};
        ~RedisNodeGroup(){};
        void set_master(ClusterRedis *cr);
        void add_node(ClusterRedis *cr);
        void show_nodes();
    private:
        vector<ClusterRedis *> nodes_;
        ClusterRedis *master_;
};

class ClusterSlots {
    public:
        explicit ClusterSlots() {};
        ~ClusterSlots(){};
        ClusterSlots(int32_t from, int32_t to);
        void add_node_info(pair<string, int32_t> ip_port);
        void show_slot();
        inline void set_from(int32_t from) { from_ = from; };
        inline void set_to(int32_t to) { to_ = to; };
        inline void add_node(ClusterRedis *cr, bool flag);

        vector<pair<string, int32_t> > ip_ports_;

    private:
        RedisNodeGroup node_group_;
        int32_t from_;
        int32_t to_;
};

inline void ClusterSlots::add_node(ClusterRedis *cr, bool flag)
{
    node_group_.add_node(cr);
    if (flag) node_group_.set_master(cr);
}



#endif  /*CLUSTER_SLOTS_H_*/
