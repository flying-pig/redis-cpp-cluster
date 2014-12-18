/*
 * Copyright (c) 2008-2014, Chen Huaying <chying.ren@gmail.com>
 * All Rights Reserved.
 *
 * redis cluster slots
 */

#ifndef  CLUSTER_SLOTS_H_
#define  CLUSTER_SLOTS_H_

#include  <stdint.h>
#include  <vector>
#include  "Cluster_Redis.h"

using std::vector;
using std::pair;

enum CLUSTER_NODE_TYPE {
    CLUSTER_NODE_ALL,
    CLUSTER_NODE_MASTER,
    CLUSTER_NODE_SLAVE
};

class RedisNodeGroup {
    public:
        explicit RedisNodeGroup():master_(NULL),next_(0) { seed_ = time(NULL); };
        ~RedisNodeGroup(){};
        void set_master(ClusterRedis *cr);
        void add_node(ClusterRedis *cr);
        void show_nodes();
        void free_clients();
        ClusterRedis *get_client(CLUSTER_NODE_TYPE type);
        inline int32_t get_nodes_count() { return nodes_.size(); }
    private:
        vector<ClusterRedis *> nodes_;
        ClusterRedis *master_;
        unsigned int seed_;
        int32_t next_;
};

class ClusterSlots {
    public:
        explicit ClusterSlots():from_(0), to_(0) {};
        ~ClusterSlots(){};
        ClusterSlots(int32_t from, int32_t to);
        void add_node_info(pair<string, int32_t> ip_port);
        void show_slot();
        inline void set_from(int32_t from) { from_ = from; };
        inline int32_t get_from() { return from_; }
        inline void set_to(int32_t to) { to_ = to; };
        inline int32_t get_to() { return to_; }
        inline void add_node(ClusterRedis *cr, bool flag);
        inline void free_clients();
        inline ClusterRedis *get_client(CLUSTER_NODE_TYPE type) {
            return node_group_.get_client(type);
        }
        inline int32_t get_nodes_count() { return node_group_.get_nodes_count(); }

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

inline void ClusterSlots::free_clients()
{
    node_group_.free_clients();
}



#endif  /*CLUSTER_SLOTS_H_*/
