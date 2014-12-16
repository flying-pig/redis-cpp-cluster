/*
 * Copyright (c) 2008-2014, Chen Huaying <chying.ren@gmail.com>
 * All Rights Reserved.
 *
 * redis cluster slots
 */

#include  <cstdint>
#include  <vector>
#include  "Cluster_Redis.h"

using std::vector;

#ifndef  CLUSTER_SLOTS_H_
#define  CLUSTER_SLOTS_H_

class RedisNodeGroup {
    public:
        explicit RedisNodeGroup() {};
        ~RedisNodeGroup();
    private:
        vector<ClusterRedis> nodes_;
        ClusterRedis *master_;
};

class ClusterSlots {
    public:
        explicit ClusterSlots() {};
        ~ClusterSlots();
        int32_t startup();
    private:
        RedisNodeGroup node_group_;
        int32_t from_;
        int32_t to_;
};



#endif  /*CLUSTER_SLOTS_H_*/
