/*
 * Copyright (c) 2008-2014, Chen Huaying <chying.ren@gmail.com>
 * All Rights Reserved.
 */

#include <iostream>

#include "cluster_slots.h"
using std::cout;
using std::endl;

void RedisNodeGroup::set_master(ClusterRedis *cr)
{
    master_ = cr;
}

void RedisNodeGroup::add_node(ClusterRedis *cr)
{
    nodes_.push_back(cr);
}


// Begin of class ClusterSlots
ClusterSlots::ClusterSlots(int32_t from, int32_t to)
    :from_(from), to_(to)
{
}

void ClusterSlots::add_node_info(pair<string, int32_t> ip_port)
{
    ip_ports_.push_back(ip_port);
}

void ClusterSlots::show_slot()
{
    cout << "\t from: " << from_ << "\n"
         << "\t to:   " << to_ << endl;
    vector<pair<string, int32_t> >::iterator itr;
    for (itr = ip_ports_.begin(); itr != ip_ports_.end(); ++itr) {
        cout <<"\t node: " << itr->first << ":" << itr->second << endl;
    }
}
