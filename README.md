redis-cpp-cluster
=================

Redis cluster client in C++ for the official cluster support targeted for redis 3.0.

This a wrapper of hiredis, and hiredis like API

##Feature
1. The base capability, following the Moved slots
2. Compute the slot id of the key
3. Reconnect and Reslots when comes to an error
4. Read slave node when can't connect to master

##Dependencies
Only hiredis needed
