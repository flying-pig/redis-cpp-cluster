/*
 * Cluster_Redis.h
 *
 *  Created on: Aug 25, 2014
 *      Author: meiyuli
 */

#ifndef CLUSTER_REDIS_H_
#define CLUSTER_REDIS_H_

#include <hiredis/hiredis.h>
#include <stdint.h>

#include <map>
#include <deque>
#include <string>

using std::deque;
using std::string;
using std::map;

#define RETINFO_LIST_MAX_SIZE 1000

#define REDIS_ERROR_OK 			0
#define REDIS_ERROR_ARGS 		-1
#define REDIS_ERROR_REQ 		-2
#define REDIS_ERROR_ASK 		-3
#define REDIS_ERROR_MOVE 		-4			// MOVE状态
#define REDIS_ERROR_STATUS 		-5			// 返回状态
#define REDIS_ERROR_VALUE 		-6			// 返回值异常
#define REDIS_ERROR_DOWN  		-7			// 集群节点挂掉


typedef struct _slot {
        int32_t begin; //slot begin No.
        int32_t end;   //slot end No.
        char ip[16];
        int32_t port;
}SlotInfo;

typedef struct _retinfo {
        int32_t errorno; //REDIS_ERROR_
        char *ip_port;
        int32_t slot;
}RetInfo;

class ClusterRedis {
private:
	int32_t Init();
public:
	explicit ClusterRedis();
	virtual ~ClusterRedis();

	int32_t Init(const char *redis_ip, const int32_t redis_port);
	int32_t UnInit();
	int32_t FreeSources();
	int32_t ReConnect();
	int32_t StatusCheck();
	int32_t ErrorInfoCheck(char *errorInfo, RetInfo* ri);
	RetInfo *GetRetInfoInstance();
	void ReleaseRetInfoInstance(RetInfo *ri);

	/* < cluster op  */
	RetInfo* SendAsk();
	int32_t Cluster_GetSlots(deque<SlotInfo*> &slot);

	/*< expire operation */
	RetInfo* Expire(const char *key, int32_t expiration);

	/*< string operation */
	RetInfo* String_Set(const char *key, const char *value, int32_t expiration);
    RetInfo* String_Get(const char *key, string &value);

	/*< list operation */
	RetInfo* List_Lpop(const char *key, string &value);

	/*< hash operation */
	RetInfo* Hash_Hgetall(const char *key, map<string, string> &res);

	RetInfo* Hash_Hset(const char *key, const char *field, const char *value);

	RetInfo* Hash_Hincyby(const char *key, const char *field, const char *value);

	RetInfo* Hash_Hincybyfloat(const char *key, const char *field, double &value);

	/*< set operation */
	RetInfo* Set_Sadd(const char *key, const char *member);

	/*< lua script operation */
	RetInfo* Lua_Script(const char* script, const char *key, const char* field);

	RetInfo* Lua_Script(const char* script, const char *key, const char* field1, const char* field2);
	RetInfo* Lua_Script(const char* script, const char *key, const char* field1, const char* field2, const char* field3);
	char *get_ip() { return _redisIP; }
	int32_t get_port() { return _redisPort; }
private:
	ClusterRedis(const ClusterRedis&);
	ClusterRedis& operator=(const ClusterRedis&);

private:
	char *_redisIP;
	int32_t _redisPort;
	redisContext *_redisContext;		 //redisClient context
	redisReply *_redisReply;			 //redisClient reply result
	deque<RetInfo *> _retInfoList;
};

#endif /* CLUSTER_REDIS_H_ */
