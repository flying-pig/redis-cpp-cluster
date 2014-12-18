/*
 * Cluster_Redis.cpp
 *
 *  Created on: Aug 25, 2014
 *      Author: meiyuli
 */
#include "log.h"
#include "Cluster_Redis.h"

#include <stdlib.h>
#include <string.h>
#include <cstdio>

ClusterRedis::ClusterRedis():_redisIP(NULL), _redisPort(0),
_redisContext(NULL), _redisReply(NULL) {
	// TODO Auto-generated constructor stub

}

ClusterRedis::~ClusterRedis() {
	// TODO Auto-generated destructor stub
}

int32_t ClusterRedis::Init() {
	struct timeval timeout = { 1, 500000}; // 1.5 seconds
	_redisContext = redisConnectWithTimeout(_redisIP, _redisPort, timeout);
	if (!_redisContext || _redisContext->err) {
		if (_redisContext) {
			logg(ERROR, "fail to connect redis, ip: %s, port: %d, errorInfo: %s",
					_redisIP, _redisPort, _redisContext->errstr);
		} else {
			logg(ERROR, "fail to connect redis, ip: %s, port: %d",
					_redisIP, _redisPort);
		}
		return -2;
	}

	//test, connect Redis-server
	//if (this->Connect_Ping()) return -3;

	this->FreeSources();
	return 0;
}

int32_t ClusterRedis::Init(const char *redis_ip, const int32_t redis_port) {
	if (!redis_ip) return -1;

	struct timeval timeout = { 1, 500000}; // 1.5 seconds
	_redisContext = redisConnectWithTimeout(redis_ip, redis_port, timeout);
	if (!_redisIP) _redisIP = strdup(redis_ip);
	_redisPort = redis_port;
	if (!_redisContext || _redisContext->err) {
		if (_redisContext) {
			logg(ERROR, "fail to connect redis, ip: %s, port: %d, errorInfo: %s",
					redis_ip, redis_port, _redisContext->errstr);
		} else {
			logg(ERROR, "fail to connect redis, ip: %s, port: %d",
					redis_ip, redis_port);
		}
		return -2;
	}

	//test, connect Redis-server
	//if (this->Connect_Ping()) return -3;

	this->FreeSources();
	return 0;
}

int32_t ClusterRedis::UnInit() {
	if (_redisContext) redisFree(_redisContext);
	if (_redisReply) freeReplyObject(_redisReply);
	if (_redisIP) free(_redisIP);

	for (uint32_t i = 0; i < _retInfoList.size(); i++) {
		RetInfo *ri = _retInfoList[i];
		if (ri) {
			if (ri->ip_port) {
				free(ri->ip_port);
				ri->ip_port = NULL;
			}
			free(ri);
			ri = NULL;
		}
	}

	_redisContext = NULL;
	_redisReply = NULL;
	_redisIP = NULL;
	_retInfoList.clear();

	return 0;
}

int32_t ClusterRedis::FreeSources() {
	if (_redisReply) {
		freeReplyObject(_redisReply);
		_redisReply = NULL;
	}

	return 0;
}

int32_t ClusterRedis::ReConnect() {
	if (_redisContext) redisFree(_redisContext);
	if (_redisReply) freeReplyObject(_redisReply);
	_redisContext = NULL;
	_redisReply = NULL;

    logg(ERROR, "ReConnect, ip: %s, port: %d\n", _redisIP, _redisPort);
	if (this->Init()) {
		logg(ERROR, "fail to ReConnect, ip: %s, port: %d\n", _redisIP, _redisPort);
		return -1;
	}

	return 0;
}

int32_t ClusterRedis::StatusCheck() {
	if (_redisContext) return 0;
	return this->ReConnect();
}

int32_t ClusterRedis::ErrorInfoCheck(char *errorInfo, RetInfo* ri) {
    logg(ERROR, "%s", errorInfo);
	if (!errorInfo || !ri) return -1;

	if (!strncmp(errorInfo, "ASK", 3)) {
		ri->errorno = REDIS_ERROR_ASK;
	} else if (!strncmp(errorInfo, "MOVED", 5)) {
		ri->errorno = REDIS_ERROR_MOVE;
	} else if (!strncmp(errorInfo, "CLUSTERDOWN", 11)){
		ri->errorno = REDIS_ERROR_DOWN;
		return 0;
	} else {
		return -2;
	}

	char *f_b = strchr(errorInfo, ' ');
	if (!f_b) return -3;

	char *s_b = strchr(f_b + 1, ' ');
	if (!s_b) return -3;

	*s_b = 0;
	ri->ip_port = strdup(s_b + 1);
	ri->slot = atoi(f_b + 1);
	*s_b = ' ';

	return 0;
}

RetInfo* ClusterRedis::GetRetInfoInstance() {
	RetInfo *ri = NULL;
	if (_retInfoList.size() == 0) {
		ri = (RetInfo*)calloc(1, sizeof(RetInfo));
	} else {
		ri = _retInfoList.front();
		_retInfoList.pop_front();
	}

	return ri;
}

void ClusterRedis::ReleaseRetInfoInstance(RetInfo *ri) {
	if (!ri) return;
	if (ri->ip_port) {
		free(ri->ip_port);
		ri->ip_port = NULL;
	}

	ri->errorno = REDIS_ERROR_OK;
	ri->slot = 0;

	if (_retInfoList.size() > RETINFO_LIST_MAX_SIZE) {
		free(ri);
		ri = NULL;
	} else {
		_retInfoList.push_back(ri);
	}
}

redisReply *ClusterRedis::redis_command(const char *format, ...)
{
    if (format == NULL || *format == '\0') return NULL;
    va_list ap;
    redisReply *reply = NULL;
    va_start(ap, format);
    reply = (redisReply*)redis_vCommand(format, ap);
    va_end(ap);
    return reply;
}

redisReply *ClusterRedis::redis_vCommand(const char *format, va_list ap)
{
    if (format == NULL || *format == '\0') return NULL;

    if (this->StatusCheck()) {
	return NULL;
    }
    redisReply *reply = NULL;
    reply = (redisReply *)redisvCommand(_redisContext, format, ap);
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
	if (reply) {
	    char buf[1024] = {0};
	    vsnprintf(buf, 1024, format, ap);
	    logg("ERROR", "%s", buf);
	}
	FreeSources();
	if (ReConnect() < 0) return NULL;
	reply = (redisReply *)redisvCommand(_redisContext, format, ap);
	return reply;
    }

    return reply;
}

/* < cluster op  */
RetInfo* ClusterRedis::SendAsk() {
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

	_redisReply = (redisReply*)redisCommand(_redisContext, "asking");
	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			logg(ERROR, "fail to SendAsk, errorInfo: %s",
					_redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type == REDIS_REPLY_STATUS && !strcmp(_redisReply->str, "OK")) {
		ri->errorno = REDIS_ERROR_OK;
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}

	this->FreeSources();
	return ri;
}

int32_t ClusterRedis::Cluster_GetSlots(deque<SlotInfo*> &slot)
{
	if (this->StatusCheck()) return -1;

	_redisReply = (redisReply*)redisCommand(_redisContext, "cluster slots");
	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			logg(ERROR, "fail to Cluster_GetSlots, errorInfo: %s",
					_redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		return -2;
	}

	if (_redisReply->type == REDIS_REPLY_ARRAY) {
		for(uint32_t i = 0; i < _redisReply->elements; i++) {
			SlotInfo *si = (SlotInfo*)calloc(1, sizeof(SlotInfo));
			if (!si) return -3;
			si->begin = _redisReply->element[i]->element[0]->integer;
			si->end = _redisReply->element[i]->element[1]->integer;
			strcpy(si->ip, _redisReply->element[i]->element[2]->element[0]->str);
			si->port = _redisReply->element[i]->element[2]->element[1]->integer;
			slot.push_back(si);
		}
	}

	this->FreeSources();
	return 0;
}

/*< expire operation */
RetInfo* ClusterRedis::Expire(const char* key, int32_t expiration) {
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (!key) {
		ri->errorno = REDIS_ERROR_ARGS;
		return ri;
	}

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

	if (expiration > 0) {
		_redisReply = (redisReply*)redisCommand(_redisContext,"expire %s %d", key, expiration);
	}

	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			if (!this->ErrorInfoCheck(_redisReply->str, ri)) {
				this->FreeSources();
				return ri;
			}
			logg(ERROR, "fail to Expire, key: %s, errorInfo: %s",
					key, _redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type != REDIS_REPLY_ERROR) {
		ri->errorno = REDIS_ERROR_OK;
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}
	this->FreeSources();

	return ri;
}

/*< string operation */
RetInfo* ClusterRedis::String_Set(const char *key, const char *value, int32_t expiration)
{
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (!key || !value) {
		ri->errorno = REDIS_ERROR_ARGS;
		return ri;
	}

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

	if (expiration > 0) {
		_redisReply = (redisReply*)redisCommand(_redisContext,"SET %s %s ex %d", key, value, expiration);
	} else {
		_redisReply = (redisReply*)redisCommand(_redisContext,"SET %s %s", key, value);
	}

	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			if (!this->ErrorInfoCheck(_redisReply->str, ri)) {
				this->FreeSources();
				return ri;
			}
			logg(ERROR, "fail to String_Set, key: %s, value: %s, errorInfo: %s",
					key, value, _redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type == REDIS_REPLY_STATUS && !strcmp(_redisReply->str, "OK")) {
		ri->errorno = REDIS_ERROR_OK;
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}
	this->FreeSources();

	return ri;
}

RetInfo* ClusterRedis::String_Get(const char *key, string &value)
{
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (!key) {
		ri->errorno = REDIS_ERROR_ARGS;
		return ri;
	}

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

    _redisReply = (redisReply*)redisCommand(_redisContext,"Get %s", key);
	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			if (!this->ErrorInfoCheck(_redisReply->str, ri)) {
				this->FreeSources();
				return ri;
			}
			logg(ERROR, "fail to String_Get, key: %s, errorInfo: %s",
					key, _redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type == REDIS_REPLY_STRING) {
		ri->errorno = REDIS_ERROR_OK;
		value = _redisReply->str;
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}
	this->FreeSources();

    return ri;
}

/*< list operation */
RetInfo* ClusterRedis::List_Lpop(const char *key, string &value)
{
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (!key) {
		ri->errorno = REDIS_ERROR_ARGS;
		return ri;
	}

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

	_redisReply = (redisReply*)redisCommand(_redisContext,"lpop %s", key);
	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			if (!this->ErrorInfoCheck(_redisReply->str, ri)) {
				this->FreeSources();
				return ri;
			}
			logg(ERROR, "fail to List_Lpop, key: %s, errorInfo: %s",
					key, _redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type == REDIS_REPLY_STRING) {
		ri->errorno = REDIS_ERROR_OK;
		value = _redisReply->str;
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}
	this->FreeSources();

	return ri;
}

/*< hash operation */
RetInfo* ClusterRedis::Hash_Hgetall(const char *key, map<string, string> &res)
{
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (!key) {
		ri->errorno = REDIS_ERROR_ARGS;
		return ri;
	}

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

	_redisReply = (redisReply*)redisCommand(_redisContext,"hgetall %s", key);
	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			if (!this->ErrorInfoCheck(_redisReply->str, ri)) {
				this->FreeSources();
				return ri;
			}
			logg(ERROR, "fail to Hash_Hgetall, key: %s, errorInfo: %s",
					key, _redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type == REDIS_REPLY_ARRAY) {
		ri->errorno = REDIS_ERROR_OK;

		int32_t loopCount = 0;
		int32_t key_i;
		int32_t value_i;
		for(uint32_t i = 0; i < _redisReply->elements; i++) {
			loopCount += 1;
			if (loopCount == 1) {
				key_i = i;
			}

			if (loopCount == 2) {
				value_i = i;
				res[_redisReply->element[key_i]->str] = _redisReply->element[value_i]->str;
				loopCount = 0;
			}
		}
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}
	this->FreeSources();

	return ri;

}

RetInfo* ClusterRedis::Hash_Hset(const char *key, const char *field, const char *value) {
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (!key) {
		ri->errorno = REDIS_ERROR_ARGS;
		return ri;
	}

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

	_redisReply = (redisReply*)redisCommand(_redisContext,"hset %s %s %s", key, field, value);
	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			if (!this->ErrorInfoCheck(_redisReply->str, ri)) {
				this->FreeSources();
				return ri;
			}
			logg(ERROR, "fail to Hash_Hset, key: %s, field: %s, value: %s, errorInfo: %s",
					key, field, value, _redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type != REDIS_REPLY_ERROR) {
		ri->errorno = REDIS_ERROR_OK;
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}
	this->FreeSources();

	return ri;
}

RetInfo* ClusterRedis::Hash_Hincyby(const char *key, const char *field, const char *value) {
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (!key) {
		ri->errorno = REDIS_ERROR_ARGS;
		return ri;
	}

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

	_redisReply = (redisReply*)redisCommand(_redisContext,"hincrby %s %s %s", key, field, value);
	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			if (!this->ErrorInfoCheck(_redisReply->str, ri)) {
				this->FreeSources();
				return ri;
			}
			logg(ERROR, "fail to Hash_Hincyby, key: %s, field: %s, value: %s, errorInfo: %s",
					key, field, value, _redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type != REDIS_REPLY_ERROR) {
		ri->errorno = REDIS_ERROR_OK;
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}
	this->FreeSources();

	return ri;
}

RetInfo* ClusterRedis::Hash_Hincybyfloat(const char *key, const char *field, double &value) {
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (!key) {
		ri->errorno = REDIS_ERROR_ARGS;
		return ri;
	}

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

	_redisReply = (redisReply*)redisCommand(_redisContext,"hincrbyfloat %s %s %.2f", key, field, value);
	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			if (!this->ErrorInfoCheck(_redisReply->str, ri)) {
				this->FreeSources();
				return ri;
			}
			logg(ERROR, "fail to Hash_Hincybyfloat, key: %s, field: %s, value: %.2f, errorInfo: %s",
					key, field, value, _redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type != REDIS_REPLY_ERROR) {
		ri->errorno = REDIS_ERROR_OK;
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}
	this->FreeSources();

	return ri;
}

RetInfo* ClusterRedis::Set_Sadd(const char *key, const char *member) {
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (!key) {
		ri->errorno = REDIS_ERROR_ARGS;
		return ri;
	}

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

	_redisReply = (redisReply*)redisCommand(_redisContext,"sadd %s %s", key, member);
	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			if (!this->ErrorInfoCheck(_redisReply->str, ri)) {
				this->FreeSources();
				return ri;
			}
			logg(ERROR, "fail to Set_Sadd, key: %s, member: %s, errorInfo: %s",
					key, member, _redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type != REDIS_REPLY_ERROR) {
		ri->errorno = REDIS_ERROR_OK;
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}
	this->FreeSources();

	return ri;
}

RetInfo* ClusterRedis::Lua_Script(const char* script, const char *key, const char* field) {
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (!key) {
		ri->errorno = REDIS_ERROR_ARGS;
		return ri;
	}

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

	_redisReply = (redisReply*)redisCommand(_redisContext,"eval %s 1 %s %s", script, key, field);
	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			if (!this->ErrorInfoCheck(_redisReply->str, ri)) {
				this->FreeSources();
				return ri;
			}
			logg(ERROR, "fail to Lua_Script, script: %s, key: %s, errorInfo: %s",
					script, key, _redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type == REDIS_REPLY_STATUS && !strcmp(_redisReply->str, "OK")) {
		ri->errorno = REDIS_ERROR_OK;
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}
	this->FreeSources();

	return ri;
}

RetInfo* ClusterRedis::Lua_Script(const char* script, const char *key, const char* field1, const char* field2) {
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (!key) {
		ri->errorno = REDIS_ERROR_ARGS;
		return ri;
	}

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

	_redisReply = (redisReply*)redisCommand(_redisContext,"eval %s 1 %s %s %s", script, key, field1, field2);
	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			if (!this->ErrorInfoCheck(_redisReply->str, ri)) {
				this->FreeSources();
				return ri;
			}
			logg(ERROR, "fail to Lua_Script, script: %s, key: %s, errorInfo: %s",
					script, key, _redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type == REDIS_REPLY_STATUS && !strcmp(_redisReply->str, "OK")) {
		ri->errorno = REDIS_ERROR_OK;
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}
	this->FreeSources();

	return ri;
}

RetInfo* ClusterRedis::Lua_Script(const char* script, const char *key, const char* field1, const char* field2, const char* field3) {
	RetInfo *ri = this->GetRetInfoInstance();
	if (!ri) return NULL;

	if (!key) {
		ri->errorno = REDIS_ERROR_ARGS;
		return ri;
	}

	if (this->StatusCheck()) {
		ri->errorno = REDIS_ERROR_STATUS;
		return ri;
	}

	_redisReply = (redisReply*)redisCommand(_redisContext,"eval %s 1 %s %s %s %s", script, key, field1, field2, field3);
	if (!_redisReply || _redisReply->type == REDIS_REPLY_ERROR) {
		if (_redisReply) {
			if (!this->ErrorInfoCheck(_redisReply->str, ri)) {
				this->FreeSources();
				return ri;
			}
			logg(ERROR, "fail to Lua_Script, script: %s, key: %s, errorInfo: %s",
					script, key, _redisReply->str);
		}

		this->FreeSources();
		this->ReConnect();
		ri->errorno = REDIS_ERROR_REQ;
		return ri;
	}

	if (_redisReply->type == REDIS_REPLY_STATUS && !strcmp(_redisReply->str, "OK")) {
		ri->errorno = REDIS_ERROR_OK;
	} else {
		ri->errorno = REDIS_ERROR_VALUE;
	}
	this->FreeSources();

	return ri;
}
