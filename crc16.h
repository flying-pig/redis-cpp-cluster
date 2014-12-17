/*
 * Copyright (c) 2008-2014, Chen Huaying <chying.ren@gmail.com>
 * All Rights Reserved.
 */

#ifndef CRC16_H_
#define CRC16_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

uint16_t crc16(const char *buf, int len);
unsigned int keyHashSlot(const char *key, int keylen);

#ifdef __cplusplus
} 
#endif


#endif  /*CRC16_H_*/


