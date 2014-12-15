#ifndef __LOG_H
#define __LOG_H

#include <stdio.h>

#define logg(type, fmt, args...)  printf("%s:%d | "fmt"\n", __func__, __LINE__, ##args)
#endif
