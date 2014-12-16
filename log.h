#ifndef __LOG_H
#define __LOG_H

#include <stdio.h>

#if 1
#define logg(type, fmt, args...)  printf("%s:%d | "fmt"\n", __func__, __LINE__, ##args)
#else
#define logg(type, fmt, args...)
#endif
#endif
