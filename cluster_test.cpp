/*
 * Copyright (c) 2008-2014, Chen Huaying <chying.ren@gmail.com>
 * All Rights Reserved.
 *
 * Cluster test
 */

#include <iostream>
#include  <sys/time.h>
#include  <cstdio>
#include  <cstdlib>
#include  <cstring>
#include "Cluster_Redis.h"
#include "cluster_client.h"
#include "log.h"

using std::cout;
using std::endl;
int main(int argc, char *argv[])
{
    (void)argc;
    (void)argv;

    ClusterClient cr;

    cr.Init("127.0.0.1:7000;127.0.0.1:7001;127.0.0.1:7002");
    cr.show_clients();
    FILE *keys_fd = std::fopen("./test_keys1", "r");
    if (keys_fd == NULL) return 2;

    char *line;
    size_t len = 0;
    ssize_t read;
    struct timeval start;
    struct timeval end;
    int32_t time_use_msec;
    while ((read = getline(&line, &len, keys_fd)) != -1) {
        //printf("Retrieved line of length %zu :\n", read);
        char *ptr = line;
        ptr += std::strlen(line);
        while (--ptr) {
            if (isspace(*ptr)) {
                *ptr = '\0';
            } else {
                break;
            }
        };
        string value;
        RetInfo *ret = NULL;
        gettimeofday(&start, NULL);
        ret = cr.String_Get(line, value);
        gettimeofday(&end, NULL);
        if (ret != NULL) {
            time_use_msec = (end.tv_sec - start.tv_sec) * 1000 +
                            (end.tv_usec - start.tv_usec) / 1000;
            if (ret->errorno == REDIS_ERROR_OK) {
                //cout << "key: " << line << " = " << value << endl;
                cout << time_use_msec << " Get key: "
                     << line 
                     //<< " " << value
                     << " succeed"<< endl;
            } else {
                cout << time_use_msec << " Get key: "
                     << line << " failed "
                     << ret->errorno << endl;
            }
        } else {
            cout << "wrong connection" << endl;
        }
    }

    std::free(line);
}
