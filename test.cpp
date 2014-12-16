#include <iostream>
#include <cstdlib>
#include  <sys/time.h>
#include "Cluster_Redis.h"
#include "cluster_client.h"
#include "log.h"

using std::cout;
using std::endl;

int main(int argc, char** args)
{
    (void)argc;
    (void)args;
    ClusterClient cr;

    cr.Init("127.0.0.1:7010;127.0.0.1:7011;127.0.0.1:7002");
    cr.show_clients();
    struct timeval start;
    struct timeval end;
    int32_t time_use_usec;
    int32_t time_use_msec;
    gettimeofday(&start, NULL);
    int res = cr.String_Set("aaaa01", "10000000000", 300);
    gettimeofday(&end, NULL);

    time_use_usec = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
    cout << "return value : " << res << endl;
    cout << "time use: " << time_use_usec << "usec" << endl;
    time_use_msec = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
    cout << "time use: " << time_use_msec << "msec" << endl;
    //std::exit(2);
    cr.String_Set("aaaa02", "20000000000", 300);
    cr.String_Set("aaaa03", "30000000000", 300);
    cr.String_Set("aaaa04", "40000000000", 300);

    cout << "Get testing ........" << endl;
    string value1;
    string value2;
    string value3;
    string value4;
    string value5;


    cr.String_Get("aaaa01", value1);
    cr.String_Get("aaaa02", value2);
    cr.String_Get("aaaa03", value3);
    gettimeofday(&start, NULL);
    cr.String_Get("aaaa04", value4);
    gettimeofday(&end, NULL);
    time_use_usec = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
    cout << "time use: " << time_use_usec << "usec" << endl;
    res = cr.String_Get("aaaa05", value5);
    printf("aaaa5 res: %d\n", res);

    cout << "aaaa01: " << value1 << "\n"
         << "aaaa02: " << value2 << "\n"
         << "aaaa03: " << value3 << "\n"
         << "aaaa04: " << value4 << "\n"
         << "aaaa05: " << value5 << endl;
}
