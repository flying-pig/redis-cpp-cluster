#include <iostream>
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

    cr.Init("127.0.0.1:7000;127.0.0.1:7001;127.0.0.1:7002");
    cr.show_clients();
    //cr.String_Set("aaaa01", "10000000000", 300);
    //cr.String_Set("aaaa02", "20000000000", 300);
    //cr.String_Set("aaaa03", "30000000000", 300);
    //cr.String_Set("aaaa04", "40000000000", 300);

    cout << "Get testing ........" << endl;
    string value1;
    string value2;
    string value3;
    string value4;
    string value5;


    cr.String_Get("aaaa01", value1);
    cr.String_Get("aaaa02", value2);
    cr.String_Get("aaaa03", value3);
    cr.String_Get("aaaa04", value4);
    cr.String_Get("aaaa05", value5);

    cout << "aaaa01: " << value1 << "\n"
         << "aaaa02: " << value2 << "\n"
         << "aaaa03: " << value3 << "\n"
         << "aaaa04: " << value4 << "\n"
         << "aaaa05: " << value5 << endl;
}
