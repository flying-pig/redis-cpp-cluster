CFLAGS=-g -I. -I/usr/local/include/hiredis
LDFLAGS=-g -L./ -lemarrediscluster -L/usr/local/lib -lhiredis

objecets=Cluster_Redis.o cluster_client.o cluster_slots.o crc16.o

lib_exe=libemarrediscluster.a test cluster_test

all: $(lib_exe)

libemarrediscluster.a: $(objecets)
	ar r $@ $^
	rm *.o

libemarrediscluster.so: $(objecets)
	g++ -fPIC -shared -o $@ $^

test: test.o
	g++ -o $@ $^ $(LDFLAGS)

cluster_test: cluster_test.o
	g++ -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	g++ -c $^ $(CFLAGS)

%.c: %.c
	gcc -c $^ $(CFLAGS)

clean:
	rm *.o -f $(lib_exe)

.PHONY: libemarrediscluster.so
