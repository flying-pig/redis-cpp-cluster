CFLAGS=-g -I. -I/usr/local/include/hiredis
LDFLAGS=-g -L./ -lemarrediscluster -L/usr/local/lib -lhiredis

objecets=Cluster_Redis.o cluster_client.o

all: libemarrediscluster.a test

libemarrediscluster.a: $(objecets)
	ar r $@ $^
	rm *.o

libemarrediscluster.so: $(objecets)
	g++ -fPIC -shared -o $@ $^

test: test.o
	g++ -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	g++ -c $^ $(CFLAGS)

clean:
	rm *.o libemarrediscluster.a

.PHONY: libemarrediscluster.a libemarrediscluster.so
