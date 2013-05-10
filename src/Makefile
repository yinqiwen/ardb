#Makefile
#
# Created on: 2013-3-28
#     Author: wqy

CXX=g++
CC=gcc

SNAPPY_PATH=../deps/snappy
LEVELDB_PATH=../deps/leveldb
KCDB_PATH=../deps/kyotocabinet-1.2.76
LMDB_PATH=../deps/mdb/libraries/liblmdb
JEMALLOC_PATH=../deps/jemalloc-3.3.1

CXXFLAGS=-Wall -Wno-format -g  -D_FILE_OFFSET_BITS=64 -fPIC
CCFLAGS=-Wall -std=gnu99 -O2 -pedantic -Wno-format -g -D_FILE_OFFSET_BITS=64 -fPIC
LDFLAGS=-g 

INCS=-I./ -I./util -I${LEVELDB_PATH}/include -I${KCDB_PATH} -I${LMDB_PATH} -I../deps/cpp-btree 

LIBS= ${SNAPPY_PATH}/.libs/libsnappy.a -lpthread

%.o : %.cpp
	${CXX} -c ${CXXFLAGS} ${INCS} $< -o $@

%.o : %.c
	${CC} -c ${CCFLAGS} ${INCS} $< -o $@


CHANNEL_CPP_VPATH=channel channel/socket channel/fifo channel/codec channel/timer channel/signal
CHANNEL_C_VPATH=channel/redis 
CHANNEL_CPPFILES := $(foreach dir, $(CHANNEL_CPP_VPATH), $(wildcard $(dir)/*.cpp))
CHANNEL_CFILES := $(foreach dir, $(CHANNEL_C_VPATH), $(wildcard $(dir)/*.c))
CHANNEL_OBJECTS := $(patsubst %.cpp, %.o, $(CHANNEL_CPPFILES))
CHANNEL_OBJECTS := $(CHANNEL_OBJECTS) $(patsubst %.c, %.o, $(CHANNEL_CFILES))

UTIL_VPATH=./util ./util/exception  ./util/thread
UTIL_CPPFILES := $(foreach dir, $(UTIL_VPATH), $(wildcard $(dir)/*.cpp))
UTIL_OBJECTS := $(patsubst %.cpp, %.o, $(UTIL_CPPFILES)) ./util/sha1.o
CORE_OBJECTS := ardb.o ardb_data.o hash.o kv.o lists.o logger.o sets.o \
                zsets.o strings.o  table.o sort.o\
                $(UTIL_OBJECTS)

LEVELDB_ENGINE :=  engine/leveldb_engine.o       
KCDB_ENGINE :=  engine/kyotocabinet_engine.o     
LMDB_ENGINE :=  engine/lmdb_engine.o     
LEVELDB_TEST := ../test/leveldb.o 
KCDB_TEST := ../test/kcdb.o
LMDB_TEST := ../test/lmdb.o

SERVER_OBJECTS := ardb_server.o slowlog.o clients.o replication.o pubsub.o oplogs.o main.o

#DIST_LIB = libardb.so
DIST_LIBA = libardb.a

all: lib leveldb test

$(DIST_LIB):$(CORE_OBJECTS)
	${CXX} -shared -o $@ $^

$(DIST_LIBA):$(CORE_OBJECTS)
	ar rcs $@ $^

lib:$(DIST_LIBA)

server:$(SERVER_OBJECTS) $(CORE_OBJECTS) $(CHANNEL_OBJECTS)

leveldb:server $(LEVELDB_ENGINE)
	${CXX} -o ardb-server $(SERVER_OBJECTS) $(LEVELDB_ENGINE) $(CORE_OBJECTS) $(CHANNEL_OBJECTS) $(LIBS) $(LEVELDB_PATH)/libleveldb.a 

kcdb:server $(KCDB_ENGINE)
	${CXX} -o ardb-server $(SERVER_OBJECTS) $(KCDB_ENGINE) $(CORE_OBJECTS) $(CHANNEL_OBJECTS) $(LIBS) -L/usr/local/lib -lkyotocabinet -lz

lmdb:server $(LMDB_ENGINE)
	${CXX} -o ardb-server $(SERVER_OBJECTS) $(LMDB_ENGINE) $(CORE_OBJECTS) $(CHANNEL_OBJECTS) $(LIBS) ${LMDB_PATH}/liblmdb.a

test:leveldb-test kcdb-test lmdb-test

leveldb-test:$(LEVELDB_ENGINE) $(LEVELDB_TEST) $(CORE_OBJECTS) 
	${CXX} -o leveldb-test $(LEVELDB_ENGINE) $(LEVELDB_TEST) $(CORE_OBJECTS) $(LIBS) $(LEVELDB_PATH)/libleveldb.a
	
kcdb-test:$(KCDB_TEST) $(CORE_OBJECTS) $(KCDB_ENGINE)
	${CXX} -o kcdb-test $(KCDB_TEST) $(KCDB_ENGINE) $(CORE_OBJECTS) $(LIBS) ${KCDB_PATH}/libkyotocabinet.a -lz

lmdb-test:$(LMDB_TEST) $(CORE_OBJECTS) $(LMDB_ENGINE)
	${CXX} -o lmdb-test $(LMDB_TEST) $(LMDB_ENGINE) $(CORE_OBJECTS) $(LIBS) ${LMDB_PATH}/liblmdb.a

clean:
	rm -f $(LEVELDB_TEST) $(KCDB_TEST) ${CORE_OBJECTS} ${LEVELDB_OBJECTS} \
	      $(SERVER_OBJECTS) $(CHANNEL_OBJECTS) $(DIST_LIBA) $(DIST_LIB)   \
	      $(LEVELDB_ENGINE) $(KCDB_ENGINE) leveldb-test kcdb-test lmdb-test ardb-server
