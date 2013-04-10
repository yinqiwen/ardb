#Makefile
#
# Created on: 2013-3-28
#     Author: wqy

CXX=g++
CC=gcc

SNAPPY_PATH=../deps/snappy
LEVELDB_PATH=../deps/leveldb
KCDB_PATH=../deps/kyotocabinet-1.2.76
GLOG_PATH=../deps/google-glog

CXXFLAGS=-Wall -Wno-format -g -O2 -D_FILE_OFFSET_BITS=64 -fPIC
CCFLAGS=-Wall -std=gnu99 -pedantic -Wno-format -g -O2 -D_FILE_OFFSET_BITS=64 -fPIC
LDFLAGS=-g 

INCS=-I./ -I./util -I${LEVELDB_PATH}/include -I${KCDB_PATH} -I${GLOG_PATH}/src

LIBS=${GLOG_PATH}/.libs/libglog.a

%.o : %.cpp
	${CXX} -c ${CXXFLAGS} ${INCS} $< -o $@

%.o : %.c
	${CC} -c ${CCFLAGS} ${INCS} $< -o $@


CHANNEL_CPP_VPATH=channel channel/socket channel/codec channel/timer channel/signal
CHANNEL_C_VPATH=channel/redis 
CHANNEL_CPPFILES := $(foreach dir, $(CHANNEL_CPP_VPATH), $(wildcard $(dir)/*.cpp))
CHANNEL_CFILES := $(foreach dir, $(CHANNEL_C_VPATH), $(wildcard $(dir)/*.c))
CHANNEL_OBJECTS := $(patsubst %.cpp, %.o, $(CHANNEL_CPPFILES))
CHANNEL_OBJECTS := $(CHANNEL_OBJECTS) $(patsubst %.c, %.o, $(CHANNEL_CFILES))

UTIL_VPATH=./util ./util/exception 
UTIL_CPPFILES := $(foreach dir, $(UTIL_VPATH), $(wildcard $(dir)/*.cpp))
UTIL_OBJECTS := $(patsubst %.cpp, %.o, $(UTIL_CPPFILES))
CORE_OBJECTS := ardb.o ardb_data.o hash.o kv.o lists.o logger.o sets.o sorted_sets.o strings.o transactions.o \
                $(UTIL_OBJECTS)
                
LEVELDB_OBJECTS := ../test/leveldb.o engine/leveldb_engine.o
KCDB_OBJECTS := ../test/kcdb.o engine/kyotocabinet_engine.o

SERVER_OBJECTS := ardb_server.o main.o

#DIST_LIB = libardb.so
DIST_LIBA = libardb.a

all: lib leveldb kcdb server

$(DIST_LIB):$(CORE_OBJECTS)
	${CXX} -shared -o $@ $^

$(DIST_LIBA):$(CORE_OBJECTS)
	ar rcs $@ $^

lib:$(DIST_LIBA)

server:$(SERVER_OBJECTS) $(CORE_OBJECTS) $(CHANNEL_OBJECTS)
	${CXX} -o ardb-server $(SERVER_OBJECTS) $(CORE_OBJECTS) $(CHANNEL_OBJECTS) $(LIBS) $(LEVELDB_PATH)/libleveldb.a 
	
leveldb:$(LEVELDB_OBJECTS) $(CORE_OBJECTS) 
	${CXX} -o leveldb $(LEVELDB_OBJECTS) $(CORE_OBJECTS) $(LIBS) $(LEVELDB_PATH)/libleveldb.a
	
kcdb:$(KCDB_OBJECTS) $(CORE_OBJECTS) 
	${CXX} -o kcdb $(KCDB_OBJECTS) $(CORE_OBJECTS) $(LIBS) ${KCDB_PATH}/libkyotocabinet.a -lz

clean:
	rm -f ${CORE_OBJECTS} ${LEVELDB_OBJECTS} $(SERVER_OBJECTS) $(CHANNEL_OBJECTS) $(DIST_LIBA) $(DIST_LIB)  leveldb
