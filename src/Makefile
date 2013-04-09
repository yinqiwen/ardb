#Makefile
#
# Created on: 2013-3-28
#     Author: wqy

CXX=g++
CC=gcc

SNAPPY_PATH=../deps/snappy
LEVELDB_PATH=../deps/leveldb
GLOG_PATH=../deps/google-glog

CXXFLAGS=-Wall -Wno-format -g -O2 -D_FILE_OFFSET_BITS=64 -fPIC
CCFLAGS=-Wall -std=gnu99 -pedantic -Wno-format -g -O2 -D_FILE_OFFSET_BITS=64 -fPIC
LDFLAGS=-g 

INCS=-I./ -I./util -I${LEVELDB_PATH}/include -I${GLOG_PATH}/src

LIBS=$(LEVELDB_PATH)/libleveldb.a ${GLOG_PATH}/.libs/libglog.a

%.o : %.cpp
	${CXX} -c ${CXXFLAGS} ${INCS} $< -o $@

%.o : %.c
	${CC} -c ${CCFLAGS} ${INCS} $< -o $@

CPP_VPATH=. ./util ./engine
       
C_VPATH=.

CHANNEL_CPP_VPATH=channel channel/socket channel/codec channel/timer channel/signal
CHANNEL_C_VPATH=channel/redis
       
C_VPATH=channel/redis mem/mm mem concurrent database \
        util util/container misc/http misc/crypto misc/sds misc/json

CPPFILES := $(foreach dir, $(CPP_VPATH), $(wildcard $(dir)/*.cpp))
CFILES := $(foreach dir, $(C_VPATH), $(wildcard $(dir)/*.c))
CPP_OBJECTS := $(patsubst %.cpp, %.o, $(CPPFILES))
C_OBJECTS := $(patsubst %.c, %.o, $(CFILES))

LEVELDB_OBJECTS := ../test/leveldb.o
CHANNEL_CPPFILES := $(foreach dir, $(CHANNEL_CPP_VPATH), $(wildcard $(dir)/*.cpp))
CHANNEL_CFILES := $(foreach dir, $(CHANNEL_C_VPATH), $(wildcard $(dir)/*.c))
CHANNEL_CPP_OBJECTS := $(patsubst %.cpp, %.o, $(CHANNEL_CPPFILES))
CHANNEL_C_OBJECTS := $(patsubst %.c, %.o, $(CHANNEL_CFILES))

#DIST_LIB = libardb.so
DIST_LIBA = libardb.a

all: lib leveldb

$(DIST_LIB):$(CPP_OBJECTS) ${C_OBJECTS}
	${CXX} -shared -o $@ $^

$(DIST_LIBA):$(CPP_OBJECTS) ${C_OBJECTS}
	ar rcs $@ $^

lib:$(DIST_LIBA)
	
	
leveldb:$(LEVELDB_OBJECTS) $(CPP_OBJECTS) ${C_OBJECTS} ${CHANNEL_C_OBJECTS} ${CHANNEL_CPP_OBJECTS} 
	${CXX} -o leveldb $(LEVELDB_OBJECTS) $(CPP_OBJECTS) ${C_OBJECTS} $(LIBS) 

clean:
	rm -f ${CPP_OBJECTS} ${C_OBJECTS} $(DIST_LIBA) $(DIST_LIB) ${CHANNEL_C_OBJECTS} ${CHANNEL_CPP_OBJECTS} leveldb
