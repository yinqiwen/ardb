#-------------------------------------------------------------------------------
# Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
# All rights reserved.
#  
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#  
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#   * Neither the name of Redis nor the names of its contributors may be used
#     to endorse or promote products derived from this software without
#     specific prior written permission.
#  
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
# THE POSSIBILITY OF SUCH DAMAGE.
#-------------------------------------------------------------------------------
#Makefile
#
# Created on: 2013-3-28
#     Author: yinqiwen

CXX=g++
CC=gcc

ARDB_VERSION=0.6.0

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

LIB_PATH=$(PWD)/../deps

LMDB_PATH=${LIB_PATH}/mdb-mdb/libraries/liblmdb
LMDB_FILE=${LIB_PATH}/LMDB_0_9_6.gz
LMDB_LIBA=${LMDB_PATH}/liblmdb.a

JEMALLOC_VER=jemalloc-3.4.1
JEMALLOC_PATH=${LIB_PATH}/${JEMALLOC_VER}
JEMALLOC_FILE=${JEMALLOC_PATH}.tar.bz2
JEMALLOC_LIBA=${JEMALLOC_PATH}/lib/libjemalloc.a

SNAPPY_VER=snappy-1.1.1
SNAPPY_PATH=${LIB_PATH}/${SNAPPY_VER}
SNAPPY_FILE=${SNAPPY_PATH}.tar.gz
SNAPPY_LIBA=${SNAPPY_PATH}/.libs/libsnappy.a

LEVELDB_VER=leveldb-1.16.0
LEVELDB_PATH=${LIB_PATH}/${LEVELDB_VER}
LEVELDB_FILE=${LEVELDB_PATH}.tar.gz
LEVELDB_LIBA=${LEVELDB_PATH}/libleveldb.a

ROCKSDB_VER=rocksdb-2.6.fb
ROCKSDB_PATH=${LIB_PATH}/${ROCKSDB_VER}
ROCKSDB_FILE=${ROCKSDB_PATH}.tar.gz
ROCKSDB_LIBA=${ROCKSDB_PATH}/librocksdb.a

ZOOKEEPER_VER=zookeeper-3.4.5
ZOOKEEPER_PATH=${LIB_PATH}/${ZOOKEEPER_VER}
ZOOKEEPER_FILE=${ZOOKEEPER_PATH}.tar.gz
ZOOKEEPER_LIBA=${ZOOKEEPER_PATH}/.libs/libzkst.a
ZOOKEEPER_LIBHTA=${ZOOKEEPER_PATH}/.libs/libhashtable.a

KCDB_VER=kyotocabinet-1.2.76
KCDB_PATH=${LIB_PATH}/${KCDB_VER}
KCDB_FILE=${KCDB_PATH}.tar.gz
KCDB_LIBA=${KCDB_PATH}/libkyotocabinet.a

LUA_VER=lua
LUA_PATH=${LIB_PATH}/${LUA_VER}
#LUA_FILE=${LUA_PATH}.tar.gz
LUA_LIBA=${LUA_PATH}/src/liblua.a

OPTIMIZATION?=-O2
OPT=$(OPTIMIZATION)

CXXFLAGS=-Wall -g  ${OPT}  -fPIC -D__STDC_FORMAT_MACROS -DARDB_VERSION='"${ARDB_VERSION}"' 
CCFLAGS=-Wall -std=gnu99 ${OPT} -pedantic -g -fPIC -D__STDC_FORMAT_MACROS -DARDB_VERSION='"${ARDB_VERSION}"'
LDFLAGS=-g 

LUA_CFLAGS+= -O2 -Wall -DLUA_ANSI $(CCFLAGS)
LUA_LDFLAGS+= $(LDFLAGS)

INCS=-I./ -I${LIB_PATH}/cpp-btree -I${LUA_PATH}/src -I${SNAPPY_PATH} -I${ZOOKEEPER_PATH}/include -I${ZOOKEEPER_PATH}/generated

# Default allocator
ifeq ($(uname_S),Linux)
	MALLOC=jemalloc
else
	MALLOC=libc
endif

#DEP_LIBS=sparsehash

ifeq ($(MALLOC),libc)
#do nothing
else
MALLOC_LIBA=${JEMALLOC_LIBA}
DEP_LIBS+=jemalloc
endif

LIBS= ${LUA_LIBA} ${ZOOKEEPER_LIBA} ${ZOOKEEPER_LIBHTA} ${MALLOC_LIBA} -lpthread

%.o : %.cpp
	${CXX} -c ${CXXFLAGS} ${INCS} $< -o $@

%.o : %.c
	${CC} -c ${CCFLAGS} ${INCS} $< -o $@


CHANNEL_CPP_VPATH=channel channel/socket channel/fifo channel/codec channel/timer channel/signal channel/zookeeper
CHANNEL_C_VPATH=channel/redis 
CHANNEL_CPPFILES := $(foreach dir, $(CHANNEL_CPP_VPATH), $(wildcard $(dir)/*.cpp))
CHANNEL_CFILES := $(foreach dir, $(CHANNEL_C_VPATH), $(wildcard $(dir)/*.c))
CHANNEL_OBJECTS := $(patsubst %.cpp, %.o, $(CHANNEL_CPPFILES)) $(patsubst %.c, %.o, $(CHANNEL_CFILES))

UTIL_VPATH=./util ./util/exception  ./util/thread  ./redis ./helper ./buffer ./cache ./geo
UTIL_CPPFILES := $(foreach dir, $(UTIL_VPATH), $(wildcard $(dir)/*.cpp))
UTIL_CFILES := $(foreach dir, $(UTIL_VPATH), $(wildcard $(dir)/*.c))
UTIL_OBJECTS := $(patsubst %.cpp, %.o, $(UTIL_CPPFILES)) $(patsubst %.c, %.o, $(UTIL_CFILES))
CORE_OBJECTS := ardb.o data_format.o hash.o kv.o lists.o logger.o sets.o \
                zsets.o strings.o bits.o sort.o geo.o\
                $(UTIL_OBJECTS)

LEVELDB_ENGINE :=  engine/leveldb_engine.o    
ROCKSDB_ENGINE :=  engine/rocksdb_engine.o     
KCDB_ENGINE :=  engine/kyotocabinet_engine.o     
LMDB_ENGINE :=  engine/lmdb_engine.o     
TESTOBJ := ../test/ardb_test.o

SERVER_OBJECTS := ardb_server.o lua_scripting.o transaction.o slowlog.o clients.o \
                  replication/rdb.o replication/slave.o replication/repl_backlog.o \
                  replication/master.o ha/agent.o pubsub.o  main.o

#DIST_LIB = libardb.so
DIST_LIBA = libardb.a

storage_engine?=leveldb

ifeq ($(storage_engine), leveldb)
  STORAGE_ENGINE_OBJ=${LEVELDB_ENGINE}
  STORAGE_ENGINE=leveldb ${STORAGE_ENGINE_OBJ}
  INCS+=-I${LEVELDB_PATH}/include
  LIBS:=${LEVELDB_LIBA} ${SNAPPY_LIBA} ${LIBS}
else
ifeq ($(storage_engine), kyotocabinet)
  STORAGE_ENGINE_OBJ=${KCDB_ENGINE}
  STORAGE_ENGINE=kyotocabinet ${STORAGE_ENGINE_OBJ}
  INCS+=-I${KCDB_PATH}
  
  LIBS:= ${KCDB_LIBA} ${LIBS} -lz
  CXXFLAGS+=-D__USE_KYOTOCABINET__
else
ifeq ($(storage_engine), lmdb)
  STORAGE_ENGINE_OBJ=${LMDB_ENGINE}
  STORAGE_ENGINE=lmdb ${STORAGE_ENGINE_OBJ}
  INCS+=-I${LMDB_PATH}
  LIBS:= ${LMDB_LIBA} ${LIBS}
  CXXFLAGS+=-D__USE_LMDB__
else
ifeq ($(storage_engine), rocksdb)
  STORAGE_ENGINE_OBJ=${ROCKSDB_ENGINE}
  STORAGE_ENGINE=rocksdb ${STORAGE_ENGINE_OBJ}
  INCS+=-I${ROCKSDB_PATH}/include
  LIBS:= ${ROCKSDB_LIBA} ${SNAPPY_LIBA} ${LIBS}
  CXXFLAGS+=-D__USE_ROCKSDB__ -std=c++11
else
  $(error Only leveldb/kyotocabinet/lmdb/rocksdb supported as env storage_engine's value)
endif
endif 
endif
endif


all: lib test server 

$(DIST_LIB):$(CORE_OBJECTS)
	${CXX} -shared -o $@ $^

$(DIST_LIBA):$(CORE_OBJECTS)
	ar rcs $@ $^

lib:$(DEP_LIBS) $(DIST_LIBA) lua zookeeper

server:${STORAGE_ENGINE} lib clean_launch_obj $(SERVER_OBJECTS) $(CHANNEL_OBJECTS) 
	${CXX} -o ardb-server $(SERVER_OBJECTS)  $(CORE_OBJECTS) $(CHANNEL_OBJECTS) ${STORAGE_ENGINE_OBJ} $(LIBS)

test:clean_test ${STORAGE_ENGINE} lib $(CORE_OBJECTS) ${TESTOBJ} $(CHANNEL_OBJECTS)
	${CXX} -o ardb-test ${STORAGE_ENGINE_OBJ} ${TESTOBJ} $(CORE_OBJECTS) $(CHANNEL_OBJECTS) $(LIBS) 
    
jemalloc:
	@if test -f ${JEMALLOC_LIBA}; then\
      :; \
    else \
      cd ${LIB_PATH};tar jxvf ${JEMALLOC_FILE};cd ${JEMALLOC_PATH};\
      ./configure;make build_lib_static;\
    fi

    
snappy:
	@if test -f ${SNAPPY_LIBA}; then\
      :; \
    else \
      cd ${LIB_PATH};tar zxvf ${SNAPPY_FILE};cd ${SNAPPY_PATH};\
      ./configure;make libsnappy.la;\
    fi

kyotocabinet:
	@if test -f ${KCDB_LIBA}; then\
      :; \
    else \
      cd ${LIB_PATH};tar zxvf ${KCDB_FILE};cd ${KCDB_PATH};\
      ./configure;make;\
    fi

lmdb:
	@if test -f ${LMDB_LIBA}; then\
      :; \
    else \
      cd ${LIB_PATH};tar zxvf ${LMDB_FILE};cd ${LMDB_PATH};\
      make;\
    fi

leveldb:snappy
	@if test -f ${LEVELDB_LIBA}; then\
      :; \
    else \
      cd ${LIB_PATH};tar zxvf ${LEVELDB_FILE};cd ${LEVELDB_PATH};\
      CXXFLAGS="-I${SNAPPY_PATH} -fno-access-control" CFLAGS="-I${SNAPPY_PATH}" LDFLAGS="${SNAPPY_PATH}/.libs" make libleveldb.a;\
    fi
    
rocksdb:snappy
	@if test -f ${ROCKSDB_LIBA}; then\
      :; \
    else \
      cd ${LIB_PATH};tar zxvf ${ROCKSDB_FILE};cd ${ROCKSDB_PATH};\
      CXXFLAGS="-I${SNAPPY_PATH}" CFLAGS="-I${SNAPPY_PATH}" LDFLAGS="${SNAPPY_PATH}/.libs" make librocksdb.a;\
    fi

lua:
	@if test -f ${LUA_LIBA}; then\
      :; \
    else \
      cd ${LIB_PATH};cd ${LUA_PATH}/src; make all CFLAGS="$(LUA_CFLAGS)" MYLDFLAGS="$(LUA_LDFLAGS)";\
    fi

zookeeper:
	@if test -f ${ZOOKEEPER_LIBA}; then\
      :; \
    else \
      cd ${LIB_PATH};tar zxvf ${ZOOKEEPER_FILE};cd ${ZOOKEEPER_PATH}; ./configure;make;\
    fi

clean_launch_obj:
	rm -f main.o

clean_test:
	rm -f ${TESTOBJ};rm -f ardb-test
	
noopt:
	$(MAKE) OPT="-O0"
	
valgrind:
	$(MAKE) OPT="-O0" MALLOC="libc"
	
dist:clean all
	rm -rf ardb-${ARDB_VERSION};mkdir -p ardb-${ARDB_VERSION}/bin ardb-${ARDB_VERSION}/conf ardb-${ARDB_VERSION}/logs ardb-${ARDB_VERSION}/data ardb-${ARDB_VERSION}/repl ardb-${ARDB_VERSION}/backup; \
	cp ardb-server ardb-${ARDB_VERSION}/bin; cp ardb-test ardb-${ARDB_VERSION}/bin; cp ../ardb.conf ardb-${ARDB_VERSION}/conf; \
	tar czvf ardb-bin-${ARDB_VERSION}.tar.gz ardb-${ARDB_VERSION}; rm -rf ardb-${ARDB_VERSION};

clean:
	rm -f $(LEVELDB_TEST) $(KCDB_TEST) ${CORE_OBJECTS} ${LEVELDB_OBJECTS} \
	      $(SERVER_OBJECTS) $(CHANNEL_OBJECTS) $(DIST_LIBA) $(DIST_LIB)   \
	      $(LEVELDB_ENGINE) $(KCDB_ENGINE) $(LMDB_ENGINE) $(TESTOBJ)\
	      ardb-test  ardb-server
