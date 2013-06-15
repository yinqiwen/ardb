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

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

LIB_PATH=$(PWD)/../deps

LMDB_PATH=${LIB_PATH}/mdb-mdb/libraries/liblmdb
LMDB_FILE=${LIB_PATH}/LMDB_0_9_6.gz
LMDB_LIBA=${LMDB_PATH}/liblmdb.a

JEMALLOC_VER=jemalloc-3.3.1
JEMALLOC_PATH=${LIB_PATH}/${JEMALLOC_VER}
JEMALLOC_FILE=${JEMALLOC_PATH}.tar.bz2
JEMALLOC_LIBA=${JEMALLOC_PATH}/lib/libjemalloc.a

GPERF_VER=gperftools-2.0
GPERF_PATH=${LIB_PATH}/${GPERF_VER}
GPERF_FILE=${GPERF_PATH}.tar.gz
TCMALLOC_LIBA=${GPERF_PATH}/.libs/libtcmalloc_minimal.a

SNAPPY_VER=snappy-1.1.0
SNAPPY_PATH=${LIB_PATH}/${SNAPPY_VER}
SNAPPY_FILE=${SNAPPY_PATH}.tar.gz
SNAPPY_LIBA=${SNAPPY_PATH}/.libs/libsnappy.a

LEVELDB_VER=leveldb-1.11.0
LEVELDB_PATH=${LIB_PATH}/${LEVELDB_VER}
LEVELDB_FILE=${LEVELDB_PATH}.tar.gz
LEVELDB_LIBA=${LEVELDB_PATH}/libleveldb.a

KCDB_VER=kyotocabinet-1.2.76
KCDB_PATH=${LIB_PATH}/${KCDB_VER}
KCDB_FILE=${KCDB_PATH}.tar.gz
KCDB_LIBA=${KCDB_PATH}/libkyotocabinet.a

OPTIMIZATION?=-O2
OPT=$(OPTIMIZATION)

CXXFLAGS=-Wall -g  ${OPT}  -fPIC -D__STDC_FORMAT_MACROS
CCFLAGS=-Wall -std=gnu99 ${OPT} -pedantic -g -fPIC -D__STDC_FORMAT_MACROS
LDFLAGS=-g 

INCS=-I./  -I${LIB_PATH}/cpp-btree 

# Default allocator
ifeq ($(uname_S),Linux)
	MALLOC=jemalloc
else
	MALLOC=libc
endif

ifeq ($(MALLOC),libc)
#do nothing
else
MALLOC_LIBA=${JEMALLOC_LIBA}
DEP_LIBS=jemalloc
endif

LIBS=${MALLOC_LIBA} -lpthread

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
                zsets.o strings.o bits.o table.o sort.o\
                $(UTIL_OBJECTS)

LEVELDB_ENGINE :=  engine/leveldb_engine.o       
KCDB_ENGINE :=  engine/kyotocabinet_engine.o     
LMDB_ENGINE :=  engine/lmdb_engine.o     
TESTOBJ := ../test/ardb_test.o

SERVER_OBJECTS := ardb_server.o transaction.o slowlog.o clients.o replication.o pubsub.o oplogs.o main.o

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
  $(error Only leveldb/kyotocabinet/lmdb supported as env storage_engine's value)
endif 
endif
endif


all: lib test server 

$(DIST_LIB):$(CORE_OBJECTS)
	${CXX} -shared -o $@ $^

$(DIST_LIBA):$(CORE_OBJECTS)
	ar rcs $@ $^

lib:$(DEP_LIBS) $(DIST_LIBA)

server:${STORAGE_ENGINE} lib clean_launch_obj $(SERVER_OBJECTS) $(CHANNEL_OBJECTS) 
	${CXX} -o ardb-server $(SERVER_OBJECTS)  $(CORE_OBJECTS) $(CHANNEL_OBJECTS) ${STORAGE_ENGINE_OBJ} $(LIBS)

test:${STORAGE_ENGINE} lib $(CORE_OBJECTS) ${TESTOBJ}
	${CXX} -o ardb-test ${STORAGE_ENGINE_OBJ} ${TESTOBJ} $(CORE_OBJECTS) $(LIBS) 
	
tcmalloc:
	@if test -f ${TCMALLOC_LIBA}; then\
      :; \
    else \
      cd ${LIB_PATH};tar zxvf ${GPERF_FILE};cd ${GPERF_PATH};\
      ./configure;make libtcmalloc_minimal.la;\
    fi
    
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
      CXXFLAGS="-I${SNAPPY_PATH}" CFLAGS="-I${SNAPPY_PATH}" LDFLAGS="${SNAPPY_PATH}/.libs" make libleveldb.a;\
    fi

clean_launch_obj:
	rm -f main.o

clean_test:
	rm -f ${TESTOBJ};rm -f ardb-test
	
noopt:
	$(MAKE) OPT="-O0"
valgrind:
	$(MAKE) OPT="-O0" MALLOC="libc"

clean:
	rm -f $(LEVELDB_TEST) $(KCDB_TEST) ${CORE_OBJECTS} ${LEVELDB_OBJECTS} \
	      $(SERVER_OBJECTS) $(CHANNEL_OBJECTS) $(DIST_LIBA) $(DIST_LIB)   \
	      $(LEVELDB_ENGINE) $(KCDB_ENGINE) $(LMDB_ENGINE) $(TESTOBJ)\
	      ardb-test  ardb-server
