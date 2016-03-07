#-------------------------------------------------------------------------------
# Copyright (c) 2013-2016, yinqiwen <yinqiwen@gmail.com>
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

ARDB_VERSION=0.8.6

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

LIB_PATH=$(PWD)/../deps

LMDB_PATH=${LIB_PATH}/mdb-mdb/libraries/liblmdb
LMDB_FILE=${LIB_PATH}/LMDB_0.9.14.tar.gz
LMDB_LIBA=${LMDB_PATH}/liblmdb.a

JEMALLOC_VER=jemalloc-3.6.0
JEMALLOC_PATH=${LIB_PATH}/${JEMALLOC_VER}
JEMALLOC_FILE=${JEMALLOC_PATH}.tar.bz2
JEMALLOC_LIBA=${JEMALLOC_PATH}/lib/libjemalloc.a

SNAPPY_VER=snappy-1.1.2
SNAPPY_PATH=${LIB_PATH}/${SNAPPY_VER}
SNAPPY_FILE=${SNAPPY_PATH}.tar.gz
SNAPPY_LIBA=${SNAPPY_PATH}/.libs/libsnappy.a

ROCKSDB_VER=rocksdb-4.2
ROCKSDB_PATH=${LIB_PATH}/${ROCKSDB_VER}
ROCKSDB_FILE=${ROCKSDB_PATH}.tar.gz
ROCKSDB_LIBA=${ROCKSDB_PATH}/librocksdb.a

ZOOKEEPER_VER=zookeeper-3.4.5
ZOOKEEPER_PATH=${LIB_PATH}/${ZOOKEEPER_VER}
ZOOKEEPER_FILE=${ZOOKEEPER_PATH}.tar.gz
ZOOKEEPER_LIBA=${ZOOKEEPER_PATH}/.libs/libzkst.a
ZOOKEEPER_LIBHTA=${ZOOKEEPER_PATH}/.libs/libhashtable.a

SPARSEHASH_VER=sparsehash-2.0.2
SPARSEHASH_PATH=${LIB_PATH}/${SPARSEHASH_VER}
SPARSEHASH_FILE=${SPARSEHASH_PATH}.tar.gz
SPARSEHASH_CONFIG=${SPARSEHASH_PATH}/src/config.h

# Cross compiling variables
XC_HOST=
XC_TGT=
XC_BUILD=

LUA_VER=lua
LUA_PATH=${LIB_PATH}/${LUA_VER}
#LUA_FILE=${LUA_PATH}.tar.gz
LUA_LIBA=${LUA_PATH}/src/liblua.a

OPTIMIZATION?=-O2
OPT=$(OPTIMIZATION)

CXXFLAGS=-Wall -g ${OPT} -fPIC -D__STDC_FORMAT_MACROS -DARDB_VERSION='"${ARDB_VERSION}"' 
CCFLAGS=-Wall -std=gnu99 ${OPT} -fPIC -pedantic -g -D__STDC_FORMAT_MACROS -DARDB_VERSION='"${ARDB_VERSION}"'
LDFLAGS=-g 

SNAPPY_CXXFLAGS=-g $(OPT)
LEVELDB_OPT=-g $(OPT)

LUA_CFLAGS+= $(OPT) -Wall -DLUA_ANSI $(CCFLAGS)
LUA_LDFLAGS+= $(LDFLAGS)

INCS=-I./ -I./common -I${LIB_PATH}/cpp-btree -I${LUA_PATH}/src -I${SNAPPY_PATH} -I${SPARSEHASH_PATH}/src

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
DEP_LIBS+=jemalloc
endif

LIBS= ${LUA_LIBA} ${MALLOC_LIBA} ${SNAPPY_LIBA} -lpthread

%.o : %.cpp
	${CXX} -c ${CXXFLAGS} ${INCS} $< -o $@

%.o : %.c
	${CC} -c ${CCFLAGS} ${INCS} $< -o $@


COMMON_VPATH=common/channel common/channel/socket common/channel/fifo common/channel/codec common/channel/timer common/channel/signal \
             common/util common/util/exception  common/thread  common/redis common/buffer common/geo common/channel/redis
COMMON_CPPFILES := $(foreach dir, $(COMMON_VPATH), $(wildcard $(dir)/*.cpp))
COMMON_CFILES := $(foreach dir, $(COMMON_VPATH), $(wildcard $(dir)/*.c))
COMMON_OBJECTS := $(patsubst %.cpp, %.o, $(COMMON_CPPFILES)) $(patsubst %.c, %.o, $(COMMON_CFILES))

COMMAND_VPATH=command
COMMAND_CPPFILES := $(foreach dir, $(COMMAND_VPATH), $(wildcard $(dir)/*.cpp))
COMMAND_OBJECTS := $(patsubst %.cpp, %.o, $(COMMAND_CPPFILES))

DB_VPATH=db
DB_CPPFILES := $(foreach dir, $(DB_VPATH), $(wildcard $(dir)/*.cpp))
DB_OBJECTS := $(patsubst %.cpp, %.o, $(DB_CPPFILES))

CORE_OBJECTS := main.o config.o cron.o logger.o server.o statistics.o\
                $(COMMON_OBJECTS)  $(COMMAND_OBJECTS) $(DB_OBJECTS) 
  
ROCKSDB_ENGINE :=  engine/rocksdb_engine.o          
TESTOBJ := ../test/test_suite.o
SERVEROBJ := main.o

#DIST_LIB = libardb.so
DIST_LIBA = libardb.a

storage_engine?=rocksdb

ifeq ($(storage_engine), leveldb)
  STORAGE_ENGINE_OBJ=${LEVELDB_ENGINE}
  STORAGE_ENGINE=$(LEVELDB_LIBA)
  STORAGE_ENGINE_PATH=$(LEVELDB_PATH)
  INCS+=-I${LEVELDB_PATH}/include
  LIBS:=${LEVELDB_LIBA} ${SNAPPY_LIBA} ${LIBS}
else
ifeq ($(storage_engine), lmdb)
  STORAGE_ENGINE_OBJ=${LMDB_ENGINE}
  STORAGE_ENGINE=$(LMDB_LIBA)
  STORAGE_ENGINE_PATH=$(LMDB_PATH)
  INCS+=-I${LMDB_PATH}
  LIBS:= ${LMDB_LIBA} ${SNAPPY_LIBA} ${LIBS}
  CXXFLAGS+=-D__USE_LMDB__
else
ifeq ($(storage_engine), rocksdb)
  STORAGE_ENGINE_OBJ=${ROCKSDB_ENGINE}
  STORAGE_ENGINE=$(ROCKSDB_LIBA)
  STORAGE_ENGINE_PATH=$(ROCKSDB_PATH)
  INCS+=-I${ROCKSDB_PATH}/include
  LIBS:= ${ROCKSDB_LIBA} ${SNAPPY_LIBA} ${LIBS} -lz -lbz2 -lrt
  CXXFLAGS+=-D__USE_ROCKSDB__ -std=c++11
else
ifeq ($(storage_engine), wiredtiger)
  STORAGE_ENGINE_OBJ=${WIREDTIGER_ENGINE}
  STORAGE_ENGINE=$(WIREDTIGER_LIBA)
  STORAGE_ENGINE_PATH=$(WIREDTIGER_PATH)
  INCS+=-I${WIREDTIGER_PATH}
  LIBS:= ${WIREDTIGER_LIBA} ${SNAPPY_LIBA} ${LIBS} -lrt -ldl
  CXXFLAGS+=-D__USE_WIREDTIGER__
else
  $(error Only leveldb/lmdb/rocksdb supported as env storage_engine value)
endif
endif 
endif
endif


LAST_ENGINE := $(shell sh -c 'cat .kv_engine 2>&1')
ifeq ($(storage_engine), $(LAST_ENGINE))
#do nothing
else
   $(shell sh -c 'echo $(storage_engine) > .kv_engine')
   PREBUILD = clean_launch_obj clean_test
endif
#

#$(CORE_OBJECTS) $(SERVEROBJ) $(TESTOBJ): | $(ZOOKEEPER_PATH) $(STORAGE_ENGINE_PATH)

all: ${PREBUILD}  test server 

$(STORAGE_ENGINE_OBJ): $(STORAGE_ENGINE)

$(DIST_LIB): $(CORE_OBJECTS)
	${CXX} -shared -o $@ $^

$(DIST_LIBA):$(CORE_OBJECTS)
	ar rcs $@ $^

lib: $(DEP_LIBS) lua snappy sparsehash $(storage_engine) $(DIST_LIBA)

server: ${STORAGE_ENGINE_OBJ} lib $(CORE_OBJECTS) ${SERVEROBJ}
	${CXX} -o ardb-server $(SERVEROBJ)  $(CORE_OBJECTS) ${STORAGE_ENGINE_OBJ} $(LIBS)

test: ${STORAGE_ENGINE_OBJ} lib ${TESTOBJ} $(CORE_OBJECTS)
	${CXX} -o ardb-test ${STORAGE_ENGINE_OBJ} ${TESTOBJ} $(CORE_OBJECTS) $(LIBS) 

.PHONY: jemalloc
jemalloc: $(JEMALLOC_LIBA)
$(JEMALLOC_LIBA):
	echo ">>>>> Building JEMALLOC" && \
	cd ${LIB_PATH} && \
	tar jxf ${JEMALLOC_FILE} && \
	cd ${JEMALLOC_PATH} && \
	./configure  && \
	$(MAKE) build_lib_static && \
	echo "<<<<< Done building JEMALLOC"

.PHONY: snappy
snappy: $(SNAPPY_LIBA)
$(SNAPPY_LIBA):
	echo ">>>>> Building SNAPPY" && \
	cd ${LIB_PATH} && \
	tar zxf ${SNAPPY_FILE} && \
	cd ${SNAPPY_PATH} && \
	./configure CXXFLAGS="$(SNAPPY_CXXFLAGS=)" ${XC_BUILD} ${XC_HOST} ${XC_TGT} > configure.out && \
	$(MAKE) libsnappy.la && \
	echo "<<<<< Done building SNAPPY"

.PHONY: lmdb
lmdb: $(LMDB_LIBA)
$(LMDB_LIBA): $(LMDB_PATH)
	echo ">>>>> Building LMDB" && \
	cd ${LMDB_PATH} && \
	$(MAKE) && \
	echo "<<<<< Done building LMDB"

$(LMDB_PATH):
	echo ">>>>> Unpacking LMDB" && \
	cd ${LIB_PATH} && \
	tar zxf ${LMDB_FILE} && \
	echo "<<<<< Done unpacking LMDB"

.PHONY: leveldb
leveldb: $(LEVELDB_LIBA)
$(LEVELDB_LIBA): $(SNAPPY_LIBA) $(LEVELDB_PATH)
	echo ">>>>> Building LEVELDB" && \
	cd ${LEVELDB_PATH} && \
	OPT="$(LEVELDB_OPT)" \
	CXXFLAGS="-I${SNAPPY_PATH} -fno-access-control" \
		CFLAGS="-I${SNAPPY_PATH}" \
		LDFLAGS="${SNAPPY_PATH}/.libs" \
		$(MAKE) libleveldb.a && \
	echo "<<<<< Done building LEVELDB"

$(LEVELDB_PATH):
	echo ">>>>> Unpacking LEVELDB" && \
	cd ${LIB_PATH} && \
	tar zxf ${LEVELDB_FILE} && \
	echo "<<<<< Done unpacking LEVELDB"

.PHONY: rocksdb
rocksdb: $(ROCKSDB_LIBA)
$(ROCKSDB_LIBA): $(SNAPPY_LIBA) $(ROCKSDB_PATH)
	echo ">>>>> Building ROCKSDB" && \
	cd ${ROCKSDB_PATH} && \
	CXXFLAGS="-I${SNAPPY_PATH} -O2 -DNDEBUG" CFLAGS="-I${SNAPPY_PATH}" LDFLAGS="${SNAPPY_PATH}/.libs" $(MAKE) static_lib && \
	echo "<<<<< Done building ROCKSDB"

$(ROCKSDB_PATH):
	echo ">>>>> Unpacking ROCKSDB" && \
	cd ${LIB_PATH} && \
	tar zxf ${ROCKSDB_FILE} && \
	echo "<<<<< Done unpacking LEVELDB"

.PHONY: lua
lua: $(LUA_LIBA)
$(LUA_LIBA):
	echo ">>>>> Building LUA" && \
	cd ${LIB_PATH} && \
	cd ${LUA_PATH}/src && \
	$(MAKE) all CFLAGS="$(LUA_CFLAGS)" MYLDFLAGS="$(LUA_LDFLAGS)" && \
	echo ">>>>> Done building LUA"

.PHONY: zookeeper
zookeeper: $(ZOOKEEPER_LIBA)
$(ZOOKEEPER_LIBA): $(ZOOKEEPER_PATH)
	echo ">>>>> Building ZOOKEEPER" && \
	cd $(ZOOKEEPER_PATH) && \
	./configure ${XC_BUILD} ${XC_HOST} ${XC_TGT} > configure.out && \
	$(MAKE)

$(ZOOKEEPER_PATH):
	echo ">>>>> Unpacking ZOOKEEPER" && \
	cd ${LIB_PATH} && \
	tar zxf ${ZOOKEEPER_FILE} && \
	cd ${ZOOKEEPER_PATH}/src && \
	patch < ../patch/zookeeper.mt_adaptor.patch && \
	cd ${ZOOKEEPER_PATH} && \
	patch < ../patch/zookeeper.configure.ac.patch && \
	autoconf configure.ac > configure 2> autoconf.out && \
	echo "<<<<<< Done unpacking ZOOKEEPER"

.PHONY: sparsehash
sparsehash: $(SPARSEHASH_CONFIG)
$(SPARSEHASH_CONFIG):
	echo ">>>>> Building sparsehash" && \
	cd ${LIB_PATH}; tar zxvf ${SPARSEHASH_FILE};\
	cd ${SPARSEHASH_PATH}; ./configure; \
	${MAKE} src/sparsehash/internal/sparseconfig.h && \
	echo ">>>>> Done building sparsehash"

clean_launch_obj:
	rm -f main.o

clean_test:
	rm -f ${TESTOBJ};rm -f ardb-test

clean_deps:
	rm -rf $(LMDB_PATH) $(JEMALLOC_PATH) $(SNAPPY_PATH) $(LEVELDB_PATH) \
		$(ROCKSDB_PATH) $(ZOOKEEPER_PATH)
	$(MAKE) -C $(LUA_PATH) clean

noopt:
	$(MAKE) OPT="-O0"

valgrind:
	$(MAKE) OPT="-O0" MALLOC="libc"

dist:clean all
	rm -rf ardb-${ARDB_VERSION};mkdir -p ardb-${ARDB_VERSION}/bin ardb-${ARDB_VERSION}/conf ardb-${ARDB_VERSION}/logs ardb-${ARDB_VERSION}/data ardb-${ARDB_VERSION}/repl ardb-${ARDB_VERSION}/backup; \
	cp ardb-server ardb-${ARDB_VERSION}/bin; cp ardb-test ardb-${ARDB_VERSION}/bin; cp ../ardb.conf ardb-${ARDB_VERSION}/conf; \
	tar czvf ardb-bin-${ARDB_VERSION}.tar.gz ardb-${ARDB_VERSION}; rm -rf ardb-${ARDB_VERSION};

clean:
	rm -f  ${CORE_OBJECTS} $(STORAGE_ENGINE_OBJ) $(SERVEROBJ) ${TESTOBJ}\
	       ardb-test  ardb-server

clobber: clean_deps clean
