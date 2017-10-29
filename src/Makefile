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

ARDB_VERSION=0.9.5

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

LIB_PATH=$(PWD)/../deps

JEMALLOC_VER=5.0.1
JEMALLOC_PATH=${LIB_PATH}/jemalloc-${JEMALLOC_VER}
JEMALLOC_FILE=${JEMALLOC_PATH}.tar.bz2
JEMALLOC_REMOTE_FILE=https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_VER}/jemalloc-${JEMALLOC_VER}.tar.bz2
JEMALLOC_LIBA=${JEMALLOC_PATH}/lib/libjemalloc.a

SNAPPY_VER=1.1.4
SNAPPY_PATH=${LIB_PATH}/snappy-${SNAPPY_VER}
SNAPPY_FILE=${SNAPPY_PATH}.tar.gz
SNAPPY_REMOTE_FILE=https://github.com/google/snappy/releases/download/${SNAPPY_VER}/snappy-${SNAPPY_VER}.tar.gz
SNAPPY_LIBA=${SNAPPY_PATH}/.libs/libsnappy.a

ROCKSDB_VER=5.2.1
ROCKSDB_PATH=${LIB_PATH}/rocksdb-${ROCKSDB_VER}
ROCKSDB_REMOTE_FILE=https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VER}.tar.gz
ROCKSDB_FILE=${ROCKSDB_PATH}.tar.gz
ROCKSDB_LIBA=${ROCKSDB_PATH}/librocksdb.a

LEVELDB_VER=1.19
LEVELDB_PATH=${LIB_PATH}/leveldb-${LEVELDB_VER}
LEVELDB_FILE=${LEVELDB_PATH}.tar.gz
LEVELDB_REMOTE_FILE=https://github.com/google/leveldb/archive/v${LEVELDB_VER}.tar.gz
LEVELDB_LIBA=${LEVELDB_PATH}/out-static/libleveldb.a

LMDB_VER=0.9.21
LMDB_PATH=${LIB_PATH}/lmdb-LMDB_${LMDB_VER}/libraries/liblmdb
LMDB_FILE=${LIB_PATH}/lmdb-${LMDB_VER}.tar.gz
LMDB_REMOTE_FILE=https://github.com/LMDB/lmdb/archive/LMDB_${LMDB_VER}.tar.gz
LMDB_LIBA=${LMDB_PATH}/liblmdb.a

FORESTDB_VER=forestdb-1.2
FORESTDB_PATH=${LIB_PATH}/${FORESTDB_VER}
FORESTDB_FILE=${LIB_PATH}/${FORESTDB_VER}.tar.gz
FORESTDB_REMOTE_FILE=https://github.com/couchbase/forestdb/archive/v1.2.tar.gz
FORESTDB_SO=${FORESTDB_PATH}/build/libforestdb.so
FORESTDB_LIBA=${FORESTDB_PATH}/build/libforestdb.a

WIREDTIGER_VER=2.9.1
WIREDTIGER_PATH=${LIB_PATH}/wiredtiger-${WIREDTIGER_VER}
WIREDTIGER_FILE=${WIREDTIGER_PATH}.tar.bz2
WIREDTIGER_REMOTE_FILE=https://github.com/wiredtiger/wiredtiger/releases/download/${WIREDTIGER_VER}/wiredtiger-${WIREDTIGER_VER}.tar.bz2
WIREDTIGER_LIBA=${WIREDTIGER_PATH}/.libs/libwiredtiger.a

PERCONAFT_VER=PerconaFT-master
PERCONAFT_PATH=${LIB_PATH}/${PERCONAFT_VER}
PERCONAFT_FILE=${LIB_PATH}/${PERCONAFT_VER}.zip
PERCONAFT_REMOTE_FILE=https://github.com/percona/PerconaFT/archive/master.zip
PERCONAFT_LIBA=${PERCONAFT_PATH}/prefix/lib/libtokufractaltree_static.a
PERCONAFT_LIBA2=${PERCONAFT_PATH}/prefix/lib/libtokuportability_static.a

ZOOKEEPER_VER=zookeeper-3.4.5
ZOOKEEPER_PATH=${LIB_PATH}/${ZOOKEEPER_VER}
ZOOKEEPER_FILE=${ZOOKEEPER_PATH}.tar.gz
ZOOKEEPER_LIBA=${ZOOKEEPER_PATH}/.libs/libzkst.a
ZOOKEEPER_LIBHTA=${ZOOKEEPER_PATH}/.libs/libhashtable.a

SPARSEHASH_VER=sparsehash-sparsehash-2.0.3
SPARSEHASH_PATH=${LIB_PATH}/${SPARSEHASH_VER}
SPARSEHASH_FILE=${SPARSEHASH_PATH}.tar.gz
SPARSEHASH_CONFIG=${SPARSEHASH_PATH}/src/config.h

# Cross compiling variables
XC_HOST=
XC_TGT=
XC_BUILD=

LUA_VER=lua
LUA_PATH=${LIB_PATH}/${LUA_VER}
LUA_LIBA=${LUA_PATH}/src/liblua.a

CJSON_VER=cJSON
CJSON_PATH=${LIB_PATH}/${CJSON_VER}
CJSON_LIBA=${CJSON_PATH}/libcjson.a

OPTIMIZATION?=-O2
OPT=$(OPTIMIZATION)

CXXFLAGS=-Wall -g ${OPT} -fPIC -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -DARDB_VERSION='"${ARDB_VERSION}"' 
CCFLAGS=-Wall -std=gnu99 ${OPT} -fPIC -pedantic -g -D__STDC_FORMAT_MACROS -DARDB_VERSION='"${ARDB_VERSION}"'
LDFLAGS=-g 

SNAPPY_CXXFLAGS=-g $(OPT)
LEVELDB_OPT=-g $(OPT)

LUA_CFLAGS+= $(OPT) -Wall -DLUA_ANSI $(CCFLAGS)
LUA_LDFLAGS+= $(LDFLAGS)

INCS=-I./ -I./common -I${LIB_PATH}/cpp-btree -I${LUA_PATH}/src -I${SNAPPY_PATH} -I${SPARSEHASH_PATH}/src


ifeq ($(MALLOC),libc)
#do nothing
else
MALLOC_LIBA=${JEMALLOC_LIBA}
DEP_LIBS+=jemalloc
endif

storage_engine?=rocksdb

LIBS= ${LUA_LIBA} ${MALLOC_LIBA} ${SNAPPY_LIBA} -lpthread -ldl

# Default allocator
ifeq ($(uname_S),Linux)
	MALLOC=jemalloc
	CXXFLAGS+=-DCORO_ASM
	CCFLAGS+=-DCORO_ASM
	LIBS+=-lrt 
else
	MALLOC=libc
	CXXFLAGS+=-DCORO_UCONTEXT -D_XOPEN_SOURCE
	CCFLAGS+=-DCORO_UCONTEXT -D_XOPEN_SOURCE
endif

%.o : %.cpp
	${CXX} -c ${CXXFLAGS} ${INCS} $< -o $@

%.o : %.c
	${CC} -c ${CCFLAGS} ${INCS} $< -o $@


COMMON_VPATH=common/channel common/channel/socket common/channel/fifo common/channel/codec common/channel/timer common/channel/signal \
             common/util common/util/exception  common/thread  common/coro common/redis common/buffer common/geo common/channel/redis
COMMON_CPPFILES := $(foreach dir, $(COMMON_VPATH), $(wildcard $(dir)/*.cpp))
COMMON_CFILES := $(foreach dir, $(COMMON_VPATH), $(wildcard $(dir)/*.c))
COMMON_OBJECTS := $(patsubst %.cpp, %.o, $(COMMON_CPPFILES)) $(patsubst %.c, %.o, $(COMMON_CFILES))

COMMAND_VPATH=command
COMMAND_CPPFILES := $(foreach dir, $(COMMAND_VPATH), $(wildcard $(dir)/*.cpp))
COMMAND_OBJECTS := $(patsubst %.cpp, %.o, $(COMMAND_CPPFILES))

DB_VPATH=db repl
DB_CPPFILES := $(foreach dir, $(DB_VPATH), $(wildcard $(dir)/*.cpp))
DB_CFILES := $(foreach dir, $(DB_VPATH), $(wildcard $(dir)/*.c))
DB_OBJECTS := $(patsubst %.cpp, %.o, $(DB_CPPFILES)) $(patsubst %.c, %.o, $(DB_CFILES))

CORE_OBJECTS :=  config.o cron.o logger.o network.o types.o statistics.o\
                $(COMMON_OBJECTS)  $(COMMAND_OBJECTS) $(DB_OBJECTS) 
        
TESTOBJ := ../test/test_main.o
REPAIR_TOOL_OBJ := tools/repair.o
SERVEROBJ := main.o

STORAGE_ENGINE_VPATH=db/${storage_engine}
STORAGE_ENGINE_CPPFILES := $(foreach dir, $(STORAGE_ENGINE_VPATH), $(wildcard $(dir)/*.cpp))
STORAGE_ENGINE_CFILES := $(foreach dir, $(STORAGE_ENGINE_VPATH), $(wildcard $(dir)/*.c))
STORAGE_ENGINE_OBJ := $(patsubst %.cpp, %.o, $(STORAGE_ENGINE_CPPFILES)) $(patsubst %.c, %.o, $(STORAGE_ENGINE_CFILES))
STORAGE_ENGINE_ALL_OBJ := db/*/*.o   


#DIST_LIB = libardb.so
DIST_LIBA = libardb.a

ifeq ($(storage_engine), leveldb)
  STORAGE_ENGINE=$(LEVELDB_LIBA)
  STORAGE_ENGINE_PATH=$(LEVELDB_PATH)
  INCS+=-I${LEVELDB_PATH}/include
  LIBS:=${LEVELDB_LIBA} ${SNAPPY_LIBA} ${LIBS}
  CXXFLAGS+=-D__USE_LEVELDB__
else
ifeq ($(storage_engine), forestdb)
  STORAGE_ENGINE=$(FORESTDB_LIBA)
  STORAGE_ENGINE_PATH=$(FORESTDB_PATH)
  INCS+=-I${FORESTDB_PATH}/include
  LIBS:=${FORESTDB_LIBA} ${SNAPPY_LIBA} ${LIBS} 
  CXXFLAGS+=-D__USE_FORESTDB__
  ifeq ($(uname_S),Darwin)
     SEDCMD:=sed -i '.bak' 's/SHARED/STATIC/g' CMakeLists.txt
  else
     SEDCMD:=sed -i 's/SHARED/STATIC/g' CMakeLists.txt
  endif
  ifneq ("$(wildcard /usr/include/libaio.h)","")
     LIBS+=-laio
  endif
else
ifeq ($(storage_engine), lmdb)
  STORAGE_ENGINE=$(LMDB_LIBA)
  STORAGE_ENGINE_PATH=$(LMDB_PATH)
  INCS+=-I${LMDB_PATH}
  LIBS:= ${LMDB_LIBA} ${SNAPPY_LIBA} ${LIBS}
  CXXFLAGS+=-D__USE_LMDB__
else
ifeq ($(storage_engine), rocksdb)
  STORAGE_ENGINE=$(ROCKSDB_LIBA)
  STORAGE_ENGINE_PATH=$(ROCKSDB_PATH)
  INCS+=-I${ROCKSDB_PATH}/include -I${ROCKSDB_PATH}
  LIBS:= ${ROCKSDB_LIBA} ${SNAPPY_LIBA} ${LIBS} 
  CXXFLAGS+=-D__USE_ROCKSDB__ -std=c++11
else
ifeq ($(storage_engine), wiredtiger)
  STORAGE_ENGINE=$(WIREDTIGER_LIBA)
  STORAGE_ENGINE_PATH=$(WIREDTIGER_PATH)
  INCS+=-I${WIREDTIGER_PATH}
  #wiredtiger use dlsym to load extension, must use -rdynamic to export symbol
  LIBS:= ${WIREDTIGER_LIBA} ${SNAPPY_LIBA} ${LIBS} -rdynamic -ldl
  CXXFLAGS+=-D__USE_WIREDTIGER__
else
ifeq ($(storage_engine), perconaft)
  STORAGE_ENGINE=$(PERCONAFT_LIBA)
  STORAGE_ENGINE_PATH=$(PERCONAFT_PATH)
  INCS+=-I${PERCONAFT_PATH}/prefix/include
  LIBS:= ${PERCONAFT_LIBA} ${PERCONAFT_LIBA2} ${SNAPPY_LIBA} ${LIBS} -lz -ldl
  CXXFLAGS+=-D__USE_PERCONAFT__
else
  $(error Only leveldb/lmdb/rocksdb/perconaft/wiredtiger/forestdb supported as env storage_engine value)
endif
endif
endif 
endif
endif
endif

LAST_ENGINE := $(shell sh -c 'cat .kv_engine 2>&1')
ifeq ($(storage_engine), $(LAST_ENGINE))
#do nothing
else
   $(shell sh -c 'echo $(storage_engine) > .kv_engine')
   PREBUILD = clean_db_obj
endif
#

#$(CORE_OBJECTS) $(SERVEROBJ) $(TESTOBJ): | $(ZOOKEEPER_PATH) $(STORAGE_ENGINE_PATH)

all: ${PREBUILD}  test server tools

$(STORAGE_ENGINE_OBJ): $(STORAGE_ENGINE)

$(DIST_LIB): $(STORAGE_ENGINE_OBJ) $(CORE_OBJECTS) 
	${CXX} -shared -o $@ $^

$(DIST_LIBA):$(STORAGE_ENGINE_OBJ) $(CORE_OBJECTS)
	ar rcs $@ $^

lib: $(DEP_LIBS) lua snappy sparsehash $(storage_engine) $(DIST_LIBA) 

server: lib $(CORE_OBJECTS) ${SERVEROBJ}
	${CXX} -o ardb-server $(SERVEROBJ)  $(CORE_OBJECTS) ${STORAGE_ENGINE_OBJ} $(LIBS)

test: lib ${TESTOBJ} $(CORE_OBJECTS)
	${CXX} -o ardb-test ${STORAGE_ENGINE_OBJ} ${TESTOBJ} $(CORE_OBJECTS) $(LIBS) 
	
tools: repair

repair: lib ${REPAIR_TOOL_OBJ}
	${CXX} -o ardb-repair ${REPAIR_TOOL_OBJ} $(DIST_LIBA) $(LIBS) 
	
.PHONY: jemalloc
jemalloc: $(JEMALLOC_LIBA)
$(JEMALLOC_LIBA): $(JEMALLOC_PATH)
	echo ">>>>> Building JEMALLOC" && \
	cd ${JEMALLOC_PATH} && \
	./configure  && \
	$(MAKE) build_lib_static && \
	echo "<<<<< Done building JEMALLOC"

$(JEMALLOC_PATH): ${JEMALLOC_FILE}
	echo ">>>>> Unpacking JEMALLOC" && \
	cd ${LIB_PATH} && \
	tar jxf ${JEMALLOC_FILE} && \
	echo "<<<<< Done unpacking JEMALLOC"

${JEMALLOC_FILE}:
	echo ">>>>> Dowloading JEMALLOC" && \
	cd ${LIB_PATH} && \
	wget -O ${JEMALLOC_FILE} ${JEMALLOC_REMOTE_FILE} && \
	echo "<<<<< Done dowloading JEMALLOC"

.PHONY: snappy
snappy: $(SNAPPY_LIBA)
$(SNAPPY_LIBA):$(SNAPPY_PATH)
	echo ">>>>> Building SNAPPY" && \
	cd ${SNAPPY_PATH} && \
	./configure CXXFLAGS="$(SNAPPY_CXXFLAGS=)" ${XC_BUILD} ${XC_HOST} ${XC_TGT} > configure.out && \
	$(MAKE) libsnappy.la && \
	echo "<<<<< Done building SNAPPY"

$(SNAPPY_PATH): $(SNAPPY_FILE)
	cd ${LIB_PATH} && \
	tar zxf ${SNAPPY_FILE}

${SNAPPY_FILE}:
	echo ">>>>> Dowloading Snappy" && \
	cd ${LIB_PATH} && \
	wget -O ${SNAPPY_FILE} ${SNAPPY_REMOTE_FILE} && \
	echo "<<<<< Done dowloading Snappy"

.PHONY: lmdb
lmdb: $(LMDB_LIBA)
$(LMDB_LIBA): $(LMDB_PATH)
	echo ">>>>> Building LMDB" && \
	cd ${LMDB_PATH} && \
	$(MAKE) && \
	echo "<<<<< Done building LMDB"

$(LMDB_PATH): $(LMDB_FILE)
	echo ">>>>> Unpacking LMDB" && \
	cd ${LIB_PATH} && \
	tar zxf ${LMDB_FILE} && \
	echo "<<<<< Done unpacking LMDB"

${LMDB_FILE}:
	echo ">>>>> Dowloading LMDB" && \
	cd ${LIB_PATH} && \
	wget -O ${LMDB_FILE} ${LMDB_REMOTE_FILE} && \
	echo "<<<<< Done dowloading LMDB"

.PHONY: leveldb
leveldb: $(LEVELDB_LIBA)
$(LEVELDB_LIBA): $(SNAPPY_LIBA) $(LEVELDB_PATH)
	echo ">>>>> Building LEVELDB" && \
	cd ${LEVELDB_PATH} && \
	OPT="$(LEVELDB_OPT)" \
	CXXFLAGS="-I${SNAPPY_PATH} -fno-access-control" \
		CFLAGS="-I${SNAPPY_PATH}" \
		LDFLAGS="${SNAPPY_PATH}/.libs" \
		$(MAKE) out-static/libleveldb.a && \
	echo "<<<<< Done building LEVELDB"

$(LEVELDB_PATH): ${LEVELDB_FILE}
	echo ">>>>> Unpacking LEVELDB" && \
	cd ${LIB_PATH} && \
	tar zxf ${LEVELDB_FILE} && \
	echo "<<<<< Done unpacking LEVELDB"

${LEVELDB_FILE}:
	echo ">>>>> Dowloading LevelDB" && \
	cd ${LIB_PATH} && \
	wget -O ${LEVELDB_FILE} ${LEVELDB_REMOTE_FILE} && \
	echo "<<<<< Done dowloading LevelDB"

.PHONY: rocksdb  
rocksdb: $(ROCKSDB_LIBA) rocksdb_flags

rocksdb_flags:
	$(eval ROCKSDB_LDFLAGS=$(shell grep PLATFORM_LDFLAGS $(ROCKSDB_PATH)/make_config.mk | cut -d '=' -f2 | sed 's/\-lsnappy//' | sed 's/\-lrt//' | sed 's/\-lpthread//')) 
	$(eval LIBS+=$(ROCKSDB_LDFLAGS))
	
$(ROCKSDB_LIBA): $(SNAPPY_LIBA) $(ROCKSDB_PATH)
	@echo ">>>>> Building ROCKSDB" && \
	cd ${ROCKSDB_PATH} && \
	CXXFLAGS="-I${SNAPPY_PATH}" CFLAGS="-I${SNAPPY_PATH}" LDFLAGS="${SNAPPY_PATH}/.libs" $(MAKE) static_lib && \
	echo "<<<<< Done building ROCKSDB" 

$(ROCKSDB_PATH): ${ROCKSDB_FILE}
	@echo ">>>>> Unpacking ROCKSDB" && \
	cd ${LIB_PATH} && \
	tar zxf ${ROCKSDB_FILE} && \
	echo "<<<<< Done unpacking ROCKSDB"
	
${ROCKSDB_FILE}:
	@echo ">>>>> Dowloading RocksDB" && \
	cd ${LIB_PATH} && \
	wget -O ${ROCKSDB_FILE} ${ROCKSDB_REMOTE_FILE} && \
	echo "<<<<< Done dowloading RocksDB"
	
.PHONY: wiredtiger
wiredtiger: $(WIREDTIGER_LIBA)
$(WIREDTIGER_LIBA): $(SNAPPY_LIBA) $(WIREDTIGER_PATH)
	@echo ">>>>> Building WiredTiger" && \
	cd ${WIREDTIGER_PATH} && \
	./configure && $(MAKE) libwiredtiger_static.la && \
	echo "<<<<< Done building WiredTiger"

$(WIREDTIGER_PATH): ${WIREDTIGER_FILE}
	@echo ">>>>> Unpacking WiredTiger" && \
	cd ${LIB_PATH} && \
	tar jxf ${WIREDTIGER_FILE} && \
	echo "<<<<< Done unpacking WiredTiger"
	
${WIREDTIGER_FILE}:
	@echo ">>>>> Dowloading WiredTiger" && \
	cd ${LIB_PATH} && \
	wget -O ${WIREDTIGER_FILE} ${WIREDTIGER_REMOTE_FILE} && \
	echo "<<<<< Done dowloading WiredTiger"

.PHONY: forestdb
forestdb: $(FORESTDB_LIBA)
$(FORESTDB_LIBA): $(FORESTDB_PATH) $(SNAPPY_LIBA)
	echo ">>>>> Building forestdb" ; \
	cd ${FORESTDB_PATH} ; ${SEDCMD} ;\
	mkdir -p build ; cd build ;\
	SNAPPY_DIR=${SNAPPY_PATH}:${SNAPPY_PATH}/.libs cmake ../ ; \
	cc=`grep -c libaio ${FORESTDB_PATH}/build/CMakeCache.txt`; \
	echo ${cc};\
	$(MAKE) ; \
	echo "<<<<< Done building forestdb"


${FORESTDB_PATH}: ${FORESTDB_FILE}
	echo ">>>>> Unpacking forestdb" && \
	cd ${LIB_PATH} && \
	tar zxf ${FORESTDB_FILE} && \
	echo "<<<<< Done unpacking forestdb"
	
${FORESTDB_FILE}:
	echo ">>>>> Dowloading forestdb" && \
	cd ${LIB_PATH} && \
	wget -O ${FORESTDB_FILE} ${FORESTDB_REMOTE_FILE} && \
	echo "<<<<< Done dowloading forestdb"

.PHONY: perconaft
perconaft: $(PERCONAFT_LIBA)
$(PERCONAFT_LIBA): ${PERCONAFT_PATH} $(SNAPPY_LIBA) 
	echo ">>>>> Building PerconaFT" && \
	cd ${LIB_PATH} && \
	cd ${PERCONAFT_PATH} && rm -rf build &&\
	mkdir -p build && cd build &&\
	cmake -D CMAKE_BUILD_TYPE=Debug -D BUILD_TESTING=OFF -D USE_VALGRIND=OFF -D CMAKE_INSTALL_PREFIX=../prefix/ ../ && $(MAKE) && $(MAKE) install &&\
	echo "<<<<< Done building PerconaFT"

${PERCONAFT_PATH}: ${PERCONAFT_FILE}
	echo ">>>>> Unpacking PerconaFT" && \
	cd ${LIB_PATH} && \
	unzip -o ${PERCONAFT_FILE} && \
	echo "<<<<< Done unpacking PerconaFT"

${PERCONAFT_FILE}:
	echo ">>>>> Dowloading PerconaFT" && \
	cd ${LIB_PATH} && \
	wget -O ${PERCONAFT_FILE} ${PERCONAFT_REMOTE_FILE} && \
	echo "<<<<< Done dowloading PerconaFT"

.PHONY: lua
lua: $(LUA_LIBA)
$(LUA_LIBA):
	echo ">>>>> Building LUA" && \
	cd ${LIB_PATH} && \
	cd ${LUA_PATH}/src && \
	$(MAKE) all CFLAGS="$(LUA_CFLAGS)" MYLDFLAGS="$(LUA_LDFLAGS)" && \
	echo ">>>>> Done building LUA"
	
.PHONY: cjson
cjson: $(CJSON_LIBA)
$(CJSON_LIBA):
	echo ">>>>> Building cJSON" && \
	cd ${LIB_PATH} && \
	cd ${CJSON_PATH} && \
	$(MAKE) && \
	echo ">>>>> Done building cJSON"

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
	echo "<<<<<< Done unpacking ZOOKEEPER"

.PHONY: sparsehash
sparsehash: $(SPARSEHASH_CONFIG)
$(SPARSEHASH_CONFIG):
	echo ">>>>> Building sparsehash" && \
	cd ${LIB_PATH}; tar zxvf ${SPARSEHASH_FILE};\
	cd ${SPARSEHASH_PATH}; ./configure; \
	${MAKE} src/sparsehash/internal/sparseconfig.h && \
	echo ">>>>> Done building sparsehash"


clean_db_obj:
	rm -f db/db.o

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
	cp ardb-server ardb-${ARDB_VERSION}/bin; cp ardb-test ardb-${ARDB_VERSION}/bin; cp ardb-repair ardb-${ARDB_VERSION}/bin; cp ../ardb.conf ardb-${ARDB_VERSION}/conf; \
	tar czvf ardb-bin-${ARDB_VERSION}.tar.gz ardb-${ARDB_VERSION}; rm -rf ardb-${ARDB_VERSION};

clean:
	rm -f  ${CORE_OBJECTS} $(SERVEROBJ) ${STORAGE_ENGINE_ALL_OBJ} ${TESTOBJ} ${REPAIR_TOOL_OBJ} ${DIST_LIBA} ${DIST_LIB} \
	       ardb-test  ardb-server ardb-repair

clobber: clean_deps clean
