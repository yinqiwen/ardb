#Makefile
#
# Created on: 2013-3-28
#     Author: wqy

CXX=g++
CC=gcc

LIB_PATH=$(PWD)/../deps

LMDB_PATH=${LIB_PATH}/mdb/libraries/liblmdb

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

LEVELDB_VER=leveldb-1.10.0
LEVELDB_PATH=${LIB_PATH}/${LEVELDB_VER}
LEVELDB_FILE=${LEVELDB_PATH}.tar.gz
LEVELDB_LIBA=${LEVELDB_PATH}/libleveldb.a

KCDB_VER=kyotocabinet-1.2.76
KCDB_PATH=${LIB_PATH}/${KCDB_VER}
KCDB_FILE=${KCDB_PATH}.tar.gz
KCDB_LIBA=${KCDB_PATH}/libkyotocabinet.a

CXXFLAGS=-Wall -Wno-unused-parameter -Wno-format -g -O2  -D_FILE_OFFSET_BITS=64 -fPIC
CCFLAGS=-Wall -Wno-unused-parameter -Wbad-function-cast -std=gnu99 -O2 -pedantic -Wno-format -g -D_FILE_OFFSET_BITS=64 -fPIC
LDFLAGS=-g 

INCS=-I./  -I../deps/cpp-btree 

LIBS=  ${TCMALLOC_LIBA} -lpthread

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

SERVER_OBJECTS := ardb_server.o transaction.o slowlog.o clients.o replication.o pubsub.o oplogs.o main.o

#DIST_LIB = libardb.so
DIST_LIBA = libardb.a

ifeq ($(origin storage_engine), undefined)
storage_engine=leveldb
endif

ifeq ($(storage_engine), leveldb)
  STORAGE_ENGINE_OBJ=${LEVELDB_ENGINE}
  STORAGE_ENGINE=leveldb ${STORAGE_ENGINE_OBJ}
  INCS+=-I${LEVELDB_PATH}/include
  LIBS:=${LEVELDB_LIBA} ${SNAPPY_LIBA} ${LIBS}
  TESTOBJ=${LEVELDB_TEST}
else
ifeq ($(storage_engine), kyotocabinet)
  STORAGE_ENGINE_OBJ=${KCDB_ENGINE}
  STORAGE_ENGINE=kyotocabinet ${STORAGE_ENGINE_OBJ}
  INCS+=-I${KCDB_PATH}/include
  LIBS:= ${KCDB_LIBA} ${LIBS} -lz
  TESTOBJ=${KCDB_TEST}
  CXXFLAGS+=-D__USE_KYOTOCABINET__
else
ifeq ($(storage_engine), lmdb)
  STORAGE_ENGINE_OBJ=${LMDB_ENGINE}
  STORAGE_ENGINE=lmdb ${STORAGE_ENGINE_OBJ}
  INCS+=-I${LMDB_PATH}/include
  LIBS:= ${LMDB_LIBA} ${LIBS}
  TESTOBJ=${LMDB_TEST}
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

lib:tcmalloc $(DIST_LIBA)

server:$(SERVER_OBJECTS) $(CORE_OBJECTS) $(CHANNEL_OBJECTS) ${STORAGE_ENGINE}
	${CXX} -o ardb-server $(SERVER_OBJECTS)  $(CORE_OBJECTS) $(CHANNEL_OBJECTS) ${STORAGE_ENGINE_OBJ} $(LIBS)

test:${STORAGE_ENGINE} $(CORE_OBJECTS) ${TESTOBJ}
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
      ./configure;make;\
    fi
    
snappy:
	@if test -f ${SNAPPY_LIBA}; then\
      :; \
    else \
      cd ${LIB_PATH};tar zxvf ${SNAPPY_FILE};cd ${SNAPPY_PATH};\
      ./configure;make;\
    fi

kyotocabinet:
	@if test -f ${KCDB_LIBA}; then\
      :; \
    else \
      cd ${LIB_PATH};tar zxvf ${KCDB_FILE};cd ${KCDB_PATH};\
      ./configure;make;\
    fi

leveldb:snappy
	@if test -f ${LEVELDB_LIBA}; then\
      :; \
    else \
      cd ${LIB_PATH};tar zxvf ${LEVELDB_FILE};cd ${LEVELDB_PATH};\
      CXXFLAGS="-I${SNAPPY_PATH}" CFLAGS="-I${SNAPPY_PATH} LDFLAGS=${SNAPPY_PATH}/.libs" make libleveldb.a;\
    fi

clean:
	rm -f $(LEVELDB_TEST) $(KCDB_TEST) ${CORE_OBJECTS} ${LEVELDB_OBJECTS} \
	      $(SERVER_OBJECTS) $(CHANNEL_OBJECTS) $(DIST_LIBA) $(DIST_LIB)   \
	      $(LEVELDB_ENGINE) $(KCDB_ENGINE) $(LMDB_ENGINE) \
	      ardb-test  ardb-server
