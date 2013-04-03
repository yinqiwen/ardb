#Makefile
#
# Created on: 2013-3-28
#     Author: wqy

CXX=g++
CC=gcc

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

CPPFILES := $(foreach dir, $(CPP_VPATH), $(wildcard $(dir)/*.cpp))
CFILES := $(foreach dir, $(C_VPATH), $(wildcard $(dir)/*.c))
CPP_OBJECTS := $(patsubst %.cpp, %.o, $(CPPFILES))
C_OBJECTS := $(patsubst %.c, %.o, $(CFILES))

LEVELDB_OBJECTS := ../test/leveldb.o


DIST_LIB = librddb.so
DIST_LIBA = librddb.a

all: $(DIST_LIBA) 

$(DIST_LIB):$(CPP_OBJECTS) ${C_OBJECTS}
	${CXX} -shared -o $@ $^

$(DIST_LIBA):$(CPP_OBJECTS) ${C_OBJECTS}
	ar rcs $@ $^
	
leveldb:$(LEVELDB_OBJECTS) $(CPP_OBJECTS) ${C_OBJECTS}
	${CXX} -o leveldb $(LEVELDB_OBJECTS) $(CPP_OBJECTS) ${C_OBJECTS} $(LIBS) 

clean:
	rm -f ${CPP_OBJECTS} ${C_OBJECTS} $(DIST_LIBA) $(DIST_LIB)
