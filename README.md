# Ardb: A High Performance Persistent NoSql, Full Redis-Protocol Compatibility
[![Build Status](https://travis-ci.org/yinqiwen/ardb.svg?branch=master)](https://travis-ci.org/yinqiwen/ardb)  
Ardb is a BSD licensed, redis-protocol compatible persistent nosql, it support multiple storage engines as backend like [Google's LevelDB](https://github.com/google/leveldb), [Facebook's RocksDB](https://github.com/facebook/rocksdb), [OpenLDAP's LMDB](http://symas.com/mdb/), the default backend is [Facebook's RocksDB](https://github.com/facebook/rocksdb).


## Compile
Rocksdb is the default storage engine, to compile with rocksdb, just type `make` to compile server & lib & tests.

To use LMDB or LevelDB as storage engine, you should set env `storage_engine` first.
	
	storage_engine=lmdb make
	storage_engine=leveldb make

It should compile to several executables in `src` directory, such as ardb-server, ardb-test etc.
	

## Features
- Full redis-protocol compatibility
- 2d spatial index supported. [Spatial Index](https://github.com/yinqiwen/ardb/blob/master/doc/spatial-index.md)
- Most redis commands supported, and a few new commands
  * [Ardb commands VS Redis Commands](https://github.com/yinqiwen/ardb/wiki/ARDB-Commands)
- Different storage engine supported (LevelDB/LMDB/RocksDB)
- Replication compatible with Redis 2.6/2.8
  * Ardb instance work as slave of Redis 2.6/2.8+ instance
  * Ardb instance work as master of Redis 2.6/2.8+ instance
  * Ardb instance work as slave of Ardb instance
  * [Replication detail](https://github.com/yinqiwen/ardb/wiki/Replication)
- Auto failover support by redis-sentinel
- Lua Scripting support 
- Backup data online
  * Use 'save/bgsave' to backup data
  * Use 'import' to import backup data
  * [Backup & Restore](https://github.com/yinqiwen/ardb/wiki/Backup-Commands)

## Clients
Since ardb is a full redis-protocol compatible server, you can use most existed redis client to connect it without any problem. Here lists all redis clients. <http://www.redis.io/clients>  

* **Known Issues**:   

  - For Node.js, the recommand client [node_redis](https://github.com/mranney/node_redis) would try to parse `redis_version:x.y.z` from `info` command's output, Ardb users should uncomment `additional-misc-info` in ardb.conf to makesure that `redis_version:x.y.z` exists in `info` command's output. There is an online redis GUI admin service [redsmin](https://redsmin.com) build on [node_redis](https://github.com/mranney/node_redis), users can test ardb's redis protocol conformance by a visual way. 
  
  

## Benchmark
Benchmarks were all performed on a four-core Intel(R) Xeon(R) CPU E5520@2.27GHz, with 64 GB of DDR3 RAM, 500 GB of SCSI disk

The benchmark tool is 'redis-benchmark' from redis,50 parallel clients, 10000000 requests, 1000000 random keys each test case.

GCC Version:4.8.3  
OS Version: Red Hat Enterprise Linux AS release 4 (Nahant Update 3)   
Kernel Version: 2.6.32_1-10-6-0       
Redis Version: 2.8.9  
Ardb Version: 0.7.2(LMDB 0.9.11, LevelDB1.16.0, RocksDB3.1)  
LevelDB Options: thread_pool_size=2, block_cache_size=512m, write_buffer_size=128m, compression=snappy   
RocksDB Options: thread_pool_size=2, block_cache_size=512m, write_buffer_size=128m, compression=snappy   
LMDB Option: thread_pool_size=2, database_max_size=10G, readahead=no    

![Benchmark Img](https://raw.github.com/yinqiwen/ardb/master/doc/benchmark.png)

	Becnhmark data(./redis-benchmark -r 10000000 -n 10000000):
	                        LevelDB	  LMDB	        RocksDB	    Redis
    PING_INLINE	            95075.11	91945.56	92274.76	79669.85
    PING_BULK	            99265.43	92988.66	95721.27	90044.66
    SET	                    62361.64	72490.03	55567.91	73692.51
    GET	                    69045.99	93805.12	68078.16	82459.95
    INCR	                47572.16	59805.03	34883.32	74940.61
    LPUSH	                47369.57	27713.11	40584.42	105466.32
    LPOP	                31787.41	14711.51	10088.27	105797.72
    SADD	                37583.39	41779.82	24421.81	100405.64
    SPOP	                 8538.13	17614.32	7955.45  	75253.04
    LPUSH(for LRANGE)	    49504.95	16998.71	40719.93	87989.45
    LRANGE_100 	            11090.16	17639.79	10302.91  	45831.61
    LRANGE_300               4453.15  	6578.86	    4127.29	    17608.11
    LRANGE_500 	             3503.85	4552.06  	2185.72     12345.07
    LRANGE_600 	             2680.82	3082.94	    1650.46      7906.01
    MSET (10 keys)	         9675.48	7204.56	    5255.44	     35967.6


* Note: 
  - **Ardb uses 2 threads in benchmark test, while redis is single threaded application. That's the reason ardb is faster than redis in some test cases.**
  - **LevelDB & LMDB & RocksDB all use tree like structure on disk, more data is stored, the server is slower for query operations in theory.**
         

## Cross compilation

Cross compilation is a work in progress. It has been shown to work for
the LevelDB engine and the LMDB engine when compiling on x86 for 32bit
ARM. The RocksDB engine has not been shown to work.  To cross compile,
set the make variables XC_BUILD, XC_HOST, and XC_TGT. These make
variables correspond to autoconf's notion of build, host, and
target. This [link]
(https://www.gnu.org/software/autoconf/manual/autoconf-2.69/html_node/Specifying-Target-Triplets.html#Specifying-Target-Triplets)
explains what each means. You will also need to set appropriate values
for LD, CC, and CXX to point to the appropriate compilers and linkers
for the platform are used. For example, to build on x86 for 32 bit
ARM, the make invocation could look like:

> make AR=<path to ARM ar> LD=<path to ARM ld> CC=<path to ARM gcc> CXX=<path to ARM g++> XC_HOST="x86-unknown-linux" XC_HOST="arm-unknown-linux"

If you see an error like:

>#error "Missing implementation for 32-bit atomic operations"

Then you can either use `-DJE_FORCE_SYNC_COMPARE_AND_SWAP_4` as a CPP flag or use libc for malloc by setting the make variable `MALLOC` to `libc`.

## Ardb vs Redis(2.8.9) 
 * Unsupported Redis Commands:
  - DUMP 
  - MIGRATE
  - OBJECT
  - RESTORE
  - CONFIG RESETSTAT
  - DEBUG
  - MONITOR
  - BITPOS
  - PUBSUB
 * New Ardb Commands:
  - SUnionCount/SInterCount/SDiffCount
  - HMIncrby
  - CompactDB/CompactAll
  - BitOPCount
  - Import
  - KeysCount
  - GeoAdd
  - GeoSearch
  - Cache 

## Misc
###Memory Usage
In basho's fork of leveldb [project](https://github.com/basho/leveldb), they give a way to estimate memory usage of a database.

      write_buffer_size*2    
     + max_open_files*4194304    
     + (size in NewLRUCache initialization)  
 
If you want to limit the total memory usage, you should tweak configuration options whoose names start with `leveldb.` in ardb.conf.

## Community

  - Join the [mailing list](https://groups.google.com/forum/#!forum/ardb-nosql)(Subscribe via [email](mailto:ardb-nosql+subscribe@googlegroups.com))