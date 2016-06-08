# Ardb: A High Performance Persistent NoSql, Full Redis-Protocol Compatibility

[![Join the chat at https://gitter.im/yinqiwen/ardb](https://badges.gitter.im/yinqiwen/ardb.svg)](https://gitter.im/yinqiwen/ardb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/yinqiwen/ardb.svg?branch=0.9)](https://travis-ci.org/yinqiwen/ardb)  
Ardb is a BSD licensed, redis-protocol compatible persistent nosql, it support multiple storage engines as backend like [Google's LevelDB](https://github.com/google/leveldb), [Facebook's RocksDB](https://github.com/facebook/rocksdb), [OpenLDAP's LMDB](http://symas.com/mdb/), [WiredTiger](http://www.wiredtiger.com/), [PerconaFT](https://github.com/percona/PerconaFT),[Couchbase's ForestDB](https://github.com/couchbase/forestdb) the default backend is [Facebook's RocksDB](https://github.com/facebook/rocksdb).


## Compile
Rocksdb is the default storage engine, to compile with rocksdb, just type `make` to compile server & lib & tests.

To use LMDB or LevelDB or WiredTiger as storage engine, you should set env `storage_engine` first.
	
	storage_engine=rocksdb make
    storage_engine=leveldb make
    storage_engine=lmdb make
	storage_engine=wiredtiger make
	storage_engine=perconaft make
	storage_engine=forestdb make


It should compile to several executables in `src` directory, such as ardb-server, ardb-test etc.
	

## Features
- Full redis-protocol compatibility
- 2d spatial index supported. [Spatial Index](https://github.com/yinqiwen/ardb/wiki/Spatial-Index)
	- Redis 3.2 geo commands support
- Most redis commands supported, and a few new commands.
  * [Ardb commands VS Redis Commands](https://github.com/yinqiwen/ardb/wiki/ARDB-Commands)
- Multi storage engines supported
  * [RocksDB](https://github.com/facebook/rocksdb)
  * [LevelDB](https://github.com/google/leveldb)
  * [LMDB](http://symas.com/mdb/)
  * [WiredTiger](http://www.wiredtiger.com/)
  * [PerconaFT](https://github.com/percona/PerconaFT)
  * [ForestDB](https://github.com/couchbase/forestdb)
- Replication compatible with Redis 2.6/2.8
  * Ardb instance work as slave of Redis 2.6/2.8+ instance
  * Ardb instance work as master of Redis 2.6/2.8+ instance
  * Ardb instance work as slave of Ardb instance
- Auto failover support by redis-sentinel
- Lua Scripting support 
- Pub/Sub
  * All redis pubsub commands supported
- Transactions
  * All redis transaction commands supported
- Backup data online
  * Use 'save/bgsave' to backup data
  * Use 'import' to import backup data


## Clients
Since ardb is a full redis-protocol compatible server, you can use most existed redis client to connect it without any problem. Here lists all redis clients. <http://www.redis.io/clients>  

* **Known Issues**:   

  - For Node.js, the recommand client [node_redis](https://github.com/mranney/node_redis) would try to parse `redis_version:x.y.z` from `info` command's output, Ardb users should configure `redis-compatible-version` in ardb.conf to makesure that `redis_version:x.y.z` exists in `info` command's output. There is an online redis GUI admin service [redsmin](https://redsmin.com) build on [node_redis](https://github.com/mranney/node_redis), users can test ardb's redis protocol conformance by a visual way. 
  
  

## Benchmark
Benchmarks were all performed on a four-core Intel(R) Xeon(R) CPU E5520@2.27GHz, with 64 GB of DDR3 RAM, 500 GB of SCSI disk

The benchmark tool is 'redis-benchmark' from redis,50 parallel clients, 10000000 requests, 1000000 random keys each test case.

GCC Version:4.8.3  
OS Version: Red Hat Enterprise Linux AS release 4 (Nahant Update 3)   
Kernel Version: 2.6.32_1-10-6-0       
Redis Version: 2.8.9  
Ardb Version: 0.9.1(RocksDB4.3.1), 1 thread(thread-pool-size configured 1) & 16 threads(thread-pool-size configured 16) 
RocksDB Options: 

     write_buffer_size=128M;max_write_buffer_number=16;compression=kSnappyCompression;
     block_based_table_factory={block_cache=512M;block_size=4;filter_policy=bloomfilter:10:true};
     create_if_missing=true;max_open_files=-1;rate_limiter_bytes_per_sec=50M   

![Benchmark Img](https://raw.githubusercontent.com/yinqiwen/ardb/0.9/doc/benchmark.png)

	Becnhmark data(./redis-benchmark -r 10000000 -n 10000000):
                                    Ardb(1thread)   Ardb(16threads)    Redis
    PING_INLINE                     66313.01        79394.7            67294.75
    PING_BULK                       66844.91        79384.61           65703.02
    SET                             36238.45        67963.41           64574.45
    GET                             46979.24        74050.48           65112.64
    INCR                            35522.72        68102.27           65274.15
    LPUSH                           24789.29        35788.93           66093.85
    LPOP                            15812.53        15657              65832.78
    SADD                            13130.08        12998.49           65573.77
    SPOP                            200             200                63291.14
    LPUSH(for LRANGE)               27693.16        38611.53           65487.89
    LRANGE_100 (first 100 elements) 7857.93         33828.36           30797.66
    LRANGE_300 (first 300 elements) 3176.16         16369.29           15710.92
    LRANGE_500 (first 450 elements) 2156.1          11706.17           11504.83
    LRANGE_600 (first 600 elements) 1647.88         9192.53            9094.22
    MSET (10 keys)                  10217.64        13552.71           37678.97



#####Note     
- Ardb uses 1 thread & 16 threads in this benchmark test, while redis is actually single threaded application. Ardb is a multithreaded applcation, you can start the server with more threads by setting 'thread-pool-size' to 16 or higher to increase the read/write performance.    
- There is no any performance improve for SADD/LPUSH/LPOP with 16 threads , because in the test SADD/LPUSH/LPOP always operate on same key, while SADD/LPUSH/LPOP would lock the key until write operation done.
- SPOP have very poor performance in ardb.
         

## Misc
- [Ardb commands VS Redis Commands](https://github.com/yinqiwen/ardb/wiki/ARDB-Commands)
- [Ardb design draft](https://github.com/yinqiwen/ardb/wiki/Design-Draft)
- [Spatial Index](https://github.com/yinqiwen/ardb/wiki/Spatial-Index)

## Community

  - Join the [mailing list](https://groups.google.com/forum/#!forum/ardb-nosql)(Subscribe via [email](mailto:ardb-nosql+subscribe@googlegroups.com))
