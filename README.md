# Ardb
Ardb is a BSD licensed, redis-protocol compatible persistent storage server, it support different storage engines. Currently LevelDB/KyotoCabinet/LMDB are supported, but only LevelDB engine is well tested.


## Status
Still in developement, not full production ready yet.

## Compile
LevelDB is the defaul storeage engine, to compile with leveldb, just type 'make' to compile server & lib & tests.

To use 	KyotoCabinet/LMDB as storage engine, you should set env 'storage_engine' first.
	
	storage_engine=kyotocabinet make
	storage_engine=lmdb make

It should compile to several executables in 'src' directory, such as ardb-server, ardb-test etc.
	

## Features
- Full redis-protocol compatible
- Most redis commands supported
- Different storage engine supported(LevelDB/KyotoCabinet/LMDB)
- Replication(Master-Slave/Master-Master)
- Backup data online
- Simple Table data structure [Table In Ardb](https://github.com/yinqiwen/ardb/wiki/Table-In-Ardb)

## Client API
Since ardb is a full redis-protocol compatible server, you can use any redis client to connect it. Here lists all redis clients. <http://www.redis.io/clients>

## Benchmark
Benchmarks were all performed on a four-core Intel(R) Xeon(R) CPU X3440 @ 2.53GHz, with 8 GB of DDR3 RAM.

The benchmark tool is 'redis-benchmark' from redis,50 parallel clients, 10000000 requests, 1000000 random keys each test case.

LevelDB Options: block_cache_size=512m, write_buffer_size=512m, thread_pool_size=2

![Benchmark Img](https://raw.github.com/yinqiwen/ardb/master/doc/benchmark.png)

	Becnhmark data(./redis-benchmark -t 'xxx' -r 10000000 -n 10000000):
	                                  Ardb-LevelDB(qps)    Redis(qps)    PING_INLINE	                      158730.16            156250    PING_BULK	                      161290.33            163934.42    SET	                              90859.53	            140845.06    GET	                              128279.13            149253.73    INCR	                          79841.59	            149253.73    LPUSH	                          74112.50	            161290.33    LPOP	                          41666.67	            161290.33    SADD	                          29583.09	            149253.73    SPOP	                          27638.05	            153846.16    LPUSH	                          72674.41	            161290.33    LRANGE_100(first 100 elements)	   15262.52             49019.61    LRANGE_300(first 300 elements)	   5015.80	            21008.4    LRANGE_500(first 450 elements)	   3430.06	            14684.29    LRANGE_600(first 600 elements)	   2614.72	            11312.22    MSET (10 keys)	                   12923.24	            54347.82

- Note: The 'get' performance in Ardb may be slower in reality becauseof cache missing.

         

## Ardb vs Redis(2.6) 
 * Unsupported Redis Commands:
  - DUMP 
  - MIGRATE
  - OBJECT
  - RANDOMKEY
  - RESTORE
  - BLPOP/BRPOP
  - SRANDMEMBER
  - EVAL/EVALSHA/SCRIPT
  - AUTH
  - CONFIG RESETSTAT
  - DEBUG/MONITOR
 * Additional Commands:
  - HClear/SClear/ZClear/LClear
  - SUnionCount/SInterCount/SDiffCount
  - ZAdd with limit
  - ZPop/ZRPop
  - HMIncrby
  - \_\_SET\_\_/\_\_DEL\_\_(for replication)
  - TCreate/TDesc/TLen
  - TInsert/TReplace
  - TUpdate/TDel
  - TCreateIndex
  - TGet/TGetAll(Table commands)
  - CompactDB/CompactAll
  - BitOPCount
  






