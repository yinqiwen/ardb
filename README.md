# Ardb
===================================
Ardb is a redis-protocol compatible persistent storage server, it support different storage engines. Currently LevelDB/KyotoCabinet/LMDB are supported. 

## Overview
TODO

## Features
- Full redis-protocol compatibel
- Most redis commands supported
- Different storage engine supported(LevelDB/KyotoCabinet/LMDB)
- Replication(Master-Slave/Master-Master)
- Backup data online
- Simple Table data structure supported

## Client API
Since ardb is a full redis-protocol compatible server, you can use any redis client to connect it. Here lists all redis clients. <http://www.redis.io/clients>

## Benchmark
Benchmarks were all performed on a four-core Intel(R) Xeon(R) CPU X3440 @ 2.53GHz, with 8 GB of DDR3 RAM.

The benchmark tool is 'redis-benchmark' from redis,50 parallel clients, 10000000 requests, 1000000 random keys each test case.

LevelDB Options: block_cache_size=512m, write_buffer_size=512m

![Benchmark Img](https://raw.github.com/yinqiwen/ardb/master/doc/benchmark.png)

	Becnhmark test data:
	                                Ardb-LevelDB  Redis    PING_INLINE	                      158730.16  156250    PING_BULK	                      161290.33 163934.42    SET	                              67824.20	140845.06    GET	                              72181.32	149253.73    INCR	                          52397.17	149253.73    LPUSH	                          58139.53	161290.33    LPOP	                          41666.67	161290.33    SADD	                          42129.39	149253.73    SPOP	                          30340.73	153846.16    LPUSH	                          51546.39	161290.33    LRANGE_100(first 100 elements)	   6531.68	49019.61    LRANGE_300(first 300 elements)	   2151.93	21008.4    LRANGE_500(first 450 elements)	   1462.63	14684.29    LRANGE_600(first 600 elements)	   1096.61	11312.22    MSET (10 keys)	                   17035.78	54347.82

- Note: The 'get' performance in Ardb may be slower in reality becauseof cache missing.

         

## Redis COMMAND Supported
------------------------------------------
* keys:
  - del/exists/expire/expireat
  - pexpire/pexpireat/pttl/rename/renameex
  - incr/incrby
  - keys/migrate/move/persist
  - sort
  - ttl/type

* strings:
  - append
  - bitcount/bitop/getbit/setbit
  - decr/decrby/incr/incrby/incrbyfloat
  - get/getrange/getset/mget/mset/msetnx/psetnx/set
  - setex/setnx/setrange/strlen

* hashs:
  - hget/hmget
  - hset/hmset/hsetnx
  - hkeys
  - hsetnx
  - hdel
  - hexists
  - hgetall
  - hvals
  - hlen
  - hincrby/hincrbyfloat/hmincrby
  
* lists:
  - lindex/linsert
  - llen/lrange
  - lpop/rpop
  - lpush/rpush
  - lpushx/rpushx
  - lrem/lset
  - ltrim
  - rpoplpush
  
* sets:
  - sadd/spop
  - scard
  - sdiff/sdiffstore
  - sinter/sinterstore
  - sunion/sunionstore
  - sintercount/suinioncount/sdiffcount
  - sismember/smembers
  - smove/srem
  
* sorted sets:
  - zadd/zrem
  - zcard/zcount
  - zincrby
  - zrange/zrangebyscore
  - zinterstore/zunionstore
  - zrank/zremrangebyrank
  - zscore/zremrangebyscore
  - zrevrange/zrevrangebyscore
  - zrevrank
  
* Pub/Sub:
  - psubscribe/punsubscribe
  - subscribe/unsubscibe
  - publish
  
* transaction commands:
  - multi
  - exec
  - discard
  - watch/unwatch

* connection commans:
  - select
  - echo
  - ping
  - quit

* server:
  - bgsave/save/lastsave
  - client kill/client list/client getname/client setname
  - config get/config set
  - dbsize
  - flushall/flushdb
  - info
  - shutdown
  - slaveof
  - slowlog
  - sync
  - time






