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
The benchmark tool is 'redis-benchmark' from redis.

    Ping: 50 parallel clients, 10000000 requests
        Ardb:  161759.95 requests per second 
        Redis: 158914.30 requests per second
    Set: 50 parallel clients, 10000000 requests
        ./redis-benchmark -t set -n 10000000 -r 10000000
        Redis:         146515.86 requests per second
        Ardb-LevelDB:  62638.98 requests per second
         

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






