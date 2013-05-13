# Ardb
============================================================
Ardb is a redis-protocol compatible persistent storage server, it support different storage engines. Currently LevelDB/KyotoCabinet/LMDB are supported. 

## Overview
TODO

## Client API
Since ardb is a full redis-protocol compatible server, you can use any redis client to connect it.

## Benchmark
TODO

## Redis COMMAND Supported
------------------------------------------------------------

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






