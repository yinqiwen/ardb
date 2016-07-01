# Changelog
## Ardb 0.9.3
### Features
- New command 'Touch'.

### Improve
- Bug fix


## Ardb 0.9.2
### Features
- New storage engine forestdb.
- New command 'Client Reply/Kill'.
- Native backup/restore support for LMDB engine.
- Add internal snaoshot manager to manage all snapshots

### Improve
- Bug fix


## Ardb 0.9.1
### Features
- New snapshot type 'bakcup' for rocksdb engine to improve master/slave full resync performance.
- 'Monitor' command support.

### Improve
- Much faster slave replication speed by using more than 1 thread
- Bug fix

## Ardb 0.9.0
### Features
- Redesign whole project. It's not compatible with previous version.
- More redis commands supported. (restore/migrate/dump...)
- More storage engine supported. (rocksdb/leveldb/lmdb/wiredtiger/perconaft).
- Use more rocksdb's own features or better perormance, like preix seek/compact filter/merge operations.
- Better ttl expire mechanism support.
- Lua unit test cases.

### Improve
- NA


## Ardb 0.8.0
### Features
- High level cache supported for string/list/set/zset/hash.
- Each listen adress has its own thread pool size and qps limit setting.
- Auto compact support for leveldb/rocksdb engine.
- Trusted IP support, which could refuse untrusted client from unknown ip.

### Improve
- Refactor all code, much easier to add new command & unit test case, and easier to embed it as a library.
- New internal encode/decode format, which make it not compatible with previous version.
- Huge performance improvement for lpop/spop/lrange.
- More unit test case. 
- Many bug fixed.


## Ardb 0.7.0
### Features
- New command 'GeoAdd/GeoSearch' to store/search 2d spatial data in sorted set.
- New command 'Cache Load/Evict' to improve 'GeoSearch' performance.
- New redis commands 'Scan/ZScan/HSacan/SScan' supported now.

### Improve
- Many bugs fix 
- Changed string's comparator behavior, now using default string comparator(like 'strcmp') instead of previous comparator which would compare string's length first.
- Improve keys/keyscount performance while pattern's last char is '*' 
- Upgrade dependency libraries(jemalloc,leveldb,rocksdb)

  






