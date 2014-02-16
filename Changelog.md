# Changelog

## Ardb 0.6.1
### Features
- New command 'GeoAdd/GeoSearch' to store/search 2d spatial data in sorted set.
- New command 'Cache Load/Evict' to improve 'GeoSearch' performance.
- New redis commands 'Scan/ZScan/HSacan/SScan' supported now.

### Improve
- Many bugs fix
- Changed string's comparator behavior, now using default string comparator(like 'strcmp') instead of previous comparator which would compare string's length first.
- Improve keys/keyscount performance while pattern's last char is '*' 
- Upgrade dependency libraries(jemalloc,leveldb,rocksdb)

  






