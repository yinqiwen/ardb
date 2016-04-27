--[[   --]]
ardb.call("del", "key1", "key2")
ardb.call("mset2", "key1", "foobar", "key2", "abcdef")
local s = ardb.call("bitop", "and", "dest", "key1", "key2")
ardb.assert2(s == 6, s)

ardb.call("del", "mykey")
s = ardb.call("setbit", "mykey", "7", "1")
ardb.assert2(s == 0, s)
s = ardb.call("getbit", "mykey", "7")
ardb.assert2(s == 1, s)
s = ardb.call("setbit", "mykey", "7", "0")
ardb.assert2(s == 1, s)