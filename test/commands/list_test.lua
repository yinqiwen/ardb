--[[   --]]
ardb.call("del", "mylist")
local s = ardb.call("lpush", "mylist", "world")
ardb.assert2(s == 1, s)
s = ardb.call("lpush", "mylist", "hello")
ardb.assert2(s == 2, s)
--[[ sequential list lrange  --]]
local vs = ardb.call("lrange", "mylist", "0", "-1")
ardb.assert2(table.getn(vs) == 2, vs)
ardb.assert2(vs[1] == "hello", vs)
ardb.assert2(vs[2] == "world", vs)
s = ardb.call("rpush", "mylist", "a0", "a1")
ardb.assert2(s == 4, s)
--[[ sequential list lindex  --]]
s = ardb.call("lindex", "mylist", "10")
ardb.assert2(s == false, s)
s = ardb.call("lindex", "mylist", "1")
ardb.assert2(s == "world", s)

s = ardb.call("lrem", "mylist", "1", "ax")
ardb.assert2(s == 0, s)
s = ardb.call("lrem", "mylist", "-2", "a0")
ardb.assert2(s == 1, s)
s = ardb.call("lrem", "mylist", "2", "a1")
ardb.assert2(s == 1, s)
--[[ non sequential list lrange  --]]
local vs = ardb.call("lrange", "mylist", "0", "-1")
ardb.assert2(table.getn(vs) == 2, vs)
ardb.assert2(vs[1] == "hello", vs)
ardb.assert2(vs[2] == "world", vs)
--[[ non sequential list lindex  --]]
s = ardb.call("lindex", "mylist", "10")
ardb.assert2(s == false, s)
s = ardb.call("lindex", "mylist", "1")
ardb.assert2(s == "world", s)
s = ardb.call("llen", "mylist")
ardb.assert2(s == 2, s)

