--[[   --]]
ardb.call("del", "mylist")
local s = ardb.call("lpush", "mylist", "world")
ardb.assert2(s == 1, s)
s = ardb.call("lpushx", "notexist_list", "world")
ardb.assert2(s == 0, s)
s = ardb.call("rpushx", "notexist_list", "world")
ardb.assert2(s == 0, s)
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
ardb.call("lpush", "mylist", "lpopvalue0", "lpopvalue1")
--[[ sequential list lpop  --]]
s = ardb.call("lpop", "mylist")
ardb.assert2(s == "lpopvalue1", s)
s = ardb.call("lpop", "mylist")
ardb.assert2(s == "lpopvalue0", s)
s = ardb.call("lpop", "notexist_list")
ardb.assert2(s == false, s)
--[[ sequential list lset  --]]
s = ardb.call("lset", "mylist", "1", "x")
ardb.assert2(s["ok"] == "OK", s)
s = ardb.call("lindex", "mylist", "1")
ardb.assert2(s == "x", s)
ardb.call("lset", "mylist", "1", "world")

--[[ sequential list ltrim  --]]
ardb.call("lpush", "mylist", "ltrim0", "ltrim1")
--[[ "ltrim1 ltrim0 hello world a0 a1 rtrim0 rtrim1 rtrim2"  --]]
ardb.call("rpush", "mylist", "rtrim0", "rtrim1", "rtrim2")
local vss = ardb.call("lrange", "mylist", "0", "-1")

--[[ "hello world a0 a1"  --]]
s = ardb.call("ltrim", "mylist", "2", "-4")

ardb.assert2(s["ok"] == "OK", s)

--[[ "hello world a0 a1"  --]]
s = ardb.call("linsert", "mylist", "before", "ax", "ss")
ardb.assert2(s == -1, s)

--[[ "hello ss world a0 a1"  --]]
s = ardb.call("linsert", "mylist", "after", "hello", "ss")
ardb.assert2(s == 5, s)


--[[ "hello ss world a0 a1 zzz"  --]]
ardb.call("rpush", "mylist", "zzz")
ardb.call("lrem", "mylist", "1", "zzz")
--[[ "hello ss world a0 a1 "  --]]
s = ardb.call("lrem", "mylist", "1", "ss")
--[[ "hello  world a0 a1"  --]]
ardb.assert2(s == 1, s)
s = ardb.call("lrem", "mylist", "1", "ax")
ardb.assert2(s == 0, s)
s = ardb.call("lrem", "mylist", "-2", "a0")
ardb.assert2(s == 1, s)
s = ardb.call("lrem", "mylist", "2", "a1")
ardb.assert2(s == 1, s)

--[[ "hello  world "  --]]
s=ardb.call("llen", "mylist")
ardb.assert2(s == 2, s)

--[[ non sequential list lrange  --]]
ardb.assert2(table.getn(vs) == 2, vs)
ardb.assert2(vs[1] == "hello", vs)
ardb.assert2(vs[2] == "world", vs)
--[[ non sequential list lset  --]]
s = ardb.call("lset", "mylist", "1", "y")
ardb.assert2(s["ok"] == "OK", s)
s = ardb.call("lindex", "mylist", "1")
ardb.assert2(s == "y", s)
ardb.call("lset", "mylist", "1", "world")
--[[ non sequential list lindex  --]]
s = ardb.call("lindex", "mylist", "10")
ardb.assert2(s == false, s)
s = ardb.call("lindex", "mylist", "1")
ardb.assert2(s == "world", s)
s = ardb.call("llen", "mylist")
ardb.assert2(s == 2, s)

--[[ non sequential list ltrim  --]]
ardb.call("lpush", "mylist", "ltrim0", "ltrim1", "ltrim2")
ardb.call("rpush", "mylist", "rtrim0", "rtrim1", "rtrim2", "rtrim3")
--[[ "ltrim2 ltrim1 ltrim0 hello  world rtrim0 rtrim1 rtrim2 rtrim3"  --]]
s = ardb.call("ltrim", "mylist", "3", "-5")
ardb.assert2(s["ok"] == "OK", s)

--[[ "hello  world"  --]]
ardb.call("lpush", "mylist", "lpopvaluex")

--[[ non sequential list lpop  --]]
s = ardb.call("lpop", "mylist")
ardb.assert2(s == "lpopvaluex", s)
s = ardb.call("lpop", "mylist")
ardb.assert2(s == "hello", s)
s = ardb.call("lpop", "mylist")
ardb.assert2(s == "world", s)
s = ardb.call("llen", "mylist")
ardb.assert2(s == 0, s)

ardb.call("del", "srclist", "destlist")
ardb.call("rpush", "srclist", "one", "two", "three")
s = ardb.call("rpoplpush", "srclist", "destlist")
ardb.assert2(s == "three", s)
vs = ardb.call("lrange", "srclist", "0", "-1")
ardb.assert2(table.getn(vs) == 2, vs)
ardb.assert2(vs[1] == "one", vs)
ardb.assert2(vs[2] == "two", vs)
vs = ardb.call("lrange", "destlist", "0", "-1")
ardb.assert2(table.getn(vs) == 1, vs)
ardb.assert2(vs[1] == "three", vs)

