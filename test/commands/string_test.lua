--[[  GET/SET test --]]
local s = ardb.call("set", "k0", "v0")
ardb.assert2(s["ok"] == "OK", s)
local v = ardb.call("get", "k0")
ardb.assert2(v == "v0", v)
ardb.call("set", "k1", "100")
v = ardb.call("get", "k1")
ardb.assert2(v == "100", v)
v = ardb.call("get", "not_exist_key")
ardb.assert2( v == false, v)

--[[ Merge set test --]]
s = ardb.call("set2", "merge_k0", "1")
ardb.assert2(s["ok"] == "OK", s)
v = ardb.call("get", "merge_k0")
ardb.assert2(v == "1", v)

--[[  MGET/MSET test --]]
s = ardb.call("mset", "k2", "v2", "k3", "v3")
ardb.assert2(s["ok"] == "OK", s)
local vs = ardb.call("mget", "k2", "k3")
ardb.assert2(vs[1] == "v2", vs)
ardb.assert2(vs[2] == "v3", vs)
vs = ardb.call("mget", "not_exist_key", "k3")
ardb.assert2(vs[1] == false, vs)
ardb.assert2(vs[2] == "v3", vs)

