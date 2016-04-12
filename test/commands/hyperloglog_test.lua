ardb.call("del", "hll", "some-other-hll")
local s = ardb.call("pfadd", "hll", "foo", "bar", "zap")
ardb.assert2(s == 1, s)
s = ardb.call("pfadd", "hll", "zap", "zap", "zap")
ardb.assert2(s == 0, s)
s = ardb.call("pfadd", "hll", "foo", "bar")
ardb.assert2(s == 0, s)
s = ardb.call("pfcount", "hll")
ardb.assert2(s == 3, s)
s = ardb.call("pfadd2", "some-other-hll", "1", "2", "3")
ardb.assert2(s["ok"] == "OK", s)
s = ardb.call("pfcount", "hll", "some-other-hll")
ardb.assert2(s == 6, s)

ardb.call("del", "hll1", "hll2", "hll3")
ardb.call("pfadd", "hll1", "foo", "bar", "zap", "a")
ardb.call("pfadd", "hll2", "a", "b", "c", "foo")
s = ardb.call("pfmerge", "hll3", "hll1", "hll2")
ardb.assert2(s["ok"] == "OK", s)
s = ardb.call("pfcount", "hll3")
ardb.assert2(s == 6, s)



