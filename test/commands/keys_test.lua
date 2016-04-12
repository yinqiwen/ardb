--[[   --]]
local s = ardb.call("echo", "hello,world")
ardb.assert2(s == "hello,world", s)