/*
 * keys_test.cpp
 *
 *  Created on: 2014Äê8ÔÂ5ÈÕ
 *      Author: wangqiying
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_keys_exists(Context& ctx, Ardb& db)
{
    RedisCommandFrame del;
    del.SetFullCommand("del mystring mylist myset myhash myzset mybitset");
    db.Call(ctx, del, 0);

    RedisCommandFrame exists;
    exists.SetFullCommand("exists mystring");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 0 || ctx.reply.type != REDIS_REPLY_INTEGER, "exists failed");
    exists.SetFullCommand("exists mylist");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 0 || ctx.reply.type != REDIS_REPLY_INTEGER, "exists failed");
    exists.SetFullCommand("exists myset");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 0 || ctx.reply.type != REDIS_REPLY_INTEGER, "exists failed");
    exists.SetFullCommand("exists myset");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 0 || ctx.reply.type != REDIS_REPLY_INTEGER, "exists failed");
    exists.SetFullCommand("exists myzset");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 0 || ctx.reply.type != REDIS_REPLY_INTEGER, "exists failed");
    exists.SetFullCommand("exists myhash");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 0 || ctx.reply.type != REDIS_REPLY_INTEGER, "exists failed");
    exists.SetFullCommand("exists mybitset");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 0 || ctx.reply.type != REDIS_REPLY_INTEGER, "exists failed");

    RedisCommandFrame set;
    set.SetFullCommand("set mystring 123");
    db.Call(ctx, set, 0);
    set.SetFullCommand("lpush mylist 123");
    db.Call(ctx, set, 0);
    set.SetFullCommand("hset myhash  f 123");
    db.Call(ctx, set, 0);
    set.SetFullCommand("zadd myzset 123 f");
    db.Call(ctx, set, 0);
    set.SetFullCommand("sadd myset 123");
    db.Call(ctx, set, 0);
    set.SetFullCommand("setbit mybitset 123 1");
    db.Call(ctx, set, 0);
    exists.SetFullCommand("exists mystring");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "exists failed");
    exists.SetFullCommand("exists mylist");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "exists failed");
    exists.SetFullCommand("exists myset");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "exists failed");
    exists.SetFullCommand("exists myset");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "exists failed");
    exists.SetFullCommand("exists myzset");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "exists failed");
    exists.SetFullCommand("exists myhash");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "exists failed");
    exists.SetFullCommand("exists mybitset");
    db.Call(ctx, exists, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "exists failed");
}

void test_keys_expire(Context& ctx, Ardb& db)
{
    RedisCommandFrame del;
    del.SetFullCommand("del mystring mylist myset myhash myzset mybitset");
    db.Call(ctx, del, 0);

    RedisCommandFrame set;
    set.SetFullCommand("set mystring 123");
    db.Call(ctx, set, 0);
    set.SetFullCommand("lpush mylist 123");
    db.Call(ctx, set, 0);
    set.SetFullCommand("hset myhash  f 123");
    db.Call(ctx, set, 0);
    set.SetFullCommand("zadd myzset 123 f");
    db.Call(ctx, set, 0);
    set.SetFullCommand("sadd myset 123");
    db.Call(ctx, set, 0);
    set.SetFullCommand("setbit mybitset 123 1");
    db.Call(ctx, set, 0);

    RedisCommandFrame expire;
    expire.SetFullCommand("expire mystring 5");
    db.Call(ctx, expire, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "expire failed");
    expire.SetFullCommand("expire mylist 5");
    db.Call(ctx, expire, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "expire failed");
    expire.SetFullCommand("expire myhash 5");
    db.Call(ctx, expire, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "expire failed");
    expire.SetFullCommand("expire myzset 5");
    db.Call(ctx, expire, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "expire failed");
    expire.SetFullCommand("expire myset 5");
    db.Call(ctx, expire, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "expire failed");
    expire.SetFullCommand("expire mybitset 5");
    db.Call(ctx, expire, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "expire failed");

    RedisCommandFrame ttl;
    ttl.SetFullCommand("ttl mystring");
    db.Call(ctx, ttl, 0);
    CHECK_FATAL(ctx.reply.integer > 4 && ctx.reply.integer <= 5, "ttl failed");
    ttl.SetFullCommand("ttl mylist");
    db.Call(ctx, ttl, 0);
    CHECK_FATAL(ctx.reply.integer > 4 && ctx.reply.integer <= 5, "ttl failed");
    ttl.SetFullCommand("ttl myhash");
    db.Call(ctx, ttl, 0);
    CHECK_FATAL(ctx.reply.integer > 4 && ctx.reply.integer <= 5, "ttl failed");
    ttl.SetFullCommand("ttl myset");
    db.Call(ctx, ttl, 0);
    CHECK_FATAL(ctx.reply.integer > 4 && ctx.reply.integer <= 5, "ttl failed");
    ttl.SetFullCommand("ttl myzset");
    db.Call(ctx, ttl, 0);
    CHECK_FATAL(ctx.reply.integer > 4 && ctx.reply.integer <= 5, "ttl failed");
    ttl.SetFullCommand("ttl mybitset");
    db.Call(ctx, ttl, 0);
    CHECK_FATAL(ctx.reply.integer > 4 && ctx.reply.integer <= 5, "ttl failed");

    RedisCommandFrame pttl;
    pttl.SetFullCommand("pttl mystring");
    db.Call(ctx, pttl, 0);
    CHECK_FATAL(ctx.reply.integer > 4000 && ctx.reply.integer <= 5000, "ttl failed");
    pttl.SetFullCommand("pttl mylist");
    db.Call(ctx, pttl, 0);
    CHECK_FATAL(ctx.reply.integer > 4000 && ctx.reply.integer <= 5000, "ttl failed");
    pttl.SetFullCommand("pttl myhash");
    db.Call(ctx, ttl, 0);
    CHECK_FATAL(ctx.reply.integer > 4000 && ctx.reply.integer <= 5000, "ttl failed");
    pttl.SetFullCommand("pttl myset");
    db.Call(ctx, pttl, 0);
    CHECK_FATAL(ctx.reply.integer > 4000 && ctx.reply.integer <= 5000, "ttl failed");
    pttl.SetFullCommand("pttl myzset");
    db.Call(ctx, pttl, 0);
    CHECK_FATAL(ctx.reply.integer > 4000 && ctx.reply.integer <= 5000, "ttl failed");
    pttl.SetFullCommand("pttl mybitset");
    db.Call(ctx, pttl, 0);
    CHECK_FATAL(ctx.reply.integer > 4000 && ctx.reply.integer <= 5000, "ttl failed");
}

void test_keys_rename(Context& ctx, Ardb& db)
{
    RedisCommandFrame del;
    del.SetFullCommand(
            "del mystring mylist myset myhash myzset mybitset mystring1 myset1 myhash1 mylist1 myset1 myzset1 mybitset1");
    db.Call(ctx, del, 0);
    RedisCommandFrame set;
    set.SetFullCommand("set mystring 123");
    db.Call(ctx, set, 0);
    set.SetFullCommand("lpush mylist 123");
    db.Call(ctx, set, 0);
    set.SetFullCommand("hset myhash  f 123");
    db.Call(ctx, set, 0);
    set.SetFullCommand("zadd myzset 123 f");
    db.Call(ctx, set, 0);
    set.SetFullCommand("sadd myset 123");
    db.Call(ctx, set, 0);
    set.SetFullCommand("setbit mybitset 123 1");
    db.Call(ctx, set, 0);

    RedisCommandFrame rename;
    rename.SetFullCommand("renamenx mystring mystring1");
    db.Call(ctx, rename, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "rename failed");
    rename.SetFullCommand("renamenx myset myset1");
    db.Call(ctx, rename, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "rename failed");
    rename.SetFullCommand("renamenx myhash myhash1");
    db.Call(ctx, rename, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "rename failed");
    rename.SetFullCommand("renamenx mylist mylist1");
    db.Call(ctx, rename, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "rename failed");
    rename.SetFullCommand("renamenx myzset myzset1");
    db.Call(ctx, rename, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "rename failed");
    rename.SetFullCommand("renamenx mybitset mybitset1");
    db.Call(ctx, rename, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "rename failed");
}

void test_keys(Ardb& db)
{
    Context tmpctx;
    test_keys_exists(tmpctx, db);
    test_keys_expire(tmpctx, db);
}

