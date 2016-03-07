/*
 * hash_testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: yinqiwen
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_hash_common(Context& ctx, Ardb& db)
{
    db.GetConfig().hash_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myhash");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame hset;
        hset.SetFullCommand("hset myhash field%u value%u", i, i);
        db.Call(ctx, hset, 0);
        CHECK_FATAL(ctx.reply.integer != 1, "hset myhash failed");

        RedisCommandFrame hget;
        hget.SetFullCommand("hget myhash field%u", i);
        db.Call(ctx, hget, 0);
        CHECK_FATAL(ctx.reply.str != hset.GetArguments()[2], "hget myhash failed");
    }
    RedisCommandFrame hexist;
    hexist.SetFullCommand("hexists myhash field4");
    db.Call(ctx, hexist, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "hexist myhash failed");
    hexist.SetFullCommand("hexists myhash field400");
    db.Call(ctx, hexist, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "hexist myhash failed");

    RedisCommandFrame hdel;
    hdel.SetFullCommand("hdel myhash yfield5");
    db.Call(ctx, hdel, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "hdel myhash failed");
    hdel.SetFullCommand("hdel myhash field5");
    db.Call(ctx, hdel, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "hdel myhash failed");

    RedisCommandFrame hgetall;
    hgetall.SetFullCommand("hgetall myhash");
    db.Call(ctx, hgetall, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 18, "hgetall myhash failed");

    RedisCommandFrame hkeys;
    hkeys.SetFullCommand("hkeys myhash");
    db.Call(ctx, hkeys, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 9, "hkeys myhash failed");

    RedisCommandFrame hvalues;
    hvalues.SetFullCommand("hvals myhash");
    db.Call(ctx, hvalues, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 9, "hvalues myhash failed");

    RedisCommandFrame hlen;
    hlen.SetFullCommand("hlen myhash");
    db.Call(ctx, hlen, 0);
    CHECK_FATAL(ctx.reply.integer != 9, "hlen myhash failed");

    //non zipmap
    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame hset;
        hset.SetFullCommand("hset myhash xfield%u xvalue%u", i, i);
        db.Call(ctx, hset, 0);
        CHECK_FATAL(ctx.reply.integer != 1, "hset myhash failed");
        RedisCommandFrame hget;
        hget.SetFullCommand("hget myhash xfield%u", i);
        db.Call(ctx, hget, 0);
        CHECK_FATAL(ctx.reply.str != hset.GetArguments()[2], "hget myhash failed");
    }

    hexist.SetFullCommand("hexists myhash xfield4");
    db.Call(ctx, hexist, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "hexist myhash failed");
    hexist.SetFullCommand("hexists myhash xfield400");
    db.Call(ctx, hexist, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "hexist myhash failed");
    hdel.SetFullCommand("hdel myhash yfield5 yfield6");
    db.Call(ctx, hdel, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "hdel myhash failed");
    hdel.SetFullCommand("hdel myhash xfield5 xfield6");
    db.Call(ctx, hdel, 0);
    CHECK_FATAL(ctx.reply.integer != 2, "hdel myhash failed");

    db.Call(ctx, hgetall, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 34, "hgetall myhash failed");
    db.Call(ctx, hkeys, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 17, "hkeys myhash failed");
    db.Call(ctx, hvalues, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 17, "hvalues myhash failed");
    db.Call(ctx, hlen, 0);
    CHECK_FATAL(ctx.reply.integer != 17, "hlen myhash failed");
}

void test_hash_incr(Context& ctx, Ardb& db)
{
    db.GetConfig().hash_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myhash");
    db.Call(ctx, del, 0);

    //incr on not exist key
    RedisCommandFrame incr;
    incr.SetFullCommand("HINCRBY myhash filed 11");
    db.Call(ctx, incr, 0);
    CHECK_FATAL(ctx.reply.integer != 11, "HINCRBY myhash failed");
    RedisCommandFrame hget;
    hget.SetFullCommand("hget myhash filed");
    db.Call(ctx, hget, 0);
    CHECK_FATAL(ctx.reply.str != "11", "hget myhash failed");
    db.Call(ctx, incr, 0);
    CHECK_FATAL(ctx.reply.integer != 22, "HINCRBY myhash failed");;
    db.Call(ctx, hget, 0);
    CHECK_FATAL(ctx.reply.str != "22", "hget myhash failed");

    RedisCommandFrame incrbyfloat;
    incrbyfloat.SetFullCommand("hincrbyfloat myhash ffiled 10.1");
    db.Call(ctx, incrbyfloat, 0);
    CHECK_FATAL(ctx.reply.str != "10.1", "hincrbyfloat myhash failed");
    hget.SetFullCommand("hget myhash ffiled");
    db.Call(ctx, hget, 0);
    CHECK_FATAL(ctx.reply.str != "10.1", "hget myhash failed");
    db.Call(ctx, incrbyfloat, 0);
    CHECK_FATAL(ctx.reply.str != "20.2", "incrbyfloat myhash failed");;
    db.Call(ctx, hget, 0);
    CHECK_FATAL(ctx.reply.str != "20.2", "hget myhash failed");

    for (uint32 i = 0; i < 20; i++)
    {
        RedisCommandFrame hset;
        hset.SetFullCommand("hset myhash xfield%u xvalue%u", i, i);
        db.Call(ctx, hset, 0);
    }
    incr.SetFullCommand("HINCRBY myhash filedx 11");
    db.Call(ctx, incr, 0);
    CHECK_FATAL(ctx.reply.integer != 11, "HINCRBY myhash failed");
    hget.SetFullCommand("hget myhash filedx");
    db.Call(ctx, hget, 0);
    CHECK_FATAL(ctx.reply.str != "11", "hget myhash failed");
    db.Call(ctx, incr, 0);
    CHECK_FATAL(ctx.reply.integer != 22, "HINCRBY myhash failed");;
    db.Call(ctx, hget, 0);
    CHECK_FATAL(ctx.reply.str != "22", "hget myhash failed");

    incrbyfloat.SetFullCommand("hincrbyfloat myhash xffiled 10.1");
    db.Call(ctx, incrbyfloat, 0);
    CHECK_FATAL(ctx.reply.str != "10.1", "hincrbyfloat myhash failed");
    hget.SetFullCommand("hget myhash xffiled");
    db.Call(ctx, hget, 0);
    CHECK_FATAL(ctx.reply.str != "10.1", "hget myhash failed");
    db.Call(ctx, incrbyfloat, 0);
    CHECK_FATAL(ctx.reply.str != "20.2", "incrbyfloat myhash failed");;
    db.Call(ctx, hget, 0);
    CHECK_FATAL(ctx.reply.str != "20.2", "hget myhash failed");
}

void test_hash_mgetset(Context& ctx, Ardb& db)
{
    db.GetConfig().hash_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myhash");
    db.Call(ctx, del, 0);

    //incr on not exist key
    RedisCommandFrame hmset;
    hmset.SetFullCommand("hmset myhash filed0 0 field1 1");
    db.Call(ctx, hmset, 0);
    CHECK_FATAL(ctx.reply.str != "OK", "hmset myhash failed");

    RedisCommandFrame hmget;
    hmget.SetFullCommand("hmget myhash filed0 field10");
    db.Call(ctx, hmget, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 2, "hmget myhash failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).type != REDIS_REPLY_NIL, "hmget myhash failed");

    for (uint32 i = 0; i < 20; i++)
    {
        RedisCommandFrame hset;
        hset.SetFullCommand("hset myhash xfield%u xvalue%u", i, i);
        db.Call(ctx, hset, 0);
    }
    db.Call(ctx, hmset, 0);
    CHECK_FATAL(ctx.reply.str != "OK", "hmset myhash failed");
    db.Call(ctx, hmget, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 2, "hmget myhash failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).type != REDIS_REPLY_NIL, "hmget myhash failed");
}

void test_hash_setnx(Context& ctx, Ardb& db)
{
    db.GetConfig().hash_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myhash");
    db.Call(ctx, del, 0);

    //incr on not exist key
    RedisCommandFrame hsetnx;
    hsetnx.SetFullCommand("hsetnx myhash filed0 0");
    db.Call(ctx, hsetnx, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "hsetnx myhash failed");
    db.Call(ctx, hsetnx, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "hsetnx myhash failed");

    for (uint32 i = 0; i < 20; i++)
    {
        RedisCommandFrame hset;
        hset.SetFullCommand("hset myhash xfield%u xvalue%u", i, i);
        db.Call(ctx, hset, 0);
    }
    hsetnx.SetFullCommand("hsetnx myhash zfiled0 0");
    db.Call(ctx, hsetnx, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "hsetnx myhash failed");
    db.Call(ctx, hsetnx, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "hsetnx myhash failed");
}

void test_hash(Ardb& db)
{
    Context tmpctx;
    test_hash_common(tmpctx, db);
    test_hash_incr(tmpctx, db);
    test_hash_mgetset(tmpctx, db);
    test_hash_setnx(tmpctx, db);
}

