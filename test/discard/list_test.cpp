/*
 * lists_testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: yinqiwen
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_lists_push(Context& ctx, Ardb& db)
{
    db.GetConfig().list_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del mylist");
    db.Call(ctx, del, 0);
    //ziplist push
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist value%d", i);
        db.Call(ctx, lpush, 0);
        CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "lpush mylist failed");
        CHECK_FATAL(ctx.reply.integer != i + 1, "lpush mylist failed");
    }
    RedisCommandFrame mlpush;
    mlpush.SetFullCommand("lpush mylist value10 value11 value12");
    db.Call(ctx, mlpush, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "lpush mylist failed");
    CHECK_FATAL(ctx.reply.integer != 13, "lpush mylist failed");

    //non ziplist lpush
    for (int i = 13; i < db.GetConfig().list_max_ziplist_entries + 100; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist value%d", i);
        db.Call(ctx, lpush, 0);
        CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "lpush mylist failed");
        CHECK_FATAL(ctx.reply.integer != i + 1, "lpush mylist failed");
    }

    db.Call(ctx, del, 0);
    //ziplist push
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame rpush;
        rpush.SetFullCommand("rpush mylist value%d", i);
        db.Call(ctx, rpush, 0);
        CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "rpush mylist failed");
        CHECK_FATAL(ctx.reply.integer != i + 1, "rpush mylist failed");
    }
    RedisCommandFrame mrpush;
    mrpush.SetFullCommand("rpush mylist value10 value11 value12");
    db.Call(ctx, mrpush, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "rpush mylist failed");
    CHECK_FATAL(ctx.reply.integer != 13, "rpush mylist failed");

    //non ziplist lpush
    for (int i = 13; i < db.GetConfig().list_max_ziplist_entries + 100; i++)
    {
        RedisCommandFrame rpush;
        rpush.SetFullCommand("rpush mylist value%d", i);
        db.Call(ctx, rpush, 0);
        CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "rpush mylist failed");
        CHECK_FATAL(ctx.reply.integer != i + 1, "rpush mylist failed");
    }
}

void test_lists_pushx(Context& ctx, Ardb& db)
{
    db.GetConfig().list_max_ziplist_entries = 10;
    RedisCommandFrame del;
    del.SetFullCommand("del mylist");
    db.Call(ctx, del, 0);
    RedisCommandFrame lpushx;
    lpushx.SetFullCommand("lpushx mylist value");
    db.Call(ctx, lpushx, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "lpushx mylist failed");
    CHECK_FATAL(ctx.reply.integer != 0, "lpushx mylist failed");
    RedisCommandFrame rpushx;
    rpushx.SetFullCommand("rpushx mylist value");
    db.Call(ctx, rpushx, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "rpushx mylist failed");
    CHECK_FATAL(ctx.reply.integer != 0, "rpushx mylist failed");

    RedisCommandFrame mlpush;
    mlpush.SetFullCommand(
            "lpush mylist value10 value11 value12 value13 value10 value10 value10 value10 value10 value10");
    db.Call(ctx, mlpush, 0);
    db.Call(ctx, lpushx, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "lpushx mylist failed");
    CHECK_FATAL(ctx.reply.integer != 11, "lpushx mylist failed");
    db.Call(ctx, rpushx, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "rpushx mylist failed");
    CHECK_FATAL(ctx.reply.integer != 12, "rpushx mylist failed");
}

void test_lists_index(Context& ctx, Ardb& db)
{
    db.GetConfig().list_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del mylist");
    db.Call(ctx, del, 0);
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist value%d", i);
        db.Call(ctx, lpush, 0);
    }
    RedisCommandFrame index;
    index.SetFullCommand("lindex mylist 5");
    db.Call(ctx, index, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "lindex mylist failed");
    CHECK_FATAL(ctx.reply.str != "value4", "lindex mylist failed");
    index.SetFullCommand("lindex mylist 50");
    db.Call(ctx, index, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_NIL, "lindex mylist failed");

    //now list is not ziplist
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist value%d", i);
        db.Call(ctx, lpush, 0);
    }
    index.SetFullCommand("lindex mylist 15");
    db.Call(ctx, index, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "lindex mylist failed");
    CHECK_FATAL(ctx.reply.str != "value4", "lindex mylist failed");
    index.SetFullCommand("lindex mylist 50");
    db.Call(ctx, index, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_NIL, "lindex mylist failed");
}

void test_lists_insert(Context& ctx, Ardb& db)
{
    db.GetConfig().list_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del mylist");
    db.Call(ctx, del, 0);
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame rpush;
        rpush.SetFullCommand("rpush mylist value%d", i);
        db.Call(ctx, rpush, 0);
    }
    RedisCommandFrame insert;
    insert.SetFullCommand("linsert mylist before value3 valuexx");
    db.Call(ctx, insert, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "linsert mylist failed");
    CHECK_FATAL(ctx.reply.integer != 11, "linsert mylist failed");

    RedisCommandFrame index;
    index.SetFullCommand("lindex mylist 3");
    db.Call(ctx, index, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "lindex mylist failed");
    CHECK_FATAL(ctx.reply.str != "valuexx", "lindex mylist failed");

    //now list is not ziplist
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame rpush;
        rpush.SetFullCommand("rpush mylist xvalue%d", i);
        db.Call(ctx, rpush, 0);
    }
    insert.SetFullCommand("linsert mylist after xvalue3 valueyy");
    db.Call(ctx, insert, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "linsert mylist failed");
    CHECK_FATAL(ctx.reply.integer != 22, "linsert mylist failed");
    index.SetFullCommand("lindex mylist 14");
    db.Call(ctx, index, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "lindex mylist failed");
    CHECK_FATAL(ctx.reply.str != "valueyy", "lindex mylist failed");
}

void test_lists_lrange(Context& ctx, Ardb& db)
{
    db.GetConfig().list_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del mylist");
    db.Call(ctx, del, 0);
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame rpush;
        rpush.SetFullCommand("rpush mylist value%d", i);
        db.Call(ctx, rpush, 0);
    }
    RedisCommandFrame lrange;
    lrange.SetFullCommand("lrange mylist 0 -1");
    db.Call(ctx, lrange, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 10, "lrange mylist failed");
    CHECK_FATAL(ctx.reply.MemberAt(4).str != "value4", "lrange mylist failed");

    //now list is not ziplist
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame rpush;
        rpush.SetFullCommand("rpush mylist xvalue%d", i);
        db.Call(ctx, rpush, 0);
    }
    lrange.SetFullCommand("lrange mylist 0 -1");
    db.Call(ctx, lrange, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 20, "lrange mylist failed");

    lrange.SetFullCommand("lrange mylist 10 12");
    db.Call(ctx, lrange, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 3, "lrange mylist failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "xvalue0", "lrange mylist failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "xvalue1", "lrange mylist failed");
    CHECK_FATAL(ctx.reply.MemberAt(2).str != "xvalue2", "lrange mylist failed");
}

void test_lists_pop(Context& ctx, Ardb& db)
{
    db.GetConfig().list_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del mylist");
    db.Call(ctx, del, 0);
    //ziplist push
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist value%d", i);
        db.Call(ctx, lpush, 0);
    }
    RedisCommandFrame lpop;
    lpop.SetFullCommand("lpop mylist");
    db.Call(ctx, lpop, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "lpop mylist failed");
    CHECK_FATAL(ctx.reply.str != "value9", "lpop mylist failed");

    RedisCommandFrame rpop;
    rpop.SetFullCommand("rpop mylist");
    db.Call(ctx, rpop, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "rpop mylist failed");
    CHECK_FATAL(ctx.reply.str != "value0", "rpop mylist failed");

    //non ziplist lpush
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist xvalue%d", i);
        db.Call(ctx, lpush, 0);
    }

    db.Call(ctx, lpop, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "lpop mylist failed");
    CHECK_FATAL(ctx.reply.str != "xvalue9", "lpop mylist failed");
    db.Call(ctx, rpop, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "rpop mylist failed");
    CHECK_FATAL(ctx.reply.str != "value1", "rpop mylist failed");
}

void test_lists_llen(Context& ctx, Ardb& db)
{
    db.GetConfig().list_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del mylist");
    db.Call(ctx, del, 0);
    //ziplist push
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist value%d", i);
        db.Call(ctx, lpush, 0);
    }
    RedisCommandFrame llen;
    llen.SetFullCommand("llen mylist");
    db.Call(ctx, llen, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "llen mylist failed");
    CHECK_FATAL(ctx.reply.integer != 10, "llen mylist failed");

    //non ziplist lpush
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist xvalue%d", i);
        db.Call(ctx, lpush, 0);
    }
    db.Call(ctx, llen, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "llen mylist failed");
    CHECK_FATAL(ctx.reply.integer != 20, "llen mylist failed");
}

void test_lists_lrem(Context& ctx, Ardb& db)
{
    db.GetConfig().list_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del mylist");
    db.Call(ctx, del, 0);
    //ziplist push
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist valuexx");
        db.Call(ctx, lpush, 0);
    }
    RedisCommandFrame lrem;
    lrem.SetFullCommand("lrem mylist 2 valueyy");
    db.Call(ctx, lrem, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "lrem mylist failed");
    CHECK_FATAL(ctx.reply.integer != 0, "lrem mylist failed");
    lrem.SetFullCommand("lrem mylist -2 valuexx");
    db.Call(ctx, lrem, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "lrem mylist failed");
    CHECK_FATAL(ctx.reply.integer != 2, "lrem mylist failed");
    lrem.SetFullCommand("lrem mylist 0 valuexx");
    db.Call(ctx, lrem, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "lrem mylist failed");
    CHECK_FATAL(ctx.reply.integer != 8, "lrem mylist failed");

    //non ziplist lpush
    for (int i = 0; i < 20; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist valuexx");
        db.Call(ctx, lpush, 0);
    }
    lrem.SetFullCommand("lrem mylist -2 valueyy");
    db.Call(ctx, lrem, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "lrem mylist failed");
    CHECK_FATAL(ctx.reply.integer != 0, "lrem mylist failed");
    lrem.SetFullCommand("lrem mylist -2 valuexx");
    db.Call(ctx, lrem, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "lrem mylist failed");
    CHECK_FATAL(ctx.reply.integer != 2, "lrem mylist failed");
    lrem.SetFullCommand("lrem mylist 0 valuexx");
    db.Call(ctx, lrem, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "lrem mylist failed");
    CHECK_FATAL(ctx.reply.integer != 18, "lrem mylist failed");
}

void test_lists_lset(Context& ctx, Ardb& db)
{
    db.GetConfig().list_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del mylist");
    db.Call(ctx, del, 0);
    //ziplist push
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist value%d", i);
        db.Call(ctx, lpush, 0);
    }
    RedisCommandFrame lset;
    lset.SetFullCommand("lset mylist 4 abcd");
    db.Call(ctx, lset, 0);
    RedisCommandFrame index;
    index.SetFullCommand("lindex mylist 4");
    db.Call(ctx, index, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "lindex mylist failed");
    CHECK_FATAL(ctx.reply.str != "abcd", "lindex mylist failed");

    //non ziplist lpush
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist xvalue%d", i);
        db.Call(ctx, lpush, 0);
    }
    lset.SetFullCommand("lset mylist -5 xxyy");
    db.Call(ctx, lset, 0);
    index.SetFullCommand("lindex mylist -5");
    db.Call(ctx, index, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "lindex mylist failed");
    CHECK_FATAL(ctx.reply.str != "xxyy", "lindex mylist failed");

}

void test_lists_ltrim(Context& ctx, Ardb& db)
{
    db.GetConfig().list_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del mylist");
    db.Call(ctx, del, 0);
    //ziplist push
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist value%d", i);
        db.Call(ctx, lpush, 0);
    }
    RedisCommandFrame ltrim;
    ltrim.SetFullCommand("ltrim mylist 2 5");
    db.Call(ctx, ltrim, 0);
    RedisCommandFrame llen;
    llen.SetFullCommand("llen mylist");
    db.Call(ctx, llen, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "llen mylist failed");
    CHECK_FATAL(ctx.reply.integer != 4, "llen mylist failed");

    db.Call(ctx, del, 0);
    //non ziplist lpush
    for (int i = 0; i < 20; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist xvalue%d", i);
        db.Call(ctx, lpush, 0);
    }
    ltrim.SetFullCommand("ltrim mylist 18 -1");
    db.Call(ctx, ltrim, 0);
    db.Call(ctx, llen, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "llen mylist failed");
    CHECK_FATAL(ctx.reply.integer != 2, "llen mylist failed");
}

void test_lists_rpoplpush(Context& ctx, Ardb& db)
{
    db.GetConfig().list_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del mylist mylist1");
    db.Call(ctx, del, 0);
    //ziplist push
    for (int i = 0; i < 10; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist value%d", i);
        db.Call(ctx, lpush, 0);
    }
    RedisCommandFrame rpoplpush;
    rpoplpush.SetFullCommand("rpoplpush mylist mylist1");
    db.Call(ctx, rpoplpush, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "rpoplpush mylist failed");
    CHECK_FATAL(ctx.reply.str != "value0", "rpoplpush mylist failed");

    //non ziplist lpush
    for (int i = 0; i < 20; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist xvalue%d", i);
        db.Call(ctx, lpush, 0);
    }
    db.Call(ctx, rpoplpush, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "rpoplpush mylist failed");
    CHECK_FATAL(ctx.reply.str != "value1", "rpoplpush mylist failed");
}

void test_sequential_list(Context& ctx, Ardb& db)
{
    db.GetConfig().list_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del mylist");
    db.Call(ctx, del, 0);

    for (int i = 0; i < 10000; i++)
    {
        RedisCommandFrame lpush;
        lpush.SetFullCommand("lpush mylist xvalue%d", i);
        db.Call(ctx, lpush, 0);
    }
    for (int i = 0; i < 10000; i++)
    {
        RedisCommandFrame rpush;
        rpush.SetFullCommand("rpush mylist yvalue%d", i);
        db.Call(ctx, rpush, 0);
    }
    RedisCommandFrame index;
    index.SetFullCommand("lindex mylist 10001");
    uint64 start = get_current_epoch_millis();
    db.Call(ctx, index, 0);
    uint64 end = get_current_epoch_millis();
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "lindex mylist failed");
    CHECK_FATAL(ctx.reply.str != "yvalue1", "lindex mylist failed");
    CHECK_FATAL((end - start) > 100, "lindex too slow for sequential list");

    RedisCommandFrame lrange;
    lrange.SetFullCommand("lrange mylist 10000 10005");
    start = get_current_epoch_millis();
    db.Call(ctx, lrange, 0);
    end = get_current_epoch_millis();
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_ARRAY, "lrange mylist failed");
    CHECK_FATAL(ctx.reply.MemberSize() != 6, "lrange mylist failed");
    CHECK_FATAL(ctx.reply.MemberAt(5).str != "yvalue5", "lrange mylist failed");
    CHECK_FATAL((end - start) > 100, "lrange too slow for sequential list");

}

void test_list(Ardb& db)
{
    Context tmpctx;
    test_lists_push(tmpctx, db);
    test_lists_pushx(tmpctx, db);
    test_lists_index(tmpctx, db);
    test_lists_lrange(tmpctx, db);
    test_lists_pop(tmpctx, db);
    test_lists_llen(tmpctx, db);
    test_lists_lrem(tmpctx, db);
    test_lists_lset(tmpctx, db);
    test_lists_ltrim(tmpctx, db);
    test_lists_rpoplpush(tmpctx, db);
    test_sequential_list(tmpctx, db);
}

