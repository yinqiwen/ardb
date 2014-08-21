/*
 * sets_testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: yinqiwen
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_set_common(Context& ctx, Ardb& db)
{
    db.GetConfig().set_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myset");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame sadd;
        sadd.SetFullCommand("sadd myset field%u", i);
        db.Call(ctx, sadd, 0);
        CHECK_FATAL(ctx.reply.integer != 1, "sadd myset failed");
    }
    RedisCommandFrame sadd;
    sadd.SetFullCommand("sadd myset field0 field1 nvalue1 nvalue2");
    db.Call(ctx, sadd, 0);
    CHECK_FATAL(ctx.reply.integer != 2, "sadd myset failed");
    RedisCommandFrame scard;
    scard.SetFullCommand("scard myset");
    db.Call(ctx, scard, 0);
    CHECK_FATAL(ctx.reply.integer != 12, "scard myset failed");
    RedisCommandFrame ismember;
    ismember.SetFullCommand("sismember myset xxxx");
    db.Call(ctx, ismember, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "sismember myset failed");
    ismember.SetFullCommand("sismember myset nvalue2");
    db.Call(ctx, ismember, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "sismember myset failed");
    RedisCommandFrame smembers;
    smembers.SetFullCommand("smembers myset");
    db.Call(ctx, smembers, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 12, "smembers myset failed");

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame sadd;
        sadd.SetFullCommand("sadd myset nfield%u", i);
        db.Call(ctx, sadd, 0);
        CHECK_FATAL(ctx.reply.integer != 1, "sadd myset failed");
    }
    db.Call(ctx, scard, 0);
    CHECK_FATAL(ctx.reply.integer != 22, "scard myset failed");
    ismember.SetFullCommand("sismember myset xxxx");
    db.Call(ctx, ismember, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "sismember myset failed");
    ismember.SetFullCommand("sismember myset nvalue2");
    db.Call(ctx, ismember, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "sismember myset failed");
    db.Call(ctx, smembers, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 22, "smembers myset failed");
}

void test_set_remove(Context& ctx, Ardb& db)
{
    db.GetConfig().set_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myset");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame sadd;
        sadd.SetFullCommand("sadd myset field%u", i);
        db.Call(ctx, sadd, 0);
    }
    RedisCommandFrame spop;
    spop.SetFullCommand("spop myset");
    db.Call(ctx, spop, 0);
    RedisCommandFrame scard;
    scard.SetFullCommand("scard myset");
    db.Call(ctx, scard, 0);
    CHECK_FATAL(ctx.reply.integer != 9, "scard myhash failed");
    RedisCommandFrame srem;
    srem.SetFullCommand("srem myset bbbb cccc");
    db.Call(ctx, srem, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "srem myhash failed");
    srem.SetFullCommand("srem myset field5 field6");
    db.Call(ctx, srem, 0);
    CHECK_FATAL(ctx.reply.integer != 2, "srem myhash failed");
    db.Call(ctx, scard, 0);
    CHECK_FATAL(ctx.reply.integer != 7, "scard myhash failed");

    for (uint32 i = 0; i < 20; i++)
    {
        RedisCommandFrame sadd;
        sadd.SetFullCommand("sadd myset zfield%u", i);
        db.Call(ctx, sadd, 0);
    }
    db.Call(ctx, spop, 0);
    db.Call(ctx, scard, 0);
    CHECK_FATAL(ctx.reply.integer != 26, "scard myhash failed");
    srem.SetFullCommand("srem myset bbbb cccc");
    db.Call(ctx, srem, 0);
    CHECK_FATAL(ctx.reply.integer != 2, "srem myhash failed");
    srem.SetFullCommand("srem myset zfield5 zfield6");
    db.Call(ctx, srem, 0);
    CHECK_FATAL(ctx.reply.integer != 2, "srem myhash failed");
    db.Call(ctx, scard, 0);
    CHECK_FATAL(ctx.reply.integer != 24, "scard myhash failed");

    RedisCommandFrame del1;
    del1.SetFullCommand("del myset1");
    db.Call(ctx, del1, 0);
    RedisCommandFrame move;
    move.SetFullCommand("smove myset myset1 aaaaaaaa");
    db.Call(ctx, move, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "smove myhash failed");
    move.SetFullCommand("smove myset myset1 zfield10");
    db.Call(ctx, move, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "smove myhash failed");
    RedisCommandFrame ismember;
    ismember.SetFullCommand("sismember myset zfield10");
    db.Call(ctx, ismember, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "sismember myhash failed");
    ismember.SetFullCommand("sismember myset1 zfield10");
    db.Call(ctx, ismember, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "sismember myhash failed");
}

void test_set_union(Context& ctx, Ardb& db)
{
    db.GetConfig().set_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myset1 myset2 myset3 myset4");
    db.Call(ctx, del, 0);

    RedisCommandFrame sadd1, sadd2, sadd3;
    sadd1.SetFullCommand("sadd myset1 a b c d");
    sadd2.SetFullCommand("sadd myset2 c");
    sadd3.SetFullCommand("sadd myset3 a c e");
    db.Call(ctx, sadd1, 0);
    db.Call(ctx, sadd2, 0);
    db.Call(ctx, sadd3, 0);
    RedisCommandFrame sunion;
    sunion.SetFullCommand("sunion myset1 myset2 myset3");
    db.Call(ctx, sunion, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 5, "sunion failed");
    sunion.SetFullCommand("sunioncount myset1 myset2 myset3");
    db.Call(ctx, sunion, 0);
    CHECK_FATAL(ctx.reply.integer != 5, "sunion failed");
    sunion.SetFullCommand("sunionstore myset4 myset1 myset2 myset3");
    db.Call(ctx, sunion, 0);
    CHECK_FATAL(ctx.reply.integer != 5, "sunion failed");
    RedisCommandFrame scard;
    scard.SetFullCommand("scard myset4");
    db.Call(ctx, scard, 0);
    CHECK_FATAL(ctx.reply.integer != 5, "scard failed");

    for (uint32 i = 0; i < 50; i++)
    {
        RedisCommandFrame sadd;
        sadd.SetFullCommand("sadd myset1 %u", i);
        db.Call(ctx, sadd, 0);
        sadd.SetFullCommand("sadd myset3 %u", i);
        db.Call(ctx, sadd, 0);
    }
    sunion.SetFullCommand("sunion myset1 myset2 myset3");
    db.Call(ctx, sunion, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 55, "sunion failed");
    sunion.SetFullCommand("sunioncount myset1 myset2 myset3");
    db.Call(ctx, sunion, 0);
    CHECK_FATAL(ctx.reply.integer != 55, "sunion failed");
    sunion.SetFullCommand("sunionstore myset4 myset1 myset2 myset3");
    db.Call(ctx, sunion, 0);
    CHECK_FATAL(ctx.reply.integer != 55, "sunion failed");
    db.Call(ctx, scard, 0);
    CHECK_FATAL(ctx.reply.integer != 55, "scard failed");
}

void test_set_inter(Context& ctx, Ardb& db)
{
    db.GetConfig().set_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myset1 myset2 myset3 myset4");
    db.Call(ctx, del, 0);

    RedisCommandFrame sadd1, sadd2, sadd3;
    sadd1.SetFullCommand("sadd myset1 a b c d");
    sadd2.SetFullCommand("sadd myset2 c");
    sadd3.SetFullCommand("sadd myset3 a c e");
    db.Call(ctx, sadd1, 0);
    db.Call(ctx, sadd2, 0);
    db.Call(ctx, sadd3, 0);
    RedisCommandFrame sinter;
    sinter.SetFullCommand("sinter myset1 myset2 myset3");
    db.Call(ctx, sinter, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 1, "sinter failed");
    sinter.SetFullCommand("sintercount myset1 myset2 myset3");
    db.Call(ctx, sinter, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "sinter failed");
    sinter.SetFullCommand("sinterstore myset4 myset1 myset2 myset3");
    db.Call(ctx, sinter, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "sinter failed");
    RedisCommandFrame scard;
    scard.SetFullCommand("scard myset4");
    db.Call(ctx, scard, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "scard failed");

    for (uint32 i = 0; i < 50; i++)
    {
        RedisCommandFrame sadd;
        sadd.SetFullCommand("sadd myset2 %u", i);
        db.Call(ctx, sadd, 0);
        sadd.SetFullCommand("sadd myset3 %u", i);
        db.Call(ctx, sadd, 0);
        sadd.SetFullCommand("sadd myset1 %u", i);
        db.Call(ctx, sadd, 0);
    }
    sinter.SetFullCommand("sinter myset1 myset2 myset3");
    db.Call(ctx, sinter, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 51, "sinter failed");
    sinter.SetFullCommand("sintercount myset1 myset2 myset3");
    db.Call(ctx, sinter, 0);
    CHECK_FATAL(ctx.reply.integer != 51, "sinter failed");
    sinter.SetFullCommand("sinterstore myset4 myset1 myset2 myset3");
    db.Call(ctx, sinter, 0);
    CHECK_FATAL(ctx.reply.integer != 51, "sinter failed");
    db.Call(ctx, scard, 0);
    CHECK_FATAL(ctx.reply.integer != 51, "scard failed");
}

void test_set_diff(Context& ctx, Ardb& db)
{
    db.GetConfig().set_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myset1 myset2 myset3 myset4");
    db.Call(ctx, del, 0);

    RedisCommandFrame sadd1, sadd2, sadd3;
    sadd1.SetFullCommand("sadd myset1 a b c d");
    sadd2.SetFullCommand("sadd myset2 c");
    sadd3.SetFullCommand("sadd myset3 a c e");
    db.Call(ctx, sadd1, 0);
    db.Call(ctx, sadd2, 0);
    db.Call(ctx, sadd3, 0);
    RedisCommandFrame sdiff;
    sdiff.SetFullCommand("sdiff myset1 myset2 myset3");
    db.Call(ctx, sdiff, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 2, "sdiff failed");
    sdiff.SetFullCommand("sdiffcount myset1 myset2 myset3");
    db.Call(ctx, sdiff, 0);
    CHECK_FATAL(ctx.reply.integer != 2, "sdiff failed");
    sdiff.SetFullCommand("sdiffstore myset4 myset1 myset2 myset3");
    db.Call(ctx, sdiff, 0);
    CHECK_FATAL(ctx.reply.integer != 2, "sdiff failed");
    RedisCommandFrame scard;
    scard.SetFullCommand("scard myset4");
    db.Call(ctx, scard, 0);
    CHECK_FATAL(ctx.reply.integer != 2, "scard failed");

    for (uint32 i = 0; i < 50; i++)
    {
        RedisCommandFrame sadd;
        sadd.SetFullCommand("sadd myset1 %u", i + 100);
        db.Call(ctx, sadd, 0);
        sadd.SetFullCommand("sadd myset3 %u", i);
        db.Call(ctx, sadd, 0);
    }
    sdiff.SetFullCommand("sdiff myset1 myset2 myset3");
    db.Call(ctx, sdiff, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 52, "sdiff failed");
    sdiff.SetFullCommand("sdiffcount myset1 myset2 myset3");
    db.Call(ctx, sdiff, 0);
    CHECK_FATAL(ctx.reply.integer != 52, "sdiff failed");
    sdiff.SetFullCommand("sdiffstore myset4 myset1 myset2 myset3");
    db.Call(ctx, sdiff, 0);
    CHECK_FATAL(ctx.reply.integer != 52, "sdiff failed");
    db.Call(ctx, scard, 0);
    CHECK_FATAL(ctx.reply.integer != 52, "scard failed");
}

void test_set(Ardb& db)
{
    Context tmpctx;
    test_set_common(tmpctx, db);
    test_set_remove(tmpctx, db);
    test_set_inter(tmpctx, db);
    test_set_union(tmpctx, db);
    test_set_diff(tmpctx, db);
}

