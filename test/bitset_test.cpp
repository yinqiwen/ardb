/*
 * strings.testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: yinqiwen
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_bitset_common(Context& ctx, Ardb& db)
{
    RedisCommandFrame del;
    del.SetFullCommand("del mybitset");
    db.Call(ctx, del, 0);

    RedisCommandFrame setbit;
    setbit.SetFullCommand("setbit mybitset 4011 1");
    db.Call(ctx, setbit, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "setbit myzset failed");
    db.Call(ctx, setbit, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "setbit myzset failed");

    RedisCommandFrame getbit;
    getbit.SetFullCommand("getbit mybitset 4011");
    db.Call(ctx, getbit, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "getbit myzset failed");
    getbit.SetFullCommand("getbit mybitset 14011");
    db.Call(ctx, getbit, 0);
    CHECK_FATAL(ctx.reply.integer != 0, "getbit myzset failed");

    for (uint32 i = 0; i < 10; i++)
    {
        setbit.SetFullCommand("setbit mybitset %u 1", (i + 1) * 10000);
        db.Call(ctx, setbit, 0);
    }
    RedisCommandFrame bitcount;
    bitcount.SetFullCommand("bitcount mybitset 0 -1");
    db.Call(ctx, bitcount, 0);
    CHECK_FATAL(ctx.reply.integer != 11, "bitcount myzset failed");
    bitcount.SetFullCommand("bitcount mybitset 0 50000");
    db.Call(ctx, bitcount, 0);
    CHECK_FATAL(ctx.reply.integer != 6, "bitcount myzset failed");
}

void test_bitset_bitop(Context& ctx, Ardb& db)
{
    RedisCommandFrame del;
    del.SetFullCommand("del mybitset1 mybitset2");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 4000; i++)
    {
        RedisCommandFrame setbit;
        setbit.SetFullCommand("setbit mybitset1 %u 1", i + 1000);
        db.Call(ctx, setbit, 0);
    }
    for (uint32 i = 0; i < 2000; i++)
    {
        RedisCommandFrame setbit;
        setbit.SetFullCommand("setbit mybitset2 %u 1", i + 4000);
        db.Call(ctx, setbit, 0);
    }

    RedisCommandFrame bitop;
    bitop.SetFullCommand("bitopcount and mybitset1 mybitset2");
    db.Call(ctx, bitop, 0);
    CHECK_FATAL(ctx.reply.integer != 1000, "bitopcount failed");
    bitop.SetFullCommand("bitopcount or mybitset1 mybitset2");
    db.Call(ctx, bitop, 0);
    CHECK_FATAL(ctx.reply.integer != 5000, "bitopcount failed");
    bitop.SetFullCommand("bitopcount xor mybitset1 mybitset2");
    db.Call(ctx, bitop, 0);
    CHECK_FATAL(ctx.reply.integer != 4000, "bitopcount failed");

    RedisCommandFrame bitcount;
    bitcount.SetFullCommand("bitcount mybitset3 0 -1");
    bitop.SetFullCommand("bitop and mybitset3 mybitset1 mybitset2");
    db.Call(ctx, bitop, 0);
    CHECK_FATAL(ctx.reply.integer != 1000, "bitopcount failed");
    db.Call(ctx, bitcount, 0);
    CHECK_FATAL(ctx.reply.integer != 1000, "bitcount failed");
    bitop.SetFullCommand("bitop or mybitset3 mybitset1 mybitset2");
    db.Call(ctx, bitop, 0);
    CHECK_FATAL(ctx.reply.integer != 5000, "bitopcount failed");
    db.Call(ctx, bitcount, 0);
    CHECK_FATAL(ctx.reply.integer != 5000, "bitcount failed");
    bitop.SetFullCommand("bitop xor mybitset3 mybitset1 mybitset2");
    db.Call(ctx, bitop, 0);
    CHECK_FATAL(ctx.reply.integer != 4000, "bitopcount failed");
    db.Call(ctx, bitcount, 0);
    CHECK_FATAL(ctx.reply.integer != 4000, "bitcount failed");

}

void test_bitset(Ardb& db)
{
    Context ctx;
    test_bitset_common(ctx, db);
    test_bitset_bitop(ctx, db);
}

