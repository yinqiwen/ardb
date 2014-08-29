/*
 * zset_testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: yinqiwen
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_zsets_common(Context& ctx, Ardb& db)
{
    db.GetConfig().zset_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myzset");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u field%u", i, i);
        db.Call(ctx, zadd, 0);
        CHECK_FATAL(ctx.reply.integer != 1, "zadd myzset failed");
    }
    RedisCommandFrame zadd;
    zadd.SetFullCommand("zadd myzset 100 field100 101 field101 102 field0");
    db.Call(ctx, zadd, 0);
    CHECK_FATAL(ctx.reply.integer != 2, "zadd myzset failed");
    RedisCommandFrame zcard;
    zcard.SetFullCommand("zcard myzset");
    db.Call(ctx, zcard, 0);
    CHECK_FATAL(ctx.reply.integer != 12, "zcard myzset failed");
    RedisCommandFrame zscore;
    zscore.SetFullCommand("zscore myzset field0");
    db.Call(ctx, zscore, 0);
    CHECK_FATAL(ctx.reply.str != "102", "zscore myzset failed");

    for (uint32 i = 0; i < 20; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u xfield%u", i, i);
        db.Call(ctx, zadd, 0);
        CHECK_FATAL(ctx.reply.integer != 1, "zadd myzset failed");
    }
    zadd.SetFullCommand("zadd myzset 1000 field1001 1010 field1011 104 field0");
    db.Call(ctx, zadd, 0);
    CHECK_FATAL(ctx.reply.integer != 2, "zadd myzset failed");
    db.Call(ctx, zcard, 0);
    CHECK_FATAL(ctx.reply.integer != 34, "zcard myzset failed");
    zscore.SetFullCommand("zscore myzset field1001");
    db.Call(ctx, zscore, 0);
    CHECK_FATAL(ctx.reply.str != "1000", "zscore myzset failed");

}

void test_zsets_zrangebyrank(Context& ctx, Ardb& db)
{
    db.GetConfig().zset_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myzset");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u field%u", i, i);
        db.Call(ctx, zadd, 0);
    }
    RedisCommandFrame zrange;
    zrange.SetFullCommand("zrange myzset 0 -1 WITHSCORES");
    db.Call(ctx, zrange, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 20, "zrange myzset failed");
    zrange.SetFullCommand("zrange myzset 0 -1");
    db.Call(ctx, zrange, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 10, "zrange myzset failed");

    RedisCommandFrame zrevrange;
    zrevrange.SetFullCommand("zrevrange myzset -5 -1");
    db.Call(ctx, zrevrange, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 5, "zrevrange myzset failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "field9", "zrevrange myzset failed");
    zrevrange.SetFullCommand("zrevrange myzset -5 -1 WITHSCORES");
    db.Call(ctx, zrevrange, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 10, "zrevrange myzset failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "9", "zrevrange myzset failed");

    RedisCommandFrame zremrange;
    zremrange.SetFullCommand("ZREMRANGEBYRANK myzset -5 -1");
    db.Call(ctx, zremrange, 0);
    CHECK_FATAL(ctx.reply.integer != 5, "zremrange myzset failed");
    RedisCommandFrame zcard;
    zcard.SetFullCommand("zcard myzset");
    db.Call(ctx, zcard, 0);
    CHECK_FATAL(ctx.reply.integer != 5, "zcard myzset failed");

    for (uint32 i = 0; i < 20; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u zfield%u", i + 100, i);
        db.Call(ctx, zadd, 0);
    }
    zrange.SetFullCommand("zrange myzset 0 -1 WITHSCORES");
    db.Call(ctx, zrange, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 50, "zrange myzset failed");
    zrange.SetFullCommand("zrange myzset 0 -1");
    db.Call(ctx, zrange, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 25, "zrange myzset failed");

    zrevrange.SetFullCommand("zrevrange myzset -10 -1");
    db.Call(ctx, zrevrange, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 10, "zrevrange myzset failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "zfield18", "zrevrange myzset failed");
    zrevrange.SetFullCommand("zrevrange myzset -10 -1 WITHSCORES");
    db.Call(ctx, zrevrange, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 20, "zrevrange myzset failed");
    CHECK_FATAL(ctx.reply.MemberAt(3).str != "118", "zrevrange myzset failed");

    db.Call(ctx, zremrange, 0);
    CHECK_FATAL(ctx.reply.integer != 5, "zremrange myzset failed");
    db.Call(ctx, zcard, 0);
    CHECK_FATAL(ctx.reply.integer != 20, "zcard myzset failed");
}

void test_zsets_zrangebyscore(Context& ctx, Ardb& db)
{
    db.GetConfig().zset_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myzset");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u field%u", i, i);
        db.Call(ctx, zadd, 0);
    }
    RedisCommandFrame zrangebyscore;
    zrangebyscore.SetFullCommand("zrangebyscore myzset 0 (100 WITHSCORES limit 2 7");
    db.Call(ctx, zrangebyscore, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 14, "zrangebyscore myzset failed");
    zrangebyscore.SetFullCommand("zrangebyscore myzset (4 8");
    db.Call(ctx, zrangebyscore, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 4, "zrangebyscore myzset failed");

    RedisCommandFrame zrevrangebyscore;
    zrevrangebyscore.SetFullCommand("zrevrangebyscore myzset 100 (-1 limit 3 5");
    db.Call(ctx, zrevrangebyscore, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 5, "zrevrangebyscore myzset failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "field6", "zrevrangebyscore myzset failed");
    zrevrangebyscore.SetFullCommand("zrevrangebyscore myzset (9 0 WITHSCORES");
    db.Call(ctx, zrevrangebyscore, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 18, "zrevrangebyscore myzset failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "8", "zrevrangebyscore myzset failed");

    RedisCommandFrame zremrange;
    zremrange.SetFullCommand("ZREMRANGEBYSCORE myzset 5 (9");
    db.Call(ctx, zremrange, 0);
    CHECK_FATAL(ctx.reply.integer != 4, "zremrangebyscore myzset failed");
    RedisCommandFrame zcard;
    zcard.SetFullCommand("zcard myzset");
    db.Call(ctx, zcard, 0);
    CHECK_FATAL(ctx.reply.integer != 6, "zcard myzset failed");

    for (uint32 i = 0; i < 20; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u zfield%u", i + 100, i);
        db.Call(ctx, zadd, 0);
    }
    zrangebyscore.SetFullCommand("zrangebyscore myzset (0 10000 WITHSCORES");
    db.Call(ctx, zrangebyscore, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 50, "zrangebyscore myzset failed");
    zrangebyscore.SetFullCommand("zrangebyscore myzset (0 10000 limit 10 10");
    db.Call(ctx, zrangebyscore, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 10, "zrangebyscore myzset failed");

    zrangebyscore.SetFullCommand("zrevrangebyscore myzset 30 -1");
    db.Call(ctx, zrangebyscore, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 6, "zrangebyscore myzset failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "field4", "zrangebyscore myzset failed");
    zrangebyscore.SetFullCommand("zrevrangebyscore myzset 111 -1 WITHSCORES limit 0 7");
    db.Call(ctx, zrangebyscore, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 14, "zrevrange myzset failed");
    CHECK_FATAL(ctx.reply.MemberAt(3).str != "110", "zrevrangebyscore myzset failed");

    zremrange.SetFullCommand("ZREMRANGEBYSCORE myzset 100  104");
    db.Call(ctx, zremrange, 0);
    CHECK_FATAL(ctx.reply.integer != 5, "zremrange myzset failed");
    db.Call(ctx, zcard, 0);
    CHECK_FATAL(ctx.reply.integer != 21, "zcard myzset failed");
}

void test_zsets_zrangebylex(Context& ctx, Ardb& db)
{
    db.GetConfig().zset_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myzset");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u field%u", i, i);
        db.Call(ctx, zadd, 0);
    }
    RedisCommandFrame zrangebylex;
    zrangebylex.SetFullCommand("zrangebylex myzset (field0 [field7 limit 2 4");
    db.Call(ctx, zrangebylex, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 4, "zrangebylex myzset failed");
    zrangebylex.SetFullCommand("zrangebylex myzset - +");
    db.Call(ctx, zrangebylex, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 10, "zrangebylex myzset failed");

    RedisCommandFrame zremrange;
    zremrange.SetFullCommand("ZREMRANGEBYLEX myzset (field5 [field9");
    db.Call(ctx, zremrange, 0);
    CHECK_FATAL(ctx.reply.integer != 4, "zremrangebyscore myzset failed");
    RedisCommandFrame zcard;
    zcard.SetFullCommand("zcard myzset");
    db.Call(ctx, zcard, 0);
    CHECK_FATAL(ctx.reply.integer != 6, "zcard myzset failed");

    RedisCommandFrame zcount;
    zcount.SetFullCommand("zlexcount myzset - [field9");
    db.Call(ctx, zcount, 0);
    CHECK_FATAL(ctx.reply.integer != 6, "zlexcount myzset failed");

    for (uint32 i = 0; i < 20; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u zfield%u", i + 100, i);
        db.Call(ctx, zadd, 0);
    }
    zrangebylex.SetFullCommand("zrangebylex myzset - [zfield9");
    db.Call(ctx, zrangebylex, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 26, "zrangebylex myzset failed");
    zrangebylex.SetFullCommand("zrangebylex myzset [zf +  limit 5 10");
    db.Call(ctx, zrangebylex, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 10, "zrangebylex myzset failed");

    zremrange.SetFullCommand("ZREMRANGEBYLEX myzset [zfield2  +");
    db.Call(ctx, zremrange, 0);
    CHECK_FATAL(ctx.reply.integer != 8, "ZREMRANGEBYLEX myzset failed");
    db.Call(ctx, zcard, 0);
    CHECK_FATAL(ctx.reply.integer != 18, "zcard myzset failed");
}

void test_zsets_zcount(Context& ctx, Ardb& db)
{
    db.GetConfig().zset_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myzset");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u field%u", i, i);
        db.Call(ctx, zadd, 0);
    }
    RedisCommandFrame zcount;
    zcount.SetFullCommand("zcount myzset 3 6");
    db.Call(ctx, zcount, 0);
    CHECK_FATAL(ctx.reply.integer != 4, "zcount myzset failed");

    for (uint32 i = 0; i < 10000; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u zfield%u", i, i);
        db.Call(ctx, zadd, 0);
    }
    zcount.SetFullCommand("zcount myzset -inf +inf");
    db.Call(ctx, zcount, 0);
    CHECK_FATAL(ctx.reply.integer != 10010, "zcount myzset failed");
    zcount.SetFullCommand("zcount myzset 1011 2544");
    db.Call(ctx, zcount, 0);
    CHECK_FATAL(ctx.reply.integer != 1534, "zcount myzset failed");
}

void test_zsets_zrank(Context& ctx, Ardb& db)
{
    db.GetConfig().zset_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myzset");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u field%u", i, i);
        db.Call(ctx, zadd, 0);
    }
    RedisCommandFrame zrank;
    zrank.SetFullCommand("zrank myzset field6");
    db.Call(ctx, zrank, 0);
    CHECK_FATAL(ctx.reply.integer != 6, "zrank myzset failed");
    RedisCommandFrame zrevrank;
    zrevrank.SetFullCommand("zrevrank myzset field6");
    db.Call(ctx, zrevrank, 0);
    CHECK_FATAL(ctx.reply.integer != 3, "zrevrank myzset failed");

    for (uint32 i = 0; i < 10000; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u zfield%u", i, i);
        db.Call(ctx, zadd, 0);
    }
    zrank.SetFullCommand("zrank myzset zfield5000");
    db.Call(ctx, zrank, 0);
    CHECK_FATAL(ctx.reply.integer != 5010, "zcount myzset failed");
    zrevrank.SetFullCommand("zrevrank myzset zfield5000");
    db.Call(ctx, zrevrank, 0);
    CHECK_FATAL(ctx.reply.integer != 4999, "zrevrank myzset failed");
}

void test_zsets_incr(Context& ctx, Ardb& db)
{
    db.GetConfig().zset_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myzset");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u field%u", i, i);
        db.Call(ctx, zadd, 0);
    }
    RedisCommandFrame zincr;
    zincr.SetFullCommand("zincrby myzset 10 field6");
    db.Call(ctx, zincr, 0);
    CHECK_FATAL(ctx.reply.str != "16", "zincrby myzset failed");
    RedisCommandFrame zrank;
    zrank.SetFullCommand("zrank myzset field6");
    db.Call(ctx, zrank, 0);
    CHECK_FATAL(ctx.reply.integer != 9, "zrank myzset failed");

    for (uint32 i = 0; i < 20; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u zfield%u", i, i);
        db.Call(ctx, zadd, 0);
    }
    zincr.SetFullCommand("zincrby myzset 100 zfield6");
    db.Call(ctx, zincr, 0);
    CHECK_FATAL(ctx.reply.str != "106", "zincrby myzset failed");
    zrank.SetFullCommand("zrank myzset zfield6");
    db.Call(ctx, zrank, 0);
    CHECK_FATAL(ctx.reply.integer != 29, "zrank myzset failed");

}

void test_zsets_inter(Context& ctx, Ardb& db)
{
    db.GetConfig().zset_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myzset myzset1");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u field%u", i, i);
        db.Call(ctx, zadd, 0);
    }

    for (uint32 i = 0; i < 50; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset1 %u field%u", i + 10, i);
        db.Call(ctx, zadd, 0);
    }
    RedisCommandFrame zinter;
    zinter.SetFullCommand("zinterstore myzset2 2 myzset1 myzset WEIGHTS 10 10 AGGREGATE MAX");
    db.Call(ctx, zinter, 0);
    CHECK_FATAL(ctx.reply.integer != 10, "zinter myzset failed");
    RedisCommandFrame zscore;
    zscore.SetFullCommand("zscore myzset2 field5");
    db.Call(ctx, zscore, 0);
    CHECK_FATAL(ctx.reply.str != "150", "zscore myzset2 failed");
}

void test_zsets_union(Context& ctx, Ardb& db)
{
    db.GetConfig().zset_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del myzset myzset1");
    db.Call(ctx, del, 0);

    for (uint32 i = 0; i < 10; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset %u field%u", i, i);
        db.Call(ctx, zadd, 0);
    }

    for (uint32 i = 0; i < 50; i++)
    {
        RedisCommandFrame zadd;
        zadd.SetFullCommand("zadd myzset1 %u field%u", i + 10, i);
        db.Call(ctx, zadd, 0);
    }
    RedisCommandFrame zunion;
    zunion.SetFullCommand("zunionstore myzset2 2 myzset1 myzset WEIGHTS 10 10 AGGREGATE SUM");
    db.Call(ctx, zunion, 0);
    CHECK_FATAL(ctx.reply.integer != 50, "zinter myzset failed");
    RedisCommandFrame zscore;
    zscore.SetFullCommand("zscore myzset2 field19");
    db.Call(ctx, zscore, 0);
    CHECK_FATAL(ctx.reply.str != "290", "zscore myzset2 failed");
}

void test_zset(Ardb& db)
{
    Context tmpctx;
    test_zsets_common(tmpctx, db);
    test_zsets_zrangebyrank(tmpctx, db);
    test_zsets_zrangebyscore(tmpctx, db);
    test_zsets_zrangebylex(tmpctx, db);

    test_zsets_zcount(tmpctx, db);
    test_zsets_zrank(tmpctx, db);
    test_zsets_incr(tmpctx, db);
    test_zsets_inter(tmpctx, db);
    test_zsets_union(tmpctx, db);
}
