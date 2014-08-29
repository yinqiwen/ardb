/*
 * sort_test.cpp
 *
 *  Created on: 2014Äê8ÔÂ9ÈÕ
 *      Author: wangqiying
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_misc_hyperloglog(Context& ctx, Ardb& db)
{
    RedisCommandFrame del;
    del.SetFullCommand("del hll hll1 hll2 hll3");
    db.Call(ctx, del, 0);

    RedisCommandFrame pfadd;
    pfadd.SetFullCommand("PFADD hll a b c d e f g");
    db.Call(ctx, pfadd, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "pfadd  failed");

    RedisCommandFrame pfcount;
    pfcount.SetFullCommand("PFCOUNT hll");
    db.Call(ctx, pfcount, 0);
    CHECK_FATAL(ctx.reply.integer != 7, "pfcount  failed");

    pfadd.SetFullCommand("PFADD hll1 foo bar zap a");
    db.Call(ctx, pfadd, 0);
    pfadd.SetFullCommand("PFADD hll2 a b c foo");
    db.Call(ctx, pfadd, 0);
    RedisCommandFrame pfmerge;
    pfmerge.SetFullCommand("PFMERGE hll3 hll1 hll2");
    db.Call(ctx, pfmerge, 0);
    CHECK_FATAL(ctx.reply.str != "OK", "pfmerge  failed");
    pfcount.SetFullCommand("PFCOUNT hll3");
    db.Call(ctx, pfcount, 0);
    CHECK_FATAL(ctx.reply.integer != 6, "pfcount  failed");
}

void test_misc_sortlist(Context& ctx, Ardb& db)
{
    RedisCommandFrame del;
    del.SetFullCommand("del mylist myhash weight_100 weight_10 weight_9 weight_1000");
    db.Call(ctx, del, 0);

    RedisCommandFrame rpush;
    rpush.SetFullCommand("rpush mylist 100 10 9 1000");
    db.Call(ctx, rpush, 0);

    RedisCommandFrame sort;
    sort.SetFullCommand("sort mylist");
    db.Call(ctx, sort, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 4, "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "9", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "10", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(2).str != "100", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(3).str != "1000", "sort  failed");

    sort.SetFullCommand("sort mylist limit 1 2");
    db.Call(ctx, sort, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 2, "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "10", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "100", "sort  failed");

    RedisCommandFrame mset;
    mset.SetFullCommand("mset weight_100 1000 weight_10 900 weight_9 800 weight_1000 700");
    db.Call(ctx, mset, 0);

    sort.SetFullCommand("sort mylist by weight_*");
    db.Call(ctx, sort, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 4, "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "1000", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "9", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(2).str != "10", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(3).str != "100", "sort  failed");

    RedisCommandFrame hmset;
    hmset.SetFullCommand("hmset myhash field_100 hash100 field_10 hash10 field_9 hash9 field_1000 hash1000");
    db.Call(ctx, hmset, 0);

    sort.SetFullCommand("sort mylist by weight_* get myhash->field_* get #");
    db.Call(ctx, sort, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 8, "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "1000", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(3).str != "9", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(5).str != "10", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(7).str != "100", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "hash1000", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(2).str != "hash9", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(4).str != "hash10", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(6).str != "hash100", "sort  failed");
}

void test_misc_sortset(Context& ctx, Ardb& db)
{
    RedisCommandFrame del;
    del.SetFullCommand("del myset myhash weight_100 weight_10 weight_9 weight_1000");
    db.Call(ctx, del, 0);

    RedisCommandFrame sadd;
    sadd.SetFullCommand("sadd myset 100 10 9 1000");
    db.Call(ctx, sadd, 0);

    RedisCommandFrame sort;
    sort.SetFullCommand("sort myset");
    db.Call(ctx, sort, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 4, "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "9", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "10", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(2).str != "100", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(3).str != "1000", "sort  failed");

    sort.SetFullCommand("sort myset limit 1 2");
    db.Call(ctx, sort, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 2, "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "10", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "100", "sort  failed");

    RedisCommandFrame mset;
    mset.SetFullCommand("mset weight_100 1000 weight_10 900 weight_9 800 weight_1000 700");
    db.Call(ctx, mset, 0);

    sort.SetFullCommand("sort myset by weight_*");
    db.Call(ctx, sort, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 4, "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "1000", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "9", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(2).str != "10", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(3).str != "100", "sort  failed");

    RedisCommandFrame hmset;
    hmset.SetFullCommand("hmset myhash field_100 hash100 field_10 hash10 field_9 hash9 field_1000 hash1000");
    db.Call(ctx, hmset, 0);

    sort.SetFullCommand("sort myset by weight_* get myhash->field_* get #");
    db.Call(ctx, sort, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 8, "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "1000", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(3).str != "9", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(5).str != "10", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(7).str != "100", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "hash1000", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(2).str != "hash9", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(4).str != "hash10", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(6).str != "hash100", "sort  failed");
}

void test_misc_sortzset(Context& ctx, Ardb& db)
{
    RedisCommandFrame del;
    del.SetFullCommand("del myzset myhash weight_v0 weight_v1 weight_v2 weight_v3");
    db.Call(ctx, del, 0);

    RedisCommandFrame sadd;
    sadd.SetFullCommand("zadd myzset 100 v0 10 v1 9 v2 1000 v3");
    db.Call(ctx, sadd, 0);

    RedisCommandFrame sort;
    sort.SetFullCommand("sort myzset");
    db.Call(ctx, sort, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 4, "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "v2", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "v1", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(2).str != "v0", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(3).str != "v3", "sort  failed");

    sort.SetFullCommand("sort myzset limit 1 2");
    db.Call(ctx, sort, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 2, "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "v1", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "v0", "sort  failed");

    RedisCommandFrame mset;
    mset.SetFullCommand("mset weight_v0 10 weight_v1 20 weight_v2 30 weight_v3 40");
    db.Call(ctx, mset, 0);

    sort.SetFullCommand("sort myzset by weight_*");
    db.Call(ctx, sort, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 4, "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "v0", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "v1", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(2).str != "v2", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(3).str != "v3", "sort  failed");

    RedisCommandFrame hmset;
    hmset.SetFullCommand("hmset myhash field_v0 hashv0 field_v1 hashv1 field_v2 hashv2 field_v3 hashv3");
    db.Call(ctx, hmset, 0);

    sort.SetFullCommand("sort myzset by weight_* get myhash->field_* get #");
    db.Call(ctx, sort, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 8, "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(1).str != "v0", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(3).str != "v1", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(5).str != "v2", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(7).str != "v3", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(0).str != "hashv0", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(2).str != "hashv1", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(4).str != "hashv2", "sort  failed");
    CHECK_FATAL(ctx.reply.MemberAt(6).str != "hashv3", "sort  failed");
}

void test_misc(Ardb& db)
{
    Context ctx;
    test_misc_hyperloglog(ctx, db);
    test_misc_sortlist(ctx, db);
    test_misc_sortset(ctx, db);
    test_misc_sortzset(ctx, db);
}

