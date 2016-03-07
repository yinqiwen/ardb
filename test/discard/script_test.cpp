/*
 * script_test.cpp
 *
 *  Created on: 2014Äê8ÔÂ11ÈÕ
 *      Author: wangqiying
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_scripts_eval(Context& ctx, Ardb& db)
{
    RedisCommandFrame del;
    del.SetFullCommand("del foo");
    db.Call(ctx, del, 0);

    RedisCommandFrame eval;
    eval.SetFullCommand("eval \"return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}\" 2 key1 key2 first second");
    db.Call(ctx, eval, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != 4, "eval failed");

    eval.SetFullCommand("eval \"return redis.call('set','foo','bar')\" 0");
    db.Call(ctx, eval, 0);
    CHECK_FATAL(ctx.reply.str != "OK", "eval failed");

    eval.SetFullCommand("eval \"return redis.call('set',KEYS[1],'bar')\" 1 foo");
    db.Call(ctx, eval, 0);
    CHECK_FATAL(ctx.reply.str != "OK", "eval failed");

    eval.SetFullCommand("eval \"return 10\" 0");
    db.Call(ctx, eval, 0);
    CHECK_FATAL(ctx.reply.integer != 10, "eval failed");

    eval.SetFullCommand("eval \"return redis.call('get','foo')\" 0");
    db.Call(ctx, eval, 0);
    CHECK_FATAL(ctx.reply.str != "bar", "eval failed");
}

void test_scripts(Ardb& db)
{
    Context ctx;
    test_scripts_eval(ctx, db);
}

