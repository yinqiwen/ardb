/*
 * strings.testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: yinqiwen
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_strings_append(Context& ctx, Ardb& db)
{
    RedisCommandFrame cmd;
    cmd.SetFullCommand("set key1 100");
    db.Call(ctx, cmd, 0);
    RedisCommandFrame append;
    append.SetFullCommand("append key1 abcd");
    db.Call(ctx, append, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to append key");
    CHECK_FATAL(ctx.reply.integer != 7, "Failed to append key");
    RedisCommandFrame get;
    get.SetFullCommand("get key1");
    db.Call(ctx, get, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "Failed to get append result");
    CHECK_FATAL(ctx.reply.str != "100abcd", "Failed to get append result");

    RedisCommandFrame del;
    del.SetFullCommand("del key1");
    db.Call(ctx, del, 0);
    RedisCommandFrame append1;
    append1.SetFullCommand("append key1 zzzzzz");
    db.Call(ctx, append1, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to append key");
    CHECK_FATAL(ctx.reply.integer != 6, "Failed to append key");
    db.Call(ctx, get, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "Failed to get append result");
    CHECK_FATAL(ctx.reply.str != "zzzzzz", "Failed to get append result");
}

void test_strings_getrange(Context& ctx, Ardb& db)
{
    RedisCommandFrame set;
    set.SetFullCommand("set key2 abcabc");
    db.Call(ctx, set, 0);
    RedisCommandFrame getrange;
    getrange.SetFullCommand("getrange key2 4 -1");
    db.Call(ctx, getrange, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "Failed to getrange result");
    CHECK_FATAL(ctx.reply.str != "bc", "Failed to getrange result");

    RedisCommandFrame del;
    del.SetFullCommand("del key2");
    db.Call(ctx, del, 0);
    db.Call(ctx, getrange, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "Failed to getrange result");
    CHECK_FATAL(ctx.reply.str != "", "Failed to getrange result");
}

void test_strings_setrange(Context& ctx, Ardb& db)
{
    RedisCommandFrame set;
    set.SetFullCommand("set key3 abcabc");
    db.Call(ctx, set, 0);
    RedisCommandFrame setrange;
    setrange.SetFullCommand("setrange key3 3 12345");
    db.Call(ctx, setrange, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to setrange result");
    CHECK_FATAL(ctx.reply.integer != 8, "Failed to setrange result:%d", ctx.reply.integer);
    RedisCommandFrame get;
    get.SetFullCommand("get key3");
    db.Call(ctx, get, 0);
    CHECK_FATAL(ctx.reply.str != "abc12345", "Failed to setrange result");

    RedisCommandFrame del;
    del.SetFullCommand("del key3");
    db.Call(ctx, del, 0);
    db.Call(ctx, setrange, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to setrange result");
    CHECK_FATAL(ctx.reply.integer != 8, "Failed to setrange result");
    std::string expected;
    expected.resize(3);
    expected.append("12345");
    db.Call(ctx, get, 0);
    CHECK_FATAL(ctx.reply.str != expected, "Failed to setrange result");
}

void test_strings_getset(Context& ctx, Ardb& db)
{
    RedisCommandFrame set;
    set.SetFullCommand("set key4 abcabc");
    db.Call(ctx, set, 0);
    RedisCommandFrame setrange;
    setrange.SetFullCommand("getset key4 10000");
    db.Call(ctx, setrange, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STRING, "Failed to getset result");
    CHECK_FATAL(ctx.reply.str != "abcabc", "Failed to getset result");
}

void test_strings_strlen(Context& ctx, Ardb& db)
{
    RedisCommandFrame set;
    set.SetFullCommand("set key5 abcabc");
    db.Call(ctx, set, 0);
    RedisCommandFrame strlen;
    strlen.SetFullCommand("strlen key5");
    db.Call(ctx, strlen, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to strlen result");
    CHECK_FATAL(ctx.reply.integer != 6, "Failed to strlen result");

    RedisCommandFrame set1;
    set1.SetFullCommand("set key5 1000");
    db.Call(ctx, set1, 0);
    db.Call(ctx, strlen, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to strlen result");
    CHECK_FATAL(ctx.reply.integer != 4, "Failed to strlen result");

    RedisCommandFrame del;
    del.SetFullCommand("del key5");
    db.Call(ctx, del, 0);
    db.Call(ctx, strlen, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to strlen result");
    CHECK_FATAL(ctx.reply.integer != 0, "Failed to strlen result");

}

void test_strings_incrdecr(Context& ctx, Ardb& db)
{
    RedisCommandFrame set;
    set.SetFullCommand("set key6 abcabc");
    db.Call(ctx, set, 0);
    RedisCommandFrame incr;
    incr.SetFullCommand("incr key6");
    db.Call(ctx, incr, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_ERROR, "Failed to incr");

    RedisCommandFrame set1;
    set1.SetFullCommand("set key6 100");
    db.Call(ctx, set1, 0);
    db.Call(ctx, incr, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to incr");
    CHECK_FATAL(ctx.reply.integer != 101, "Failed to incr");

    RedisCommandFrame del;
    del.SetFullCommand("del key6");
    db.Call(ctx, del, 0);
    RedisCommandFrame incrby;
    incrby.SetFullCommand("incrby key6 315");
    db.Call(ctx, incrby, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to incrby");
    CHECK_FATAL(ctx.reply.integer != 315, "Failed to incrby");

    RedisCommandFrame decr;
    decr.SetFullCommand("decr key6");
    db.Call(ctx, decr, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to decr");
    CHECK_FATAL(ctx.reply.integer != 314, "Failed to decr");

    RedisCommandFrame decrby;
    decrby.SetFullCommand("decrby key6 318");
    db.Call(ctx, decrby, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to decrby");
    CHECK_FATAL(ctx.reply.integer != -4, "Failed to decrby");

    RedisCommandFrame incrbyfloat;
    incrbyfloat.SetFullCommand("incrbyfloat key6 5.1");
    db.Call(ctx, incrbyfloat, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_DOUBLE, "Failed to incrbyfloat");
    //CHECK_FATAL(ctx.reply.double_value != 1.1, "Failed to incrbyfloat");
}

void test_strings_set(Context& ctx, Ardb& db)
{
    RedisCommandFrame del;
    del.SetFullCommand("del key6");
    db.Call(ctx, del, 0);
    RedisCommandFrame set;
    set.SetFullCommand("setnx key6 abcabc");
    db.Call(ctx, set, 0);
    CHECK_FATAL(ctx.reply.integer != 1, "Failed to setnx");

    db.Call(ctx, set, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to setnx");
    CHECK_FATAL(ctx.reply.integer != 0, "Failed to setnx");

    set.SetFullCommand("set key6 abcabc nx");
    db.Call(ctx, set, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_NIL, "Failed to set");

    set.SetFullCommand("set key6 abcabc xx");
    db.Call(ctx, set, 0);
    CHECK_FATAL(ctx.reply.str != "OK", "Failed to set");
    db.Call(ctx, del, 0);
    set.SetFullCommand("set key6 abcabc xx");
    db.Call(ctx, set, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_NIL, "Failed to set");

}

void test_strings_mget(Context& ctx, Ardb& db)
{
    RedisCommandFrame set1;
    set1.SetFullCommand("set key1 1");
    RedisCommandFrame set2;
    set2.SetFullCommand("set key2 2");
    RedisCommandFrame set3;
    set3.SetFullCommand("set key3 3");
    RedisCommandFrame del;
    del.SetFullCommand("del key4");
    db.Call(ctx, del, 0);
    db.Call(ctx, set1, 0);
    db.Call(ctx, set2, 0);
    db.Call(ctx, set3, 0);

    RedisCommandFrame mget;
    mget.SetFullCommand("mget key4 key1 key2 key3");
    db.Call(ctx, mget, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_ARRAY, "Failed to mget");
    CHECK_FATAL(ctx.reply.MemberSize() != 4, "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(0)->type != REDIS_REPLY_NIL, "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(1)->type != REDIS_REPLY_STRING, "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(2)->type != REDIS_REPLY_STRING, "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(3)->type != REDIS_REPLY_STRING, "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(1)->str != "1", "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(2)->str != "2", "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(3)->str != "3", "Failed to mget");
}

void test_strings_mset(Context& ctx, Ardb& db)
{
    RedisCommandFrame mset;
    mset.SetFullCommand("mset key1 4 key2 3 key3 2 key4 1");
    db.Call(ctx, mset, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_STATUS, "Failed to mset");
    CHECK_FATAL(ctx.reply.str != "OK", "Failed to mset");

    RedisCommandFrame mget;
    mget.SetFullCommand("mget key1 key2 key3 key4");
    db.Call(ctx, mget, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_ARRAY, "Failed to mget");
    CHECK_FATAL(ctx.reply.MemberSize() != 4, "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(0)->type != REDIS_REPLY_STRING, "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(1)->type != REDIS_REPLY_STRING, "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(2)->type != REDIS_REPLY_STRING, "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(3)->type != REDIS_REPLY_STRING, "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(0)->str != "4", "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(1)->str != "3", "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(2)->str != "2", "Failed to mget");
    CHECK_FATAL(ctx.reply.elements->at(3)->str != "1", "Failed to mget");

    RedisCommandFrame del;
    del.SetFullCommand("del key100");
    db.Call(ctx, del, 0);
    RedisCommandFrame msetnx;
    msetnx.SetFullCommand("msetnx key1 4 key2 3 key3 2 key4 1 key100 100");
    db.Call(ctx, msetnx, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to msetnx");
    CHECK_FATAL(ctx.reply.integer != 0, "Failed to msetnx");

    del.SetFullCommand("del key1 key2 key3 key4");
    db.Call(ctx, del, 0);
    db.Call(ctx, msetnx, 0);
    CHECK_FATAL(ctx.reply.type != REDIS_REPLY_INTEGER, "Failed to msetnx");
    CHECK_FATAL(ctx.reply.integer != 1, "Failed to msetnx");
}

void test_string(Ardb& db)
{
    Context tmpctx;
    test_strings_append(tmpctx, db);
    test_strings_getrange(tmpctx, db);
    test_strings_setrange(tmpctx, db);
    test_strings_getset(tmpctx, db);
    test_strings_strlen(tmpctx, db);
    test_strings_incrdecr(tmpctx, db);
    test_strings_set(tmpctx, db);
    test_strings_mget(tmpctx, db);
    test_strings_mset(tmpctx, db);
}

