/*
 * lists_testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: yinqiwen
 */
#include "db.hpp"
#include <string>

using namespace ardb;

void test_lists_zip_lpush(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    int ret = db.LPushx(dbid, "mylist", "value0");
    CHECK_FATAL(ret != 0, "lpushx mylist failed:%d", ret);
    db.LPush(dbid, "mylist", "value0");
    db.LPush(dbid, "mylist", "value1");
    db.LPush(dbid, "mylist", "value2");
    db.LPush(dbid, "mylist", "value3");
    CHECK_FATAL(db.LLen(dbid, "mylist") != 4, "lpush/llen mylist failed:%d", db.LLen(dbid, "mylist"));
    std::string v;
    ret = db.LIndex(dbid, "mylist", 0, v);
    CHECK_FATAL(v != "value3", "LIndex failed %d:%s", ret, v.c_str());
}

void test_lists_nonzip_lpush(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    for (uint32 i = 0; i < (uint32) (db.GetConfig().list_max_ziplist_entries + 10); i++)
    {
        char value[16];
        sprintf(value, "value%u", i);
        int ret = db.LPush(dbid, "mylist", value);
        CHECK_FATAL(ret <= 0, "lpush mylist failed:%d", ret);
    }
    CHECK_FATAL(db.LLen(dbid, "mylist") != (db.GetConfig().list_max_ziplist_entries + 10),
            "lpush/llen mylist failed:%d", db.LLen(dbid, "mylist"));
    std::string v;
    int ret = db.LIndex(dbid, "mylist", db.GetConfig().list_max_ziplist_entries + 10 - 1, v);
    CHECK_FATAL(v != "value0", "LIndex failed %d:%s", ret, v.c_str());
}

void test_lists_zip_rpush(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    int ret = db.RPushx(dbid, "mylist", "value0");
    CHECK_FATAL(ret != 0, "lpushx mylist failed:%d", ret);
    db.RPush(dbid, "mylist", "value0");
    db.RPush(dbid, "mylist", "value1");
    db.RPush(dbid, "mylist", "value2");
    db.RPush(dbid, "mylist", "value3");
    CHECK_FATAL(db.LLen(dbid, "mylist") != 4, "lrpush/llen mylist failed:%d", db.LLen(dbid, "mylist"));
    std::string v;
    db.LIndex(dbid, "mylist", 3, v);
    CHECK_FATAL(v != "value3", "LIndex failed:%s", v.c_str());
}

void test_lists_nonzip_rpush(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    for (uint32 i = 0; i < (uint32) (db.GetConfig().list_max_ziplist_entries + 10); i++)
    {
        char value[16];
        sprintf(value, "value%u", i);
        db.RPush(dbid, "mylist", value);
    }
    CHECK_FATAL(db.LLen(dbid, "mylist") != (db.GetConfig().list_max_ziplist_entries + 10),
            "lpush/llen mylist failed:%d", db.LLen(dbid, "mylist"));
    std::string v;
    int ret = db.LIndex(dbid, "mylist", 17, v);
    CHECK_FATAL(v != "value17", "LIndex failed %d:%s", ret, v.c_str());
}

void test_lists_zip_insert(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    db.RPush(dbid, "mylist", "value0");
    db.RPush(dbid, "mylist", "value1");
    db.RPush(dbid, "mylist", "value3");
    db.RPush(dbid, "mylist", "value4");
    db.LInsert(dbid, "mylist", "before", "value3", "value2");
    CHECK_FATAL(db.LLen(dbid, "mylist") != 5, "lrpush/llen mylist failed:%d", db.LLen(dbid, "mylist"));
    std::string v;
    db.LIndex(dbid, "mylist", 2, v);
    CHECK_FATAL(v != "value2", "LInsert failed:%s", v.c_str());
    db.LSet(dbid, "mylist", 2, "valuex");
    db.LIndex(dbid, "mylist", 2, v);
    CHECK_FATAL(v != "valuex", "LSet failed:%s", v.c_str());
    std::string vstr;
    db.LPop(dbid, "mylist", vstr);
    CHECK_FATAL(vstr != "value0", "LPop failed:%s", vstr.c_str());
}

void test_lists_nonzip_insert(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    db.RPush(dbid, "mylist", "value0");
    db.RPush(dbid, "mylist", "value1");
    db.RPush(dbid, "mylist", "value3");
    db.RPush(dbid, "mylist", "value4");
    for (uint32 i = 0; i < 20; i++)
    {
        db.LInsert(dbid, "mylist", "before", "value3", "value2.5");
    }
    CHECK_FATAL(db.LLen(dbid, "mylist") != 24, "lrpush/llen mylist failed:%d", db.LLen(dbid, "mylist"));
    std::string v;
    db.LIndex(dbid, "mylist", 2, v);
    CHECK_FATAL(v != "value2.5", "LInsert failed:%s", v.c_str());
}

void test_lists_zip_lrange(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    db.RPush(dbid, "mylist", "value0");
    db.RPush(dbid, "mylist", "value1");
    db.RPush(dbid, "mylist", "value3");
    db.RPush(dbid, "mylist", "value4");

    ValueDataArray array;
    db.LRange(dbid, "mylist", 1, 2, array);
    CHECK_FATAL(array.size() != 2, "lrange failed:");
    array.clear();
    db.LRange(dbid, "mylist", 1, -1, array);
    CHECK_FATAL(array.size() != 3, "lrange failed:");
}

void test_lists_nonzip_lrange(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    for (uint32 i = 0; i < (uint32) (db.GetConfig().list_max_ziplist_entries + 10); i++)
    {
        char value[16];
        sprintf(value, "value%u", i);
        db.RPush(dbid, "mylist", value);
    }

    ValueDataArray array;
    db.LRange(dbid, "mylist", 1, 2, array);
    CHECK_FATAL(array.size() != 2, "lrange failed:");
    array.clear();
    db.LRange(dbid, "mylist", 1, -1, array);
    CHECK_FATAL(array.size() != (uint32 ) (db.GetConfig().list_max_ziplist_entries + 9), "lrange failed:");
}

void test_lists_zip_ltrim(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    for (uint32 i = 0; i < (uint32) (db.GetConfig().list_max_ziplist_entries + 10); i++)
    {
        char value[16];
        sprintf(value, "value%u", i);
        db.RPush(dbid, "mylist", value);
    }
    db.LTrim(dbid, "mylist", 0, 3);
    CHECK_FATAL(db.LLen(dbid, "mylist") != 4, "ltrim mylist failed:%d", db.LLen(dbid, "mylist"));
}

void test_lists_nonzip_ltrim(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    db.RPush(dbid, "mylist", "value0");
    db.RPush(dbid, "mylist", "value1");
    db.RPush(dbid, "mylist", "value3");
    db.RPush(dbid, "mylist", "value4");
    db.LTrim(dbid, "mylist", 0, 2);
    CHECK_FATAL(db.LLen(dbid, "mylist") != 3, "ltrim mylist failed:%d", db.LLen(dbid, "mylist"));
}

void test_lists_zip_lrem(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    db.RPush(dbid, "mylist", "value0");
    db.RPush(dbid, "mylist", "value0");
    db.RPush(dbid, "mylist", "value0");
    db.RPush(dbid, "mylist", "value4");
    db.RPush(dbid, "mylist", "value0");
    db.LRem(dbid, "mylist", -3, "value0");
    CHECK_FATAL(db.LLen(dbid, "mylist") != 2, "lrem mylist failed:%d", db.LLen(dbid, "mylist"));
    db.RPush(dbid, "mylist", "value0");
    db.RPush(dbid, "mylist", "value0");
    db.LRem(dbid, "mylist", 3, "value4");
    CHECK_FATAL(db.LLen(dbid, "mylist") != 3, "lrem mylist failed:%d", db.LLen(dbid, "mylist"));
}

void test_list_expire(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    db.RPush(dbid, "mylist", "value0");
    db.Expire(dbid, "mylist", 1);
    CHECK_FATAL(db.Exists(dbid, "mylist") == false, "Expire mylist failed");
    sleep(2);
    CHECK_FATAL(db.Exists(dbid, "mylist") == true, "Expire mylist failed");
}

void test_lists(Ardb& db)
{
    test_lists_zip_lpush(db);
    test_lists_nonzip_lpush(db);
    test_lists_zip_rpush(db);
    test_lists_nonzip_rpush(db);
    test_lists_zip_insert(db);
    test_lists_zip_lrange(db);
    test_lists_zip_lrem(db);
    test_lists_zip_ltrim(db);
    test_lists_nonzip_insert(db);
    test_lists_nonzip_ltrim(db);
    test_lists_nonzip_lrange(db);

    test_list_expire(db);
}

