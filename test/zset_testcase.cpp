/*
 * zset_testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: yinqiwen
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_zsets_addrem(Ardb& db)
{
    DBID dbid = 0;
    db.ZClear(dbid, "myzset");

    db.ZAdd(dbid, "myzset", ValueData((int64) 3), "v0");
    db.ZAdd(dbid, "myzset", ValueData((int64) 2), "v1");
    db.ZAdd(dbid, "myzset", ValueData((int64) 1), "v2");
    CHECK_FATAL(db.ZCard(dbid, "myzset") != 3, "zadd myzset failed:%d", db.ZCard(dbid, "myzset"));
    ValueData score((int64) 0);
    db.ZScore(dbid, "myzset", "v1", score);
    CHECK_FATAL(score.NumberValue() != 2, "zscore myzset failed:%f", score.NumberValue());

    db.ZAdd(dbid, "myzset", ValueData((int64) 0), "v2");
    CHECK_FATAL(db.ZCard(dbid, "myzset") != 3, "zadd myzset failed:%d", db.ZCard(dbid, "myzset"));
    db.ZRem(dbid, "myzset", "v2");
    CHECK_FATAL(db.ZCard(dbid, "myzset") != 2, "zrem myzset failed:%d", db.ZCard(dbid, "myzset"));
    ValueDataArray values;
    ZSetQueryOptions options;
    db.ZRange(dbid, "myzset", 0, -1, values, options);
    std::string str;
    CHECK_FATAL(values.size() != 2, "Fail:%zu", values.size());
    CHECK_FATAL(values[0].ToString(str) != "v1", "Fail:%s", values[0].ToString(str).c_str());
    CHECK_FATAL(values[1].ToString(str) != "v0", "Fail:%s", values[1].ToString(str).c_str());
}

void test_zsets_zrange(Ardb& db)
{
    DBID dbid = 0;
    db.ZClear(dbid, "myzset");
    db.ZAdd(dbid, "myzset", ValueData((int64) 1), "one");
    db.ZAdd(dbid, "myzset", ValueData((int64) 1), "uno");
    db.ZAdd(dbid, "myzset", ValueData((int64) 2), "two");
    db.ZAdd(dbid, "myzset", ValueData((int64) 3), "two");
    ZSetQueryOptions options;
    options.withscores = true;
    ValueDataArray values;
    db.ZRange(dbid, "myzset", 0, -1, values, options);
    std::string str;
    CHECK_FATAL(values.size() != 6, "Fail:%zu", values.size());
    CHECK_FATAL(values[0].ToString(str) != "one", "Fail:%s", values[0].ToString(str).c_str());
    CHECK_FATAL(values[2].ToString(str) != "uno", "Fail:%s", values[1].ToString(str).c_str());
    values.clear();
    db.ZRangeByScore(dbid, "myzset", "-inf", "+inf", values, options);
    CHECK_FATAL(values.size() != 6, "Fail:%zu", values.size());
    CHECK_FATAL(values[0].ToString(str) != "one", "Fail:%s", values[0].ToString(str).c_str());
    CHECK_FATAL(values[2].ToString(str) != "uno", "Fail:%s", values[1].ToString(str).c_str());
    values.clear();
    db.ZRangeByScore(dbid, "myzset", "(1", "3", values, options);
    CHECK_FATAL(values.size() != 2, "Fail:%zu", values.size());
    CHECK_FATAL(values[0].ToString(str) != "two", "Fail:%s", values[0].ToString(str).c_str());

    db.ZClear(dbid, "myzset");
    uint32 maxzsetsize = 1000;
    for (uint32 i = 0; i < maxzsetsize; i++)
    {
        char value[16];
        sprintf(value, "value%u", i);
        db.ZAdd(dbid, "myzset", ValueData((int64) i), value);
    }
    options.withscores = false;
    values.clear();
    db.ZRange(dbid, "myzset", 1, 10, values, options);
    CHECK_FATAL(values.size() != 10, "Fail:%zu", values.size());
}

void test_zsets_zcount(Ardb& db)
{
    DBID dbid = 0;
    db.ZClear(dbid, "myzset");
    db.ZAdd(dbid, "myzset", ValueData((int64) 1), "one");
    db.ZAdd(dbid, "myzset", ValueData((int64) 2), "two");
    db.ZAdd(dbid, "myzset", ValueData((int64) 3), "three");
    int count = db.ZCount(dbid, "myzset", "-inf", "+inf");
    CHECK_FATAL(count != 3, "Fail:%d", count);
    count = db.ZCount(dbid, "myzset", "(1", "3");
    CHECK_FATAL(count != 2, "Fail:%d", count);
}

void test_zsets_zrank(Ardb& db)
{
    DBID dbid = 0;
    db.ZClear(dbid, "myzset");
    db.ZAdd(dbid, "myzset", ValueData((int64) 1), "one");
    db.ZAdd(dbid, "myzset", ValueData((int64) 2), "two");
    db.ZAdd(dbid, "myzset", ValueData((int64) 3), "three");
    db.ZCount(dbid, "myzset", "-inf", "+inf");
    int rank = db.ZRank(dbid, "myzset", "three");
    CHECK_FATAL(rank != 2, "Fail:%d", rank);

    db.ZClear(dbid, "myzset");
    uint32 maxzsetsize = 100000;
    uint64 start = get_current_epoch_millis();
    for (uint32 i = 0; i < maxzsetsize; i++)
    {
        char value[16];
        sprintf(value, "value%u", i);
        db.ZAdd(dbid, "myzset", ValueData((int64) i), value);
    }
    uint64 end = get_current_epoch_millis();
    INFO_LOG("Cost %lldms to write %u zset elements", (end - start), maxzsetsize);
    start = get_current_epoch_millis();
    rank = db.ZRank(dbid, "myzset", "value50001");
    CHECK_FATAL(rank != 50001, "Fail:%d", rank);
    end = get_current_epoch_millis();
    INFO_LOG("Cost %lldms to rank.", (end - start));
}

void test_zsets_zrem(Ardb& db)
{
    DBID dbid = 0;
    db.ZClear(dbid, "myzset");
    db.ZAdd(dbid, "myzset", ValueData((int64) 1), "one");
    db.ZAdd(dbid, "myzset", ValueData((int64) 2), "two");
    db.ZAdd(dbid, "myzset", ValueData((int64) 3), "three");
    int count = db.ZRemRangeByRank(dbid, "myzset", 0, 1);
    CHECK_FATAL(count != 2, "Fail:%d", count);
    db.ZAdd(dbid, "myzset", ValueData((int64) 1), "one");
    db.ZAdd(dbid, "myzset", ValueData((int64) 2), "two");
    count = db.ZRemRangeByScore(dbid, "myzset", "-inf", "(2");
    CHECK_FATAL(count != 1, "Fail:%d", count);
}

void test_zsets_zrev(Ardb& db)
{
    DBID dbid = 0;
    db.ZClear(dbid, "myzset");
    db.ZAdd(dbid, "myzset", ValueData((int64) 1), "one");
    db.ZAdd(dbid, "myzset", ValueData((int64) 2), "two");
    db.ZAdd(dbid, "myzset", ValueData((int64) 3), "three");
    ZSetQueryOptions options;
    options.withscores = true;
    ValueDataArray values;
    std::string str;
    db.ZRevRange(dbid, "myzset", 0, -1, values, options);
    CHECK_FATAL(values.size() != 6, "Fail:%zu", values.size());
    CHECK_FATAL(values[0].ToString(str) != "three", "Fail:%s", values[0].ToString(str).c_str());
    CHECK_FATAL(values[2].ToString(str) != "two", "Fail:%s", values[2].ToString(str).c_str());
    values.clear();
    db.ZRevRangeByScore(dbid, "myzset", "+inf", "-inf", values, options);
    CHECK_FATAL(values.size() != 6, "Fail:%zu", values.size());
    CHECK_FATAL(values[0].ToString(str) != "three", "Fail:%s", values[0].ToString(str).c_str());
    CHECK_FATAL(values[2].ToString(str) != "two", "Fail:%s", values[2].ToString(str).c_str());
    options.withscores = false;
    values.clear();
    db.ZRevRangeByScore(dbid, "myzset", "2", "1", values, options);
    CHECK_FATAL(values.size() != 2, "Fail:%zu", values.size());
    CHECK_FATAL(values[0].ToString(str) != "two", "Fail:%s", values[0].ToString(str).c_str());
    CHECK_FATAL(values[1].ToString(str) != "one", "Fail:%s", values[1].ToString(str).c_str());
    values.clear();
    db.ZRevRangeByScore(dbid, "myzset", "2", "(1", values, options);
    CHECK_FATAL(values.size() != 1, "Fail:%zu", values.size());
    CHECK_FATAL(values[0].ToString(str) != "two", "Fail:%s", values[0].ToString(str).c_str());
}

void test_zsets_incr(Ardb& db)
{
    DBID dbid = 0;
    db.ZClear(dbid, "myzset");
    db.ZAdd(dbid, "myzset", ValueData((int64) 1), "one");
    db.ZAdd(dbid, "myzset", ValueData((int64) 2), "two");
    ValueData score;
    db.ZIncrby(dbid, "myzset", ValueData((int64) 2), "one", score);
    ZSetQueryOptions options;
    options.withscores = true;
    ValueDataArray values;
    db.ZRange(dbid, "myzset", 0, -1, values, options);
    std::string str;
    CHECK_FATAL(values.size() != 4, "Fail:%zu", values.size());
    CHECK_FATAL(values[0].ToString(str) != "two", "Fail:%s", values[0].ToString(str).c_str());
}

void test_zsets_inter(Ardb& db)
{
    DBID dbid = 0;
    db.ZClear(dbid, "myzset1");
    db.ZClear(dbid, "myzset2");
    db.ZAdd(dbid, "myzset1", ValueData((int64) 1), "one");
    db.ZAdd(dbid, "myzset1", ValueData((int64) 2), "two");
    db.ZAdd(dbid, "myzset2", ValueData((int64) 1), "one");
    db.ZAdd(dbid, "myzset2", ValueData((int64) 2), "two");
    db.ZAdd(dbid, "myzset2", ValueData((int64) 3), "three");
    SliceArray keys;
    keys.push_back("myzset1");
    keys.push_back("myzset2");
    WeightArray ws;
    ws.push_back(20);
    ws.push_back(4);
    db.ZInterStore(dbid, "myzset3", keys, ws);

    int zcard = db.ZCard(dbid, "myzset3");
    CHECK_FATAL(zcard != 2, "Fail:%d", zcard);
    ZSetQueryOptions options;
    options.withscores = true;
    ValueDataArray values;
    db.ZRange(dbid, "myzset3", 0, -1, values, options);
    std::string str;
    CHECK_FATAL(values.size() != 4, "Fail:%zu", values.size());
    CHECK_FATAL(values[0].ToString(str) != "one", "Fail:");
}

void test_zsets_union(Ardb& db)
{
    DBID dbid = 0;
    db.ZClear(dbid, "myzset1");
    db.ZClear(dbid, "myzset2");
    db.ZAdd(dbid, "myzset1", ValueData((int64) 1), "one");
    db.ZAdd(dbid, "myzset1", ValueData((int64) 2), "two");
    db.ZAdd(dbid, "myzset2", ValueData((int64) 1), "one");
    db.ZAdd(dbid, "myzset2", ValueData((int64) 2), "two");
    db.ZAdd(dbid, "myzset2", ValueData((int64) 3), "three");
    SliceArray keys;
    keys.push_back("myzset1");
    keys.push_back("myzset2");
    WeightArray ws;
    ws.push_back(10);
    ws.push_back(4);
    db.ZUnionStore(dbid, "myzset3", keys, ws);

    CHECK_FATAL(db.ZCard(dbid, "myzset3") != 3, "Fail:");
    ZSetQueryOptions options;
    options.withscores = true;
    ValueDataArray values;
    db.ZRange(dbid, "myzset3", 0, -1, values, options);
    std::string str;
    CHECK_FATAL(values.size() != 6, "Fail:%zu", values.size());
    CHECK_FATAL(values[0].ToString(str) != "three", "Fail:%s", values[0].ToString(str).c_str());
    CHECK_FATAL(values[2].ToString(str) != "one", "Fail:%s", values[2].ToString(str).c_str());
    CHECK_FATAL(values[4].ToString(str) != "two", "Fail:%s", values[4].ToString(str).c_str());
}

void test_zset_expire(Ardb& db)
{
    DBID dbid = 0;
    db.ZClear(dbid, "myzset");
    db.ZAdd(dbid, "myzset", ValueData((int64) 1), "one");
    db.Expire(dbid, "myzset", 1);
    CHECK_FATAL(db.Exists(dbid, "myzset") == false, "Expire myzset failed");
    sleep(2);
    CHECK_FATAL(db.Exists(dbid, "myzset") == true, "Expire myzset failed");
}

void test_geo(Ardb& db)
{
    DBID dbid = 0;
    db.ZClear(dbid, "mygeo");
    double x = 300.3;
    double y = 300.3;

    double p_x = 1000.0;
    double p_y = 1000.0;
    double raius = 1000;
    GeoPointArray cmp;
    for (uint32 i = 0; i < 100000; i++)
    {
        char name[100];
        sprintf(name, "p%u", i);
        double xx = x + i * 0.1;
        double yy = y + i * 0.1;
        if (((xx - p_x) * (xx - p_x) + (yy - p_y) * (yy - p_y)) < raius * raius)
        {
            GeoPoint p;
            p.x = xx;
            p.y = yy;
            cmp.push_back(p);
        }
        GeoAddOptions add;
        add.x = x + i * 0.1;
        add.y = y + i * 0.1;
        add.value = name;
        db.GeoAdd(dbid, "mygeo", add);
    }
    if(db.GetL1Cache() != NULL)
    {
        db.GetL1Cache()->SyncLoad(dbid, "mygeo");
    }
    GeoSearchOptions options;
    StringArray args;
    std::string err;
    string_to_string_array("LOCATION 1000.0 1000.0 RADIUS 1000 ASC WITHCOORDINATES WITHDISTANCES", args);
    options.Parse(args, err);
    ValueDataDeque result;
    db.GeoSearch(dbid, "mygeo", options, result);
    CHECK_FATAL(cmp.size() != result.size() / 4, "Search failed with %u elements while expected %u", result.size() / 4,
            cmp.size());
    uint64 start = get_current_epoch_millis();
    for (uint32 i = 0; i < 10000; i++)
    {
        result.clear();
        options.x = p_x + i * 0.1;
        options.y = p_y + i * 0.1;
        options.radius = 100;
        db.GeoSearch(dbid, "mygeo", options, result);
    }
    uint64 end = get_current_epoch_millis();
    for (uint32 i = 0; i < result.size(); i += 4)
    {
        INFO_LOG("GeoPoint:%s x:%.2f, y:%.2f, distance:%.2f", result[i].bytes_value.c_str(),
                result[i + 1].NumberValue(), result[i + 2].NumberValue(), result[i + 3].NumberValue());
    }

    INFO_LOG("Found %d points for search while expected %d points", result.size() / 4, cmp.size());
    INFO_LOG("Cost %lldms to geo search 100000 zset elements 10000 times", (end - start));
}

void test_zsets(Ardb& db)
{
    test_zsets_addrem(db);
    test_zsets_zrange(db);
    test_zsets_zcount(db);
    test_zsets_zrank(db);
    test_zsets_zrem(db);
    test_zsets_zrev(db);
    test_zsets_incr(db);
    test_zsets_inter(db);
    test_zsets_union(db);
    test_zset_expire(db);
    test_geo(db);
}
