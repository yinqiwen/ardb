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
	db.ZAdd(dbid, "myzset", 3, "v0");
	db.ZAdd(dbid, "myzset", 2, "v1");
	db.ZAdd(dbid, "myzset", 1, "v2");
	CHECK_FATAL(db.ZCard(dbid, "myzset") != 3,
	        "zadd myzset failed:%d", db.ZCard(dbid, "myzset"));
	double score = 0;
	db.ZScore(dbid, "myzset", "v1", score);
	CHECK_FATAL( score != 2, "zscore myzset failed:%f", score);

	db.ZAdd(dbid, "myzset", 0, "v2");
	CHECK_FATAL(db.ZCard(dbid, "myzset") != 3,
	        "zadd myzset failed:%d", db.ZCard(dbid, "myzset"));
	db.ZRem(dbid, "myzset", "v2");
	CHECK_FATAL( db.ZCard(dbid, "myzset") != 2,
	        "zrem myzset failed:%d", db.ZCard(dbid, "myzset"));
	ValueArray values;
	QueryOptions options;
	db.ZRange(dbid, "myzset", 0, -1, values, options);
	std::string str;
	CHECK_FATAL(values.size() != 2, "Fail:%zu", values.size());
	CHECK_FATAL( values[0].ToString(str) != "v1",
	        "Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL(values[1].ToString(str) != "v0",
	        "Fail:%s", values[1].ToString(str).c_str());
}

void test_zsets_zrange(Ardb& db)
{
	DBID dbid = 0;
	db.ZClear(dbid, "myzset");
	db.ZAdd(dbid, "myzset", 1, "one");
	db.ZAdd(dbid, "myzset", 1, "uno");
	db.ZAdd(dbid, "myzset", 2, "two");
	db.ZAdd(dbid, "myzset", 3, "two");
	QueryOptions options;
	options.withscores = true;
	ValueArray values;
	db.ZRange(dbid, "myzset", 0, -1, values, options);
	std::string str;
	CHECK_FATAL( values.size() != 6, "Fail:%zu", values.size());
	CHECK_FATAL( values[0].ToString(str) != "one",
	        "Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL( values[2].ToString(str) != "uno",
	        "Fail:%s", values[1].ToString(str).c_str());
	values.clear();
	db.ZRangeByScore(dbid, "myzset", "-inf", "+inf", values, options);
	CHECK_FATAL( values.size() != 6, "Fail:%zu", values.size());
	CHECK_FATAL( values[0].ToString(str) != "one",
	        "Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL( values[2].ToString(str) != "uno",
	        "Fail:%s", values[1].ToString(str).c_str());
	values.clear();
	db.ZRangeByScore(dbid, "myzset", "(1", "3", values, options);
	CHECK_FATAL( values.size() != 2, "Fail:%zu", values.size());
	CHECK_FATAL( values[0].ToString(str) != "two",
	        "Fail:%s", values[0].ToString(str).c_str());
}

void test_zsets_zcount(Ardb& db)
{
	DBID dbid = 0;
	db.ZClear(dbid, "myzset");
	db.ZAdd(dbid, "myzset", 1, "one");
	db.ZAdd(dbid, "myzset", 2, "two");
	db.ZAdd(dbid, "myzset", 3, "three");
	int count = db.ZCount(dbid, "myzset", "-inf", "+inf");
	CHECK_FATAL( count != 3, "Fail:%d", count);
	count = db.ZCount(dbid, "myzset", "(1", "3");
	CHECK_FATAL( count != 2, "Fail:%d", count);
}

void test_zsets_zrank(Ardb& db)
{
	DBID dbid = 0;
	db.ZClear(dbid, "myzset");
	db.ZAdd(dbid, "myzset", 1, "one");
	db.ZAdd(dbid, "myzset", 2, "two");
	db.ZAdd(dbid, "myzset", 3, "three");
	db.ZCount(dbid, "myzset", "-inf", "+inf");
	int rank = db.ZRank(dbid, "myzset", "three");
	CHECK_FATAL( rank != 2, "Fail:%d", rank);
}

void test_zsets_zrem(Ardb& db)
{
	DBID dbid = 0;
	db.ZClear(dbid, "myzset");
	db.ZAdd(dbid, "myzset", 1, "one");
	db.ZAdd(dbid, "myzset", 2, "two");
	db.ZAdd(dbid, "myzset", 3, "three");
	int count = db.ZRemRangeByRank(dbid, "myzset", 0, 1);
	CHECK_FATAL( count != 2, "Fail:%d", count);
	db.ZAdd(dbid, "myzset", 1, "one");
	db.ZAdd(dbid, "myzset", 2, "two");
	count = db.ZRemRangeByScore(dbid, "myzset", "-inf", "(2");
	CHECK_FATAL( count != 1, "Fail:%d", count);
}

void test_zsets_zrev(Ardb& db)
{
	DBID dbid = 0;
	db.ZClear(dbid, "myzset");
	db.ZAdd(dbid, "myzset", 1, "one");
	db.ZAdd(dbid, "myzset", 2, "two");
	db.ZAdd(dbid, "myzset", 3, "three");
	QueryOptions options;
	options.withscores = true;
	ValueArray values;
	std::string str;
	db.ZRevRange(dbid, "myzset", 0, -1, values, options);
	CHECK_FATAL( values.size() != 6, "Fail:%zu", values.size());
	CHECK_FATAL( values[0].ToString(str) != "three",
	        "Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL( values[2].ToString(str) != "two",
	        "Fail:%s", values[2].ToString(str).c_str());
	values.clear();
	db.ZRevRangeByScore(dbid, "myzset", "+inf", "-inf", values, options);
	CHECK_FATAL( values.size() != 6, "Fail:%zu", values.size());
	CHECK_FATAL( values[0].ToString(str) != "three",
	        "Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL( values[2].ToString(str) != "two",
	        "Fail:%s", values[2].ToString(str).c_str());
	options.withscores = false;
	values.clear();
	db.ZRevRangeByScore(dbid, "myzset", "2", "1", values, options);
	CHECK_FATAL( values.size() != 2, "Fail:%zu", values.size());
	CHECK_FATAL( values[0].ToString(str) != "two",
	        "Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL( values[1].ToString(str) != "one",
	        "Fail:%s", values[1].ToString(str).c_str());
	values.clear();
	db.ZRevRangeByScore(dbid, "myzset", "2", "(1", values, options);
	CHECK_FATAL( values.size() != 1, "Fail:%zu", values.size());
	CHECK_FATAL( values[0].ToString(str) != "two",
	        "Fail:%s", values[0].ToString(str).c_str());
}

void test_zsets_incr(Ardb& db)
{
	DBID dbid = 0;
	db.ZClear(dbid, "myzset");
	db.ZAdd(dbid, "myzset", 1, "one");
	db.ZAdd(dbid, "myzset", 2, "two");
	double score;
	db.ZIncrby(dbid, "myzset", 2, "one", score);
	QueryOptions options;
	options.withscores = true;
	ValueArray values;
	db.ZRange(dbid, "myzset", 0, -1, values, options);
	std::string str;
	CHECK_FATAL( values.size() != 4, "Fail:%zu", values.size());
	CHECK_FATAL( values[0].ToString(str) != "two",
	        "Fail:%s", values[0].ToString(str).c_str());
}

void test_zsets_inter(Ardb& db)
{
	DBID dbid = 0;
	db.ZClear(dbid, "myzset1");
	db.ZClear(dbid, "myzset2");
	db.ZAdd(dbid, "myzset1", 1, "one");
	db.ZAdd(dbid, "myzset1", 2, "two");
	db.ZAdd(dbid, "myzset2", 1, "one");
	db.ZAdd(dbid, "myzset2", 2, "two");
	db.ZAdd(dbid, "myzset2", 3, "three");
	SliceArray keys;
	keys.push_back("myzset1");
	keys.push_back("myzset2");
	WeightArray ws;
	ws.push_back(20);
	ws.push_back(4);
	db.ZInterStore(dbid, "myzset3", keys, ws);

	CHECK_FATAL( db.ZCard(dbid, "myzset3") != 2, "Fail:");
	QueryOptions options;
	options.withscores = true;
	ValueArray values;
	db.ZRange(dbid, "myzset3", 0, -1, values, options);
	std::string str;
	CHECK_FATAL( values.size() != 4, "Fail:%zu", values.size());
	CHECK_FATAL( values[0].ToString(str) != "one", "Fail:");
}

void test_zsets_union(Ardb& db)
{
	DBID dbid = 0;
	db.ZClear(dbid, "myzset1");
	db.ZClear(dbid, "myzset2");
	db.ZAdd(dbid, "myzset1", 1, "one");
	db.ZAdd(dbid, "myzset1", 2, "two");
	db.ZAdd(dbid, "myzset2", 1, "one");
	db.ZAdd(dbid, "myzset2", 2, "two");
	db.ZAdd(dbid, "myzset2", 3, "three");
	SliceArray keys;
	keys.push_back("myzset1");
	keys.push_back("myzset2");
	WeightArray ws;
	ws.push_back(10);
	ws.push_back(4);
	db.ZUnionStore(dbid, "myzset3", keys, ws);

	CHECK_FATAL( db.ZCard(dbid, "myzset3") != 3, "Fail:");
	QueryOptions options;
	options.withscores = true;
	ValueArray values;
	db.ZRange(dbid, "myzset3", 0, -1, values, options);
	std::string str;
	CHECK_FATAL( values.size() != 6, "Fail:%zu", values.size());
	CHECK_FATAL( values[0].ToString(str) != "three",
	        "Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL( values[2].ToString(str) != "one",
	        "Fail:%s", values[2].ToString(str).c_str());
	CHECK_FATAL( values[4].ToString(str) != "two",
	        "Fail:%s", values[4].ToString(str).c_str());
}

void test_zset_expire(Ardb& db)
{
	DBID dbid = 0;
	db.ZClear(dbid, "myzset");
	db.ZAdd(dbid, "myzset", 1, "one");
	db.Expire(dbid, "myzset", 1);
	CHECK_FATAL(db.Exists(dbid, "myzset") == false, "Expire myzset failed");
	sleep(2);
	CHECK_FATAL(db.Exists(dbid, "myzset") == true, "Expire myzset failed");
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
}
