/*
 * zset_testcase.cpp
 *
 *  Created on: 2"0"13-4-9
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_zsets_addrem(Ardb& db)
{
	db.ZClear("0", "myzset");
	db.ZAdd("0", "myzset", 3, "v0");
	db.ZAdd("0", "myzset", 2, "v1");
	db.ZAdd("0", "myzset", 1, "v2");
	CHECK_FATAL(db.ZCard("0", "myzset") != 3,
			"zadd myzset failed:%d", db.ZCard("0", "myzset"));
	double score = 0;
	db.ZScore("0", "myzset", "v1", score);
	CHECK_FATAL( score != 2, "zscore myzset failed:%f", score);

	db.ZAdd("0", "myzset", 0, "v2");
	CHECK_FATAL(db.ZCard("0", "myzset") != 3,
			"zadd myzset failed:%d", db.ZCard("0", "myzset"));
	db.ZRem("0", "myzset", "v2");
	CHECK_FATAL( db.ZCard("0", "myzset") != 2,
			"zrem myzset failed:%d", db.ZCard("0", "myzset"));
	ValueArray values;
	QueryOptions options;
	db.ZRange("0", "myzset", 0, -1, values, options);
	std::string str;
	CHECK_FATAL(values.size() != 2, "Fail:%d", values.size());
	CHECK_FATAL( values[0].ToString(str) != "v1",
			"Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL(values[1].ToString(str) != "v0",
			"Fail:%s", values[1].ToString(str).c_str());
}

void test_zsets_zrange(Ardb& db)
{
	db.ZClear("0", "myzset");
	db.ZAdd("0", "myzset", 1, "one");
	db.ZAdd("0", "myzset", 1, "uno");
	db.ZAdd("0", "myzset", 2, "two");
	db.ZAdd("0", "myzset", 3, "two");
	QueryOptions options;
	options.withscores = true;
	ValueArray values;
	db.ZRange("0", "myzset", 0, -1, values, options);
	std::string str;
	CHECK_FATAL( values.size() != 6, "Fail:%d", values.size());
	CHECK_FATAL( values[0].ToString(str) != "one",
			"Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL( values[2].ToString(str) != "uno",
			"Fail:%s", values[1].ToString(str).c_str());
	values.clear();
	db.ZRangeByScore("0", "myzset", "-inf", "+inf", values, options);
	CHECK_FATAL( values.size() != 6, "Fail:%d", values.size());
	CHECK_FATAL( values[0].ToString(str) != "one",
			"Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL( values[2].ToString(str) != "uno",
			"Fail:%s", values[1].ToString(str).c_str());
	values.clear();
	db.ZRangeByScore("0", "myzset", "(1", "3", values, options);
	CHECK_FATAL( values.size() != 2, "Fail:", values.size());
	CHECK_FATAL( values[0].ToString(str) != "two",
			"Fail:%s", values[0].ToString(str).c_str());
}

void test_zsets_zcount(Ardb& db)
{
	db.ZClear("0", "myzset");
	db.ZAdd("0", "myzset", 1, "one");
	db.ZAdd("0", "myzset", 2, "two");
	db.ZAdd("0", "myzset", 3, "three");
	int count = db.ZCount("0", "myzset", "-inf", "+inf");
	CHECK_FATAL( count != 3, "Fail:%d", count);
	count = db.ZCount("0", "myzset", "(1", "3");
	CHECK_FATAL( count != 2, "Fail:%d", count);
}

void test_zsets_zrank(Ardb& db)
{
	db.ZClear("0", "myzset");
	db.ZAdd("0", "myzset", 1, "one");
	db.ZAdd("0", "myzset", 2, "two");
	db.ZAdd("0", "myzset", 3, "three");
	int rank = db.ZRank("0", "myzset", "three");
	CHECK_FATAL( rank != 2, "Fail:%d", rank);
}

void test_zsets_zrem(Ardb& db)
{
	db.ZClear("0", "myzset");
	db.ZAdd("0", "myzset", 1, "one");
	db.ZAdd("0", "myzset", 2, "two");
	db.ZAdd("0", "myzset", 3, "three");
	int count = db.ZRemRangeByRank("0", "myzset", 0, 1);
	CHECK_FATAL( count != 2, "Fail:%d", count);
	db.ZAdd("0", "myzset", 1, "one");
	db.ZAdd("0", "myzset", 2, "two");
	count = db.ZRemRangeByScore("0", "myzset", "-inf", "(2");
	CHECK_FATAL( count != 1, "Fail:%d", count);
}

void test_zsets_zrev(Ardb& db)
{
	db.ZClear("0", "myzset");
	db.ZAdd("0", "myzset", 1, "one");
	db.ZAdd("0", "myzset", 2, "two");
	db.ZAdd("0", "myzset", 3, "three");
	QueryOptions options;
	options.withscores = true;
	ValueArray values;
	std::string str;
	db.ZRevRange("0", "myzset", 0, -1, values, options);
	CHECK_FATAL( values.size() != 6, "Fail:%d", values.size());
	CHECK_FATAL( values[0].ToString(str) != "three",
			"Fail:", values[0].ToString(str).c_str());
	CHECK_FATAL( values[2].ToString(str) != "two",
			"Fail:", values[2].ToString(str).c_str());
	values.clear();
	db.ZRevRangeByScore("0", "myzset", "+inf", "-inf", values, options);
	CHECK_FATAL( values.size() != 6, "Fail:%d", values.size());
	CHECK_FATAL( values[0].ToString(str) != "three",
			"Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL( values[2].ToString(str) != "two",
			"Fail:%s", values[2].ToString(str).c_str());
	options.withscores = false;
	values.clear();
	db.ZRevRangeByScore("0", "myzset", "2", "1", values, options);
	CHECK_FATAL( values.size() != 2, "Fail:%d", values.size());
	CHECK_FATAL( values[0].ToString(str) != "two",
			"Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL( values[1].ToString(str) != "one",
			"Fail:%s", values[1].ToString(str).c_str());
	values.clear();
	db.ZRevRangeByScore("0", "myzset", "2", "(1", values, options);
	CHECK_FATAL( values.size() != 1, "Fail:%d", values.size());
	CHECK_FATAL( values[0].ToString(str) != "two",
			"Fail:%s", values[0].ToString(str).c_str());
}

void test_zsets_incr(Ardb& db)
{
	db.ZClear("0", "myzset");
	db.ZAdd("0", "myzset", 1, "one");
	db.ZAdd("0", "myzset", 2, "two");
	double score;
	db.ZIncrby("0", "myzset", 2, "one", score);
	QueryOptions options;
	options.withscores = true;
	ValueArray values;
	db.ZRange("0", "myzset", 0, -1, values, options);
	std::string str;
	CHECK_FATAL( values.size() != 4, "Fail:%d", values.size());
	CHECK_FATAL( values[0].ToString(str) != "two",
			"Fail:%s", values[0].ToString(str).c_str());
}

void test_zsets_inter(Ardb& db)
{
	db.ZClear("0", "myzset1");
	db.ZClear("0", "myzset2");
	db.ZAdd("0", "myzset1", 1, "one");
	db.ZAdd("0", "myzset1", 2, "two");
	db.ZAdd("0", "myzset2", 1, "one");
	db.ZAdd("0", "myzset2", 2, "two");
	db.ZAdd("0", "myzset2", 3, "three");
	SliceArray keys;
	keys.push_back("myzset1");
	keys.push_back("myzset2");
	WeightArray ws;
	ws.push_back(20);
	ws.push_back(4);
	db.ZInterStore("0", "myzset3", keys, ws);

	CHECK_FATAL( db.ZCard("0", "myzset3") != 2, "Fail:");
	QueryOptions options;
	options.withscores = true;
	ValueArray values;
	db.ZRange("0", "myzset3", 0, -1, values, options);
	std::string str;
	CHECK_FATAL( values.size() != 4, "Fail:%d", values.size());
	CHECK_FATAL( values[0].ToString(str) != "one", "Fail:");
}

void test_zsets_union(Ardb& db)
{
	db.ZClear("0", "myzset1");
	db.ZClear("0", "myzset2");
	db.ZAdd("0", "myzset1", 1, "one");
	db.ZAdd("0", "myzset1", 2, "two");
	db.ZAdd("0", "myzset2", 1, "one");
	db.ZAdd("0", "myzset2", 2, "two");
	db.ZAdd("0", "myzset2", 3, "three");
	SliceArray keys;
	keys.push_back("myzset1");
	keys.push_back("myzset2");
	WeightArray ws;
	ws.push_back(10);
	ws.push_back(4);
	db.ZUnionStore("0", "myzset3", keys, ws);

	CHECK_FATAL( db.ZCard("0", "myzset3") != 3, "Fail:");
	QueryOptions options;
	options.withscores = true;
	ValueArray values;
	db.ZRange("0", "myzset3", 0, -1, values, options);
	std::string str;
	CHECK_FATAL( values.size() != 6, "Fail:%d", values.size());
	CHECK_FATAL( values[0].ToString(str) != "three",
			"Fail:%s", values[0].ToString(str).c_str());
	CHECK_FATAL( values[2].ToString(str) != "one",
			"Fail:%s", values[2].ToString(str).c_str());
	CHECK_FATAL( values[4].ToString(str) != "two",
			"Fail:%s", values[4].ToString(str).c_str());
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
}
