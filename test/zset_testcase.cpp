/*
 * zset_testcase.cpp
 *
 *  Created on: 2"0"13-4-9
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string>
#include <glog/logging.h>

using namespace ardb;

void test_zsets_addrem(Ardb& db)
{
	db.ZClear("0", "myzset");
	db.ZAdd("0", "myzset", 3, "v0");
	db.ZAdd("0", "myzset", 2, "v1");
	db.ZAdd("0", "myzset", 1, "v2");
	LOG_IF(FATAL, db.ZCard("0", "myzset") != 3)
	        << "zadd myzset failed:" << db.ZCard("0", "myzset");
	double score = 0;
	db.ZScore("0", "myzset", "v1", score);
	LOG_IF(FATAL, score != 2) << "zscore myzset failed:" << score;

	//														<< db.ZCard("0", "myzset");
	db.ZAdd("0", "myzset", 0, "v2");
	LOG_IF(FATAL, db.ZCard("0", "myzset") != 3)
	        << "zadd myzset failed:" << db.ZCard("0", "myzset");
	db.ZRem("0", "myzset", "v2");
	LOG_IF(FATAL, db.ZCard("0", "myzset") != 2)
	        << "zrem myzset failed:" << db.ZCard("0", "myzset");
	ValueArray values;
	QueryOptions options;
	db.ZRange("0", "myzset", 0, -1, values, options);
	LOG_IF(FATAL, values.size() != 2) << "Fail:" << values.size();
	LOG_IF(FATAL, values[0].ToString() != "v1") << "Fail:" << values[0].ToString();
	LOG_IF(FATAL, values[1].ToString() != "v0") << "Fail:" << values[1].ToString();
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
	LOG_IF(FATAL, values.size() != 6) << "Fail:" << values.size();
	LOG_IF(FATAL, values[0].ToString() != "one") << "Fail:" << values[0].ToString();
	LOG_IF(FATAL, values[2].ToString() != "uno") << "Fail:" << values[1].ToString();
	values.clear();
	db.ZRangeByScore("0", "myzset", "-inf", "+inf", values, options);
	LOG_IF(FATAL, values.size() != 6) << "Fail:" << values.size();
	LOG_IF(FATAL, values[0].ToString() != "one") << "Fail:" << values[0].ToString();
	LOG_IF(FATAL, values[2].ToString() != "uno") << "Fail:" << values[1].ToString();
	values.clear();
	db.ZRangeByScore("0", "myzset", "(1", "3", values, options);
	LOG_IF(FATAL, values.size() != 2) << "Fail:" << values.size();
	LOG_IF(FATAL, values[0].ToString() != "two") << "Fail:" << values[0].ToString();
}

void test_zsets_zcount(Ardb& db)
{
	db.ZClear("0", "myzset");
	db.ZAdd("0", "myzset", 1, "one");
	db.ZAdd("0", "myzset", 2, "two");
	db.ZAdd("0", "myzset", 3, "three");
	int count = db.ZCount("0", "myzset", "-inf", "+inf");
	LOG_IF(FATAL, count != 3) << "Fail:" << count;
	count = db.ZCount("0", "myzset", "(1", "3");
	LOG_IF(FATAL, count != 2) << "Fail:" << count;
}

void test_zsets_zrank(Ardb& db)
{
	db.ZClear("0", "myzset");
	db.ZAdd("0", "myzset", 1, "one");
	db.ZAdd("0", "myzset", 2, "two");
	db.ZAdd("0", "myzset", 3, "three");
	int rank = db.ZRank("0", "myzset", "three");
	LOG_IF(FATAL, rank != 2) << "Fail:" << rank;
}

void test_zsets_zrem(Ardb& db)
{
	db.ZClear("0", "myzset");
	db.ZAdd("0", "myzset", 1, "one");
	db.ZAdd("0", "myzset", 2, "two");
	db.ZAdd("0", "myzset", 3, "three");
	int count = db.ZRemRangeByRank("0", "myzset", 0, 1);
	LOG_IF(FATAL, count != 2) << "Fail:" << count;
	db.ZAdd("0", "myzset", 1, "one");
	db.ZAdd("0", "myzset", 2, "two");
	count = db.ZRemRangeByScore("0", "myzset", "-inf", "(2");
	LOG_IF(FATAL, count != 1) << "Fail:" << count;
}

void test_zsets_zrev(Ardb& db)
{
	db.ZClear("0", "myzset");
	db.ZAdd("0", "myzset", 1, "one");
	db.ZAdd("0", "myzset", 2, "two");
	db.ZAdd("0", "myzset", 3, "three");
	QueryOptions options;
	options.withscores = true;
	StringArray values;
	db.ZRevRange("0", "myzset", 0, -1, values, options);
	LOG_IF(FATAL, values.size() != 6) << "Fail:" << values.size();
	LOG_IF(FATAL, values[0] != "three") << "Fail:" << values[0];
	LOG_IF(FATAL, values[2] != "two") << "Fail:" << values[2];
	values.clear();
	db.ZRevRangeByScore("0", "myzset", "+inf", "-inf", values, options);
	LOG_IF(FATAL, values.size() != 6) << "Fail:" << values.size();
	LOG_IF(FATAL, values[0] != "three") << "Fail:" << values[0];
	LOG_IF(FATAL, values[2] != "two") << "Fail:" << values[2];
	options.withscores = false;
	values.clear();
	db.ZRevRangeByScore("0", "myzset", "2", "1", values, options);
	LOG_IF(FATAL, values.size() != 2) << "Fail:" << values.size();
	LOG_IF(FATAL, values[0] != "two") << "Fail:" << values[0];
	LOG_IF(FATAL, values[1] != "one") << "Fail:" << values[1];
	values.clear();
	db.ZRevRangeByScore("0", "myzset", "2", "(1", values, options);
	LOG_IF(FATAL, values.size() != 1) << "Fail:" << values.size();
	LOG_IF(FATAL, values[0] != "two") << "Fail:" << values[0];
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
	LOG_IF(FATAL, values.size() != 4) << "Fail:" << values.size();
	LOG_IF(FATAL, values[0].ToString() != "two") << "Fail:" << values[0].ToString();
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

	LOG_IF(FATAL, db.ZCard("0", "myzset3") != 2) << "Fail:";
	QueryOptions options;
	options.withscores = true;
	ValueArray values;
	db.ZRange("0", "myzset3", 0, -1, values, options);
	LOG_IF(FATAL, values.size() != 4) << "Fail:" << values.size();
	LOG_IF(FATAL, values[0].ToString() != "one") << "Fail:";
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

	LOG_IF(FATAL, db.ZCard("0", "myzset3") != 3) << "Fail:";
	QueryOptions options;
	options.withscores = true;
	ValueArray values;
	db.ZRange("0", "myzset3", 0, -1, values, options);
	LOG_IF(FATAL, values.size() != 6) << "Fail:" << values.size();
	LOG_IF(FATAL, values[0].ToString() != "three") << "Fail:" << values[0].ToString();
	LOG_IF(FATAL, values[2].ToString() != "one") << "Fail:" << values[2].ToString();
	LOG_IF(FATAL, values[4].ToString() != "two") << "Fail:" << values[4].ToString();
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
