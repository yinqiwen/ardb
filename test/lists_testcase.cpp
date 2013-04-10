/*
 * lists_testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string>
#include <glog/logging.h>

using namespace ardb;

void test_lists_lpush(Ardb& db)
{
	db.LClear(0, "mylist");
	int ret = db.LPushx(0, "mylist", "value0");
	LOG_IF(FATAL, ret != 0) << "lpushx mylist failed:" << ret;
	db.LPush(0, "mylist", "value0");
	db.LPush(0, "mylist", "value1");
	db.LPush(0, "mylist", "value2");
	db.LPush(0, "mylist", "value3");
	LOG_IF(FATAL, db.LLen(0, "mylist") != 4) << "lpush/llen mylist failed:"
														<< db.LLen(0, "mylist");
	std::string v;
	db.LIndex(0, "mylist", 0, v);
	LOG_IF(FATAL, v!="value3") << "LIndex failed:" << v;
}

void test_lists_rpush(Ardb& db)
{
	db.LClear(0, "mylist");
	int ret = db.RPushx(0, "mylist", "value0");
	LOG_IF(FATAL, ret != 0) << "lpushx mylist failed:" << ret;
	db.RPush(0, "mylist", "value0");
	db.RPush(0, "mylist", "value1");
	db.RPush(0, "mylist", "value2");
	db.RPush(0, "mylist", "value3");
	LOG_IF(FATAL, db.LLen(0, "mylist") != 4) << "lrpush/llen mylist failed:"
														<< db.LLen(0, "mylist");
	std::string v;
	db.LIndex(0, "mylist", 3, v);
	LOG_IF(FATAL, v != "value3") << "LIndex failed:" << v;
}

void test_lists_insert(Ardb& db)
{
	db.LClear(0, "mylist");
	db.RPush(0, "mylist", "value0");
	db.RPush(0, "mylist", "value1");
	db.RPush(0, "mylist", "value3");
	db.RPush(0, "mylist", "value4");
	db.LInsert(0, "mylist", "before", "value3", "value2");
	LOG_IF(FATAL, db.LLen(0, "mylist") != 5) << "lrpush/llen mylist failed:"
														<< db.LLen(0, "mylist");
	std::string v;
	db.LIndex(0, "mylist", 2, v);
	LOG_IF(FATAL, v != "value2") << "LInsert failed:" << v;
	db.LSet(0, "mylist", 2, "valuex");
	db.LIndex(0, "mylist", 2, v);
	LOG_IF(FATAL, v != "valuex") << "LSet failed:" << v;
	std::string vstr;
	db.LPop(0, "mylist", vstr);
	LOG_IF(FATAL, vstr != "value0") << "LPop failed:" << vstr;
}

void test_lists_lrange(Ardb& db)
{
	db.LClear(0, "mylist");
	db.RPush(0, "mylist", "value0");
	db.RPush(0, "mylist", "value1");
	db.RPush(0, "mylist", "value3");
	db.RPush(0, "mylist", "value4");

	StringArray array;
	db.LRange(0, "mylist", 1, 2, array);
	LOG_IF(FATAL, array.size() != 2) << "lrange failed:";
	array.clear();
	db.LRange(0, "mylist", 1, -1, array);
	LOG_IF(FATAL, array.size() != 3) << "lrange failed:";
}

void test_lists_ltrim(Ardb& db)
{
	db.LClear(0, "mylist");
	db.RPush(0, "mylist", "value0");
	db.RPush(0, "mylist", "value1");
	db.RPush(0, "mylist", "value3");
	db.RPush(0, "mylist", "value4");
	db.LTrim(0, "mylist", 0, 2);
	LOG_IF(FATAL, db.LLen(0, "mylist") != 3) << "ltrim mylist failed:"
														<< db.LLen(0, "mylist");
}

void test_lists_lrem(Ardb& db)
{
	db.LClear(0, "mylist");
	db.RPush(0, "mylist", "value0");
	db.RPush(0, "mylist", "value0");
	db.RPush(0, "mylist", "value0");
	db.RPush(0, "mylist", "value4");
	db.RPush(0, "mylist", "value0");
	db.LRem(0, "mylist", -3, "value0");
	LOG_IF(FATAL, db.LLen(0, "mylist") != 2) << "lrem mylist failed:"
														<< db.LLen(0, "mylist");
	db.RPush(0, "mylist", "value0");
	db.RPush(0, "mylist", "value0");
	db.LRem(0, "mylist", 3, "value4");
	LOG_IF(FATAL, db.LLen(0, "mylist") != 3) << "lrem mylist failed:"
														<< db.LLen(0, "mylist");
}

void test_lists(Ardb& db)
{
	test_lists_lpush(db);
	test_lists_rpush(db);
	test_lists_insert(db);
	test_lists_lrange(db);
	test_lists_lrem(db);
	test_lists_ltrim(db);
}

