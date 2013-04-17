/*
 * lists_testcase.cpp
 *
 *  Created on: 2"0"13-4-9
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_lists_lpush(Ardb& db)
{
	db.LClear("0", "mylist");
	int ret = db.LPushx("0", "mylist", "value0");
	CHECK_FATAL( ret != 0, "lpushx mylist failed:%d", ret);
	db.LPush("0", "mylist", "value0");
	db.LPush("0", "mylist", "value1");
	db.LPush("0", "mylist", "value2");
	db.LPush("0", "mylist", "value3");
	CHECK_FATAL(db.LLen("0", "mylist") != 4,
			"lpush/llen mylist failed:%d", db.LLen("0", "mylist"));
	std::string v;
	db.LIndex("0", "mylist", 0, v);
	CHECK_FATAL(v!="value3", "LIndex failed:%s", v.c_str());
}

void test_lists_rpush(Ardb& db)
{
	db.LClear("0", "mylist");
	int ret = db.RPushx("0", "mylist", "value0");
	CHECK_FATAL(ret != 0, "lpushx mylist failed:%d", ret);
	db.RPush("0", "mylist", "value0");
	db.RPush("0", "mylist", "value1");
	db.RPush("0", "mylist", "value2");
	db.RPush("0", "mylist", "value3");
	CHECK_FATAL(db.LLen("0", "mylist") != 4,
			"lrpush/llen mylist failed:%d", db.LLen("0", "mylist"));
	std::string v;
	db.LIndex("0", "mylist", 3, v);
	CHECK_FATAL( v != "value3", "LIndex failed:%s", v.c_str());
}

void test_lists_insert(Ardb& db)
{
	db.LClear("0", "mylist");
	db.RPush("0", "mylist", "value0");
	db.RPush("0", "mylist", "value1");
	db.RPush("0", "mylist", "value3");
	db.RPush("0", "mylist", "value4");
	db.LInsert("0", "mylist", "before", "value3", "value2");
	CHECK_FATAL(db.LLen("0", "mylist") != 5,
			"lrpush/llen mylist failed:%d", db.LLen("0", "mylist"));
	std::string v;
	db.LIndex("0", "mylist", 2, v);
	CHECK_FATAL( v != "value2", "LInsert failed:", v.c_str());
	db.LSet("0", "mylist", 2, "valuex");
	db.LIndex("0", "mylist", 2, v);
	CHECK_FATAL( v != "valuex", "LSet failed:%s", v.c_str());
	std::string vstr;
	db.LPop("0", "mylist", vstr);
	CHECK_FATAL( vstr != "value0", "LPop failed:%s", vstr.c_str());
}

void test_lists_lrange(Ardb& db)
{
	db.LClear("0", "mylist");
	db.RPush("0", "mylist", "value0");
	db.RPush("0", "mylist", "value1");
	db.RPush("0", "mylist", "value3");
	db.RPush("0", "mylist", "value4");

	ValueArray array;
	db.LRange("0", "mylist", 1, 2, array);
	CHECK_FATAL( array.size() != 2, "lrange failed:");
	array.clear();
	db.LRange("0", "mylist", 1, -1, array);
	CHECK_FATAL( array.size() != 3, "lrange failed:");
}

void test_lists_ltrim(Ardb& db)
{
	db.LClear("0", "mylist");
	db.RPush("0", "mylist", "value0");
	db.RPush("0", "mylist", "value1");
	db.RPush("0", "mylist", "value3");
	db.RPush("0", "mylist", "value4");
	db.LTrim("0", "mylist", 0, 2);
	CHECK_FATAL(db.LLen("0", "mylist") != 3,
			"ltrim mylist failed:%d", db.LLen("0", "mylist"));
}

void test_lists_lrem(Ardb& db)
{
	db.LClear("0", "mylist");
	db.RPush("0", "mylist", "value0");
	db.RPush("0", "mylist", "value0");
	db.RPush("0", "mylist", "value0");
	db.RPush("0", "mylist", "value4");
	db.RPush("0", "mylist", "value0");
	db.LRem("0", "mylist", -3, "value0");
	CHECK_FATAL(db.LLen("0", "mylist") != 2,
			"lrem mylist failed:%d", db.LLen("0", "mylist"));
	db.RPush("0", "mylist", "value0");
	db.RPush("0", "mylist", "value0");
	db.LRem("0", "mylist", 3, "value4");
	CHECK_FATAL( db.LLen("0", "mylist") != 3,
			"lrem mylist failed:%d", db.LLen("0", "mylist"));
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

