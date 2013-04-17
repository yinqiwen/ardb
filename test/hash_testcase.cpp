/*
 * hash_testcase.cpp
 *
 *  Created on: 2"0"13-4-9
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_hash_hgetset(Ardb& db)
{
	db.Del("0", "myhash");
	db.HSet("0", "myhash", "field1", "value1");
	std::string v;
	db.HGet("0", "myhash", "field1", &v);
	CHECK_FATAL(v != "value1", "HGetSet failed:%s", v.c_str());
}

void test_hash_hexists(Ardb& db)
{
	db.HClear("0", "myhash");
	bool ret = db.HExists("0", "myhash", "field1");
	CHECK_FATAL(ret != false, "HExists myhash failed:%d", ret);
	db.HSet("0", "myhash", "field1", "value1");
	ret = db.HExists("0", "myhash", "field1");
	CHECK_FATAL(ret != true, "HExists myhash failed:%d", ret);
	ret = db.HExists("0", "myhash", "field2");
	CHECK_FATAL( ret != false, "HExists myhash failed:%d", ret);
}

void test_hash_hgetall(Ardb& db)
{
	db.HClear("0", "myhash");
	db.HSet("0", "myhash", "field1", "value1");
	db.HSet("0", "myhash", "field2", "value2");
	db.HSet("0", "myhash", "field3", "value3");
	ValueArray values;
	StringArray fields;
	db.HGetAll("0", "myhash", fields, values);
	CHECK_FATAL(fields.size() != 3, "hgetall myhash failed:%d", fields.size());
	CHECK_FATAL(fields[1].compare("field2") != 0,
			"hgetall myhash failed:%d", fields.size());
	int ret = values[2].ToString().compare("value3");
	CHECK_FATAL(ret != 0, "hgetall myhash failed:%d", values.size());
}

void test_hash_hkeys(Ardb& db)
{
	db.HClear("0", "myhash");
	db.HSet("0", "myhash", "field1", "value1");
	db.HSet("0", "myhash", "field2", "value2");
	db.HSet("0", "myhash", "field3", "value3");
	StringArray fields;
	db.HKeys("0", "myhash", fields);
	CHECK_FATAL( fields.size() != 3, "hgetall myhash failed:%d", fields.size());
	CHECK_FATAL(fields[1].compare("field2") != 0,
			"hgetall myhash failed:%d", fields.size());
}

void test_hash_hvals(Ardb& db)
{
	db.HClear("0", "myhash");
	db.HSet("0", "myhash", "field1", "value1");
	db.HSet("0", "myhash", "field2", "value2");
	db.HSet("0", "myhash", "field3", "value3");
	StringArray values;
	db.HVals("0", "myhash", values);
	int ret = values[2].compare("value3");
	CHECK_FATAL(ret != 0, "hgetall myhash failed:%d", values.size());
}

void test_hash_hlen(Ardb& db)
{
	db.HClear("0", "myhash");
	db.HSet("0", "myhash", "field1", "value1");
	db.HSet("0", "myhash", "field2", "value2");
	db.HSet("0", "myhash", "field3", "value3");
	db.HSet("0", "myhash", "field4", "value3");
	db.HSet("0", "myhash", "field5", "value3");

	CHECK_FATAL( db.HLen("0", "myhash") != 5,
			"hlen myhash failed:%d", db.HLen("0", "myhash"));
}

void test_hash_hsetnx(Ardb& db)
{
	db.HClear("0", "myhash");
	int ret = db.HSetNX("0", "myhash", "field1", "value1");
	CHECK_FATAL( ret != 1, "hsetnx myhash failed:%d", ret);
	ret = db.HSetNX("0", "myhash", "field1", "value2");
	CHECK_FATAL(ret != 0, "hsetnx myhash failed:%d", ret);
}

void test_hash_hincr(Ardb& db)
{
	db.HClear("0", "myhash");
	int ret = db.HSetNX("0", "myhash", "field1", "100");
	int64_t intv = 0;
	db.HIncrby("0", "myhash", "field1", 100, intv);
	CHECK_FATAL( intv != 200, "hincr myhash failed:%d", intv);
	double dv = 0;
	db.HIncrbyFloat("0", "myhash", "field1", 100.25, dv);
	CHECK_FATAL(dv != 300.25, "hincrbyfloat myhash failed:%f", dv);
}

void test_hashs(Ardb& db)
{
	test_hash_hgetset(db);
	test_hash_hexists(db);
	test_hash_hkeys(db);
	test_hash_hvals(db);
	test_hash_hlen(db);
	test_hash_hsetnx(db);
	test_hash_hincr(db);
}

