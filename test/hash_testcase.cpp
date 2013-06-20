/*
 * hash_testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: yinqiwen
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_hash_hgetset(Ardb& db)
{
	DBID dbid = 0;
	db.Del(dbid, "myhash");
	db.HSet(dbid, "myhash", "field1", "value1");
	std::string v;
	db.HGet(dbid, "myhash", "field1", &v);
	CHECK_FATAL(v != "value1", "HGetSet failed:%s", v.c_str());
}

void test_hash_hexists(Ardb& db)
{
	DBID dbid = 0;
	db.HClear(dbid, "myhash");
	bool ret = db.HExists(dbid, "myhash", "field1");
	CHECK_FATAL(ret != false, "HExists myhash failed:%d", ret);
	db.HSet(dbid, "myhash", "field1", "value1");
	ret = db.HExists(dbid, "myhash", "field1");
	CHECK_FATAL(ret != true, "HExists myhash failed:%d", ret);
	ret = db.HExists(dbid, "myhash", "field2");
	CHECK_FATAL( ret != false, "HExists myhash failed:%d", ret);
}

void test_hash_hgetall(Ardb& db)
{
	DBID dbid = 0;
	db.HClear(dbid, "myhash");
	db.HSet(dbid, "myhash", "field1", "value1");
	db.HSet(dbid, "myhash", "field2", "value2");
	db.HSet(dbid, "myhash", "field3", "value3");
	ValueArray values;
	StringArray fields;
	db.HGetAll(dbid, "myhash", fields, values);
	CHECK_FATAL(fields.size() != 3, "hgetall myhash failed:%zu", fields.size());
	CHECK_FATAL(fields[1].compare("field2") != 0,
	        "hgetall myhash failed:%zu", fields.size());
	std::string str;
	int ret = values[2].ToString(str).compare("value3");
	CHECK_FATAL(ret != 0, "hgetall myhash failed:%zu", values.size());
}

void test_hash_hkeys(Ardb& db)
{
	DBID dbid = 0;
	db.HClear(dbid, "myhash");
	db.HSet(dbid, "myhash", "field1", "value1");
	db.HSet(dbid, "myhash", "field2", "value2");
	db.HSet(dbid, "myhash", "field3", "value3");
	StringArray fields;
	db.HKeys(dbid, "myhash", fields);
	CHECK_FATAL( fields.size() != 3,
	        "hgetall myhash failed:%zu", fields.size());
	CHECK_FATAL(fields[1].compare("field2") != 0,
	        "hgetall myhash failed:%zu", fields.size());
}

void test_hash_hvals(Ardb& db)
{
	DBID dbid = 0;
	db.HClear(dbid, "myhash");
	db.HSet(dbid, "myhash", "field1", "value1");
	db.HSet(dbid, "myhash", "field2", "value2");
	db.HSet(dbid, "myhash", "field3", "value3");
	StringArray values;
	db.HVals(dbid, "myhash", values);
	int ret = values[2].compare("value3");
	CHECK_FATAL(ret != 0, "hvals myhash failed:%s", values[2].c_str());
}

void test_hash_hlen(Ardb& db)
{
	DBID dbid = 0;
	db.HClear(dbid, "myhash");
	db.HSet(dbid, "myhash", "field1", "value1");
	db.HSet(dbid, "myhash", "field2", "value2");
	db.HSet(dbid, "myhash", "field3", "value3");
	db.HSet(dbid, "myhash", "field4", "value3");
	db.HSet(dbid, "myhash", "field5", "value3");

	CHECK_FATAL( db.HLen(dbid, "myhash") != 5,
	        "hlen myhash failed:%d", db.HLen(dbid, "myhash"));
}

void test_hash_hsetnx(Ardb& db)
{
	DBID dbid = 0;
	db.HClear(dbid, "myhash");
	int ret = db.HSetNX(dbid, "myhash", "field1", "value1");
	CHECK_FATAL( ret != 1, "hsetnx myhash failed:%d", ret);
	ret = db.HSetNX(dbid, "myhash", "field1", "value2");
	CHECK_FATAL(ret != 0, "hsetnx myhash failed:%d", ret);
}

void test_hash_hincr(Ardb& db)
{
	DBID dbid = 0;
	db.HClear(dbid, "myhash");
	db.HSetNX(dbid, "myhash", "field1", "100");
	int64_t intv = 0;
	db.HIncrby(dbid, "myhash", "field1", 100, intv);
	CHECK_FATAL( intv != 200, "hincr myhash failed:%"PRId64, intv);
	double dv = 0;
	db.HIncrbyFloat(dbid, "myhash", "field1", 100.25, dv);
	CHECK_FATAL(dv != 300.25, "hincrbyfloat myhash failed:%f", dv);
}

void test_hash_expire(Ardb& db)
{
	DBID dbid = 0;
	db.HClear(dbid, "myhash");
	db.HSetNX(dbid, "myhash", "field1", "100");
	db.Expire(dbid, "myhash", 1);
	CHECK_FATAL(db.Exists(dbid, "myhash") == false, "Expire myhash failed");
	sleep(2);
	CHECK_FATAL(db.Exists(dbid, "myhash") == true, "Expire myhash failed");
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
	test_hash_expire(db);
}

