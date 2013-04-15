/*
 * hash_testcase.cpp
 *
 *  Created on: 2"0"13-4-9
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string>
#include <glog/logging.h>

using namespace ardb;

void test_hash_hgetset(Ardb& db)
{
	db.Del("0", "myhash");
	db.HSet("0", "myhash", "field1", "value1");
	std::string v;
	db.HGet("0", "myhash", "field1", &v);
	LOG_IF(FATAL, v != "value1") << "HGetSet failed:" << v;
}

void test_hash_hexists(Ardb& db)
{
	db.HClear("0", "myhash");
	bool ret = db.HExists("0", "myhash", "field1");
	LOG_IF(FATAL, ret != false) << "HExists myhash failed:" << ret;
	db.HSet("0", "myhash", "field1", "value1");
	ret = db.HExists("0", "myhash", "field1");
	LOG_IF(FATAL, ret != true) << "HExists myhash failed:" << ret;
	ret = db.HExists("0", "myhash", "field2");
	LOG_IF(FATAL, ret != false) << "HExists myhash failed:" << ret;
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
	LOG_IF(FATAL, fields.size() != 3) << "hgetall myhash failed:"
												<< fields.size();
	LOG_IF(FATAL, fields[1].compare("field2") != 0) << "hgetall myhash failed:"
															<< fields.size();
	int ret = values[2].ToString().compare("value3");
	LOG_IF(FATAL, ret != 0) << "hgetall myhash failed:" << values.size();
}

void test_hash_hkeys(Ardb& db)
{
	db.HClear("0", "myhash");
	db.HSet("0", "myhash", "field1", "value1");
	db.HSet("0", "myhash", "field2", "value2");
	db.HSet("0", "myhash", "field3", "value3");
	StringArray fields;
	db.HKeys("0", "myhash", fields);
	LOG_IF(FATAL, fields.size() != 3) << "hgetall myhash failed:"
												<< fields.size();
	LOG_IF(FATAL, fields[1].compare("field2") != 0) << "hgetall myhash failed:"
															<< fields.size();
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
	LOG_IF(FATAL, ret != 0) << "hgetall myhash failed:" << values.size();
}

void test_hash_hlen(Ardb& db)
{
	db.HClear("0", "myhash");
	db.HSet("0", "myhash", "field1", "value1");
	db.HSet("0", "myhash", "field2", "value2");
	db.HSet("0", "myhash", "field3", "value3");
	db.HSet("0", "myhash", "field4", "value3");
	db.HSet("0", "myhash", "field5", "value3");

	LOG_IF(FATAL, db.HLen("0", "myhash") != 5) << "hlen myhash failed:"
														<< db.HLen("0", "myhash");
}

void test_hash_hsetnx(Ardb& db)
{
	db.HClear("0", "myhash");
	int ret = db.HSetNX("0", "myhash", "field1", "value1");
	LOG_IF(FATAL, ret != 1) << "hsetnx myhash failed:" << ret;
	ret = db.HSetNX("0", "myhash", "field1", "value2");
	LOG_IF(FATAL, ret != 0) << "hsetnx myhash failed:" << ret;
}

void test_hash_hincr(Ardb& db)
{
	db.HClear("0", "myhash");
	int ret = db.HSetNX("0", "myhash", "field1", "100");
	int64_t intv = 0;
	db.HIncrby("0", "myhash", "field1", 100, intv);
	LOG_IF(FATAL, intv != 200) << "hincr myhash failed:" << intv;
	double dv = 0;
	db.HIncrbyFloat("0", "myhash", "field1", 100.25, dv);
	LOG_IF(FATAL, dv != 300.25) << "hincrbyfloat myhash failed:" << dv;
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



