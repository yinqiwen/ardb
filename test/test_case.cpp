/*
 * test_case.cpp
 *
 *  Created on: 2013-4-4
 *      Author: wqy
 */
#include "rddb.hpp"
#include <string>
#include <glog/logging.h>

using namespace rddb;

void test_strings_append(RDDB& db)
{
	std::string v;
	//append
	db.Set(0, "skey", "abc");
	db.Append(0, "skey", "abc");
	int ret = db.Get(0, "skey", &v);
	LOG_IF(FATAL, ret != 0) << "Failed to get skey.";
	LOG_IF(FATAL, v != "abcabc") << "Invalid str:" << v;
}

void test_strings_getrange(RDDB& db)
{
	std::string v;
	db.Set(0, "skey", "abcabc");
	db.GetRange(0, "skey", 4, -1, v);
	LOG_IF(FATAL, v != "bc") << "GetRange failed";
}

void test_strings_setrange(RDDB& db)
{
	std::string v;
	db.Set(0, "skey", "abcabc");
	db.SetRange(0, "skey", 3, "12345");
	db.Get(0, "skey", &v);
	LOG_IF(FATAL, v != "abc12345") << "SetRange failed:" << v;
}

void test_strings_getset(RDDB& db)
{
	std::string v;
	db.Set(0, "skey", "abcabc");
	db.GetSet(0, "skey", "edfgth", v);
	LOG_IF(FATAL, v != "abcabc") << "GetSet failed:" << v;
}

void test_strings_strlen(RDDB& db)
{
	db.Set(0, "skey", "abcabcabc");
	int len = db.Strlen(0, "skey");
	LOG_IF(FATAL, len != 9) << "Strlen failed:" << len;
}

void test_strings_decr(RDDB& db)
{
	db.Set(0, "intkey", "010");
	int64_t iv = 0;
	db.Decr(0, "intkey", iv);
	LOG_IF(FATAL, iv != 9) << "Decr1 failed";
	db.Decrby(0, "intkey", 2, iv);
	LOG_IF(FATAL, iv != 7) << "Decrby failed";
}

void test_strings_incr(RDDB& db)
{
	db.Set(0, "intkey", "012");
	int64_t iv = 0;
	db.Incrby(0, "intkey", 2, iv);
	LOG_IF(FATAL, iv != 14) << "Incrby failed";
	double dv;
	db.IncrbyFloat(0, "intkey", 1.23, dv);
	LOG_IF(FATAL, dv != 15.23) << "IncrbyFloat failed";
}

void test_strings_exists(RDDB& db)
{
	db.Del(0, "intkey1");
	LOG_IF(FATAL, db.Exists(0, "intkey1")) << "Exists intkey1 failed";
	db.Set(0, "intkey1", "123");
	LOG_IF(FATAL, db.Exists(0, "intkey1") == false) << "Exists intkey failed";
}

void test_strings_setnx(RDDB& db)
{
	db.Set(0, "intkey1", "123");
	LOG_IF(FATAL, db.SetNX(0, "intkey1", "2345") != 0)
																<< "SetNX intkey failed";
	db.Del(0, "intkey1");
	LOG_IF(FATAL, db.SetNX(0, "intkey1", "2345") == 0)
																<< "SetNX intkey failed";
}

void test_strings_expire(RDDB& db)
{
	ValueObject v;
	db.Set(0, "intkey1", "123");
	db.Expire(0, "intkey1", 3);
	LOG_IF(FATAL, db.Exists(0, "intkey1") == false) << "Expire intkey1 failed";
	sleep(3);
	LOG_IF(FATAL, db.Exists(0, "intkey1") == true) << "Expire intkey failed";
}

void test_strings_bitcount(RDDB& db)
{
	db.Set(0, "intkey1", "foobar");
	int bitcount = db.BitCount(0, "intkey1", 0, 0);
	LOG_IF(FATAL, bitcount != 4) << "bitcount intkey1 failed:" << bitcount;
	bitcount = db.BitCount(0, "intkey1", 0, -1);
	LOG_IF(FATAL, bitcount!= 26) << "bitcount intkey1 failed:" << bitcount;
	bitcount = db.BitCount(0, "intkey1", 1, 1);
	LOG_IF(FATAL, bitcount != 6) << "bitcount intkey1 failed:" << bitcount;
}

void test_strings_setgetbit(RDDB& db)
{
	ValueObject v;
	db.Del(0, "mykey");
	int ret = db.SetBit(0, "mykey", 7, 1);
	LOG_IF(FATAL, ret != 0) << "setbit mykey failed:" << ret;
	ret = db.GetBit(0, "mykey", 7);
	LOG_IF(FATAL, ret != 1) << "getbit mykey failed:" << ret;
	ret = db.SetBit(0, "mykey", 7, 0);
	LOG_IF(FATAL, ret != 1) << "setbit mykey failed:" << ret;
	ret = db.GetBit(0, "mykey", 7);
	LOG_IF(FATAL, ret != 0) << "getbit mykey failed:" << ret;
}

void test_strings(RDDB& db)
{
	test_strings_append(db);
	test_strings_getrange(db);
	test_strings_setrange(db);
	test_strings_getset(db);
	test_strings_strlen(db);
	test_strings_decr(db);
	test_strings_incr(db);
	test_strings_exists(db);
	test_strings_setnx(db);
	test_strings_expire(db);
	test_strings_bitcount(db);
	test_strings_setgetbit(db);
}

void test_sets(RDDB& db)
{

}

void test_zsets(RDDB& db)
{

}

void test_lists_lpush(RDDB& db)
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

void test_lists_rpush(RDDB& db)
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
	db.LIndex(0, "mylist", 2, v);
	LOG_IF(FATAL, v != "value2") << "LIndex failed:" << v;
}

void test_lists_insert(RDDB& db)
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

void test_lists_lrange(RDDB& db)
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

void test_lists_ltrim(RDDB& db)
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

void test_lists_lrem(RDDB& db)
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

void test_lists(RDDB& db)
{
	test_lists_lpush(db);
	test_lists_rpush(db);
	test_lists_insert(db);
	test_lists_lrange(db);
	test_lists_lrem(db);
	test_lists_ltrim(db);
}

void test_hash_hgetset(RDDB& db)
{
	db.Del(0, "myhash");
	db.HSet(0, "myhash", "field1", "value1");
	std::string v;
	db.HGet(0, "myhash", "field1", &v);
	LOG_IF(FATAL, v != "value1") << "HGetSet failed:" << v;
}

void test_hash_hexists(RDDB& db)
{
	db.HClear(0, "myhash");
	bool ret = db.HExists(0, "myhash", "field1");
	LOG_IF(FATAL, ret != false) << "HExists myhash failed:" << ret;
	db.HSet(0, "myhash", "field1", "value1");
	ret = db.HExists(0, "myhash", "field1");
	LOG_IF(FATAL, ret != true) << "HExists myhash failed:" << ret;
	ret = db.HExists(0, "myhash", "field2");
	LOG_IF(FATAL, ret != false) << "HExists myhash failed:" << ret;
}

void test_hash_hgetall(RDDB& db)
{
	db.HClear(0, "myhash");
	db.HSet(0, "myhash", "field1", "value1");
	db.HSet(0, "myhash", "field2", "value2");
	db.HSet(0, "myhash", "field3", "value3");
	StringArray values;
	StringArray fields;
	db.HGetAll(0, "myhash", fields, values);
	LOG_IF(FATAL, fields.size() != 3) << "hgetall myhash failed:"
												<< fields.size();
	LOG_IF(FATAL, fields[1].compare("field2") != 0) << "hgetall myhash failed:"
															<< fields.size();
	int ret = values[2].compare("value3");
	LOG_IF(FATAL, ret != 0) << "hgetall myhash failed:" << values.size();
}

void test_hash_hkeys(RDDB& db)
{
	db.HClear(0, "myhash");
	db.HSet(0, "myhash", "field1", "value1");
	db.HSet(0, "myhash", "field2", "value2");
	db.HSet(0, "myhash", "field3", "value3");
	StringArray fields;
	db.HKeys(0, "myhash", fields);
	LOG_IF(FATAL, fields.size() != 3) << "hgetall myhash failed:"
												<< fields.size();
	LOG_IF(FATAL, fields[1].compare("field2") != 0) << "hgetall myhash failed:"
															<< fields.size();
}

void test_hash_hvals(RDDB& db)
{
	db.HClear(0, "myhash");
	db.HSet(0, "myhash", "field1", "value1");
	db.HSet(0, "myhash", "field2", "value2");
	db.HSet(0, "myhash", "field3", "value3");
	StringArray values;
	db.HVals(0, "myhash", values);
	int ret = values[2].compare("value3");
	LOG_IF(FATAL, ret != 0) << "hgetall myhash failed:" << values.size();
}

void test_hash_hlen(RDDB& db)
{
	db.HClear(0, "myhash");
	db.HSet(0, "myhash", "field1", "value1");
	db.HSet(0, "myhash", "field2", "value2");
	db.HSet(0, "myhash", "field3", "value3");
	db.HSet(0, "myhash", "field4", "value3");
	db.HSet(0, "myhash", "field5", "value3");

	LOG_IF(FATAL, db.HLen(0, "myhash") != 5) << "hlen myhash failed:"
														<< db.HLen(0, "myhash");
}

void test_hash_hsetnx(RDDB& db)
{
	db.HClear(0, "myhash");
	int ret = db.HSetNX(0, "myhash", "field1", "value1");
	LOG_IF(FATAL, ret != 1) << "hsetnx myhash failed:" << ret;
	ret = db.HSetNX(0, "myhash", "field1", "value2");
	LOG_IF(FATAL, ret != 0) << "hsetnx myhash failed:" << ret;
}

void test_hash_hincr(RDDB& db)
{
	db.HClear(0, "myhash");
	int ret = db.HSetNX(0, "myhash", "field1", "100");
	int64_t intv = 0;
	db.HIncrby(0, "myhash", "field1", 100, intv);
	LOG_IF(FATAL, intv != 200) << "hincr myhash failed:" << intv;
	double dv = 0;
	db.HIncrbyFloat(0, "myhash", "field1", 100.25, dv);
	LOG_IF(FATAL, dv != 300.25) << "hincrbyfloat myhash failed:" << dv;
}

void test_hashs(RDDB& db)
{
	test_hash_hgetset(db);
	test_hash_hexists(db);
	test_hash_hkeys(db);
	test_hash_hvals(db);
	test_hash_hlen(db);
	test_hash_hsetnx(db);
	test_hash_hincr(db);
}

