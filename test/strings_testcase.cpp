/*
 * strings.testcase.cpp
 *
 *  Created on: 2"0"13-4-9
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string>
#include <glog/logging.h>

using namespace ardb;

void test_strings_append(Ardb& db)
{
	std::string v;
	//append
	db.Set("0", "skey", "abc");
	db.Append("0", "skey", "abc");
	int ret = db.Get("0", "skey", &v);
	LOG_IF(FATAL, ret != 0) << "Failed to get skey.";
	LOG_IF(FATAL, v != "abcabc") << "Invalid str:" << v;
}

void test_strings_getrange(Ardb& db)
{
	std::string v;
	db.Set("0", "skey", "abcabc");
	db.GetRange("0", "skey", 4, -1, v);
	LOG_IF(FATAL, v != "bc") << "GetRange failed";
}

void test_strings_setrange(Ardb& db)
{
	std::string v;
	db.Set("0", "skey", "abcabc");
	db.SetRange("0", "skey", 3, "12345");
	db.Get("0", "skey", &v);
	LOG_IF(FATAL, v != "abc12345") << "SetRange failed:" << v;
}

void test_strings_getset(Ardb& db)
{
	std::string v;
	db.Set("0", "skey", "abcabc");
	db.GetSet("0", "skey", "edfgth", v);
	LOG_IF(FATAL, v != "abcabc") << "GetSet failed:" << v;
}

void test_strings_strlen(Ardb& db)
{
	db.Set("0", "skey", "abcabcabc");
	int len = db.Strlen("0", "skey");
	LOG_IF(FATAL, len != 9) << "Strlen failed:" << len;
}

void test_strings_decr(Ardb& db)
{
	db.Set("0", "intkey", "10");
	int64_t iv = 0;
	db.Decr("0", "intkey", iv);
	LOG_IF(FATAL, iv != 9) << "Decr1 failed" << iv;
	db.Decrby("0", "intkey", 2, iv);
	LOG_IF(FATAL, iv != 7) << "Decrby failed";
}

void test_strings_incr(Ardb& db)
{
	db.Set("0", "intkey", "12");
	int64_t iv = 0;
	db.Incrby("0", "intkey", 2, iv);
	LOG_IF(FATAL, iv != 14) << "Incrby failed";
	double dv;
	db.IncrbyFloat("0", "intkey", 1.23, dv);
	LOG_IF(FATAL, dv != 15.23) << "IncrbyFloat failed";
}

void test_strings_exists(Ardb& db)
{
	db.Del("0", "intkey1");
	LOG_IF(FATAL, db.Exists("0", "intkey1")) << "Exists intkey1 failed";
	db.Set("0", "intkey1", "123");
	LOG_IF(FATAL, db.Exists("0", "intkey1") == false) << "Exists intkey failed";
}

void test_strings_setnx(Ardb& db)
{
	db.Set("0", "intkey1", "123");
	LOG_IF(FATAL, db.SetNX("0", "intkey1", "2345") != 0)
																<< "SetNX intkey failed";
	db.Del("0", "intkey1");
	LOG_IF(FATAL, db.SetNX("0", "intkey1", "2345") == 0)
																<< "SetNX intkey failed";
}

void test_strings_expire(Ardb& db)
{
	ValueObject v;
	db.Set("0", "intkey1", "123");
	db.Expire("0", "intkey1", 1);
	LOG_IF(FATAL, db.Exists("0", "intkey1") == false) << "Expire intkey1 failed";
	sleep(2);
	LOG_IF(FATAL, db.Exists("0", "intkey1") == true) << "Expire intkey failed";
}

void test_strings_bitcount(Ardb& db)
{
	db.Set("0", "intkey1", "foobar");
	int bitcount = db.BitCount("0", "intkey1", 0, 0);
	LOG_IF(FATAL, bitcount != 4) << "bitcount intkey1 failed:" << bitcount;
	bitcount = db.BitCount("0", "intkey1", 0, -1);
	LOG_IF(FATAL, bitcount!= 26) << "bitcount intkey1 failed:" << bitcount;
	bitcount = db.BitCount("0", "intkey1", 1, 1);
	LOG_IF(FATAL, bitcount != 6) << "bitcount intkey1 failed:" << bitcount;
}

void test_strings_setgetbit(Ardb& db)
{
	ValueObject v;
	db.Del("0", "mykey");
	int ret = db.SetBit("0", "mykey", 7, 1);
	LOG_IF(FATAL, ret != 0) << "setbit mykey failed:" << ret;
	ret = db.GetBit("0", "mykey", 7);
	LOG_IF(FATAL, ret != 1) << "getbit mykey failed:" << ret;
	ret = db.SetBit("0", "mykey", 7, 0);
	LOG_IF(FATAL, ret != 1) << "setbit mykey failed:" << ret;
	ret = db.GetBit("0", "mykey", 7);
	LOG_IF(FATAL, ret != 0) << "getbit mykey failed:" << ret;
}

void test_strings(Ardb& db)
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



