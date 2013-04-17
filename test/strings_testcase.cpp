/*
 * strings.testcase.cpp
 *
 *  Created on: 2"0"13-4-9
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_strings_append(Ardb& db)
{
	std::string v;
	//append
	db.Set("0", "skey", "abc");
	db.Append("0", "skey", "abc");
	int ret = db.Get("0", "skey", &v);
	CHECK_FATAL( ret != 0, "Failed to get skey.");
	CHECK_FATAL( v != "abcabc", "Invalid str:%s", v.c_str());
}

void test_strings_getrange(Ardb& db)
{
	std::string v;
	db.Set("0", "skey", "abcabc");
	db.GetRange("0", "skey", 4, -1, v);
	CHECK_FATAL( v != "bc", "GetRange failed");
}

void test_strings_setrange(Ardb& db)
{
	std::string v;
	db.Set("0", "skey", "abcabc");
	db.SetRange("0", "skey", 3, "12345");
	db.Get("0", "skey", &v);
	CHECK_FATAL(v != "abc12345", "SetRange failed:%s", v.c_str());
}

void test_strings_getset(Ardb& db)
{
	std::string v;
	db.Set("0", "skey", "abcabc");
	db.GetSet("0", "skey", "edfgth", v);
	CHECK_FATAL( v != "abcabc", "GetSet failed:%s", v.c_str());
}

void test_strings_strlen(Ardb& db)
{
	db.Set("0", "skey", "abcabcabc");
	int len = db.Strlen("0", "skey");
	CHECK_FATAL( len != 9, "Strlen failed:%d", len);
}

void test_strings_decr(Ardb& db)
{
	db.Set("0", "intkey", "10");
	int64_t iv = 0;
	db.Decr("0", "intkey", iv);
	CHECK_FATAL(iv != 9, "Decr1 failed %d", iv);
	db.Decrby("0", "intkey", 2, iv);
	CHECK_FATAL( iv != 7, "Decrby failed");
}

void test_strings_incr(Ardb& db)
{
	db.Set("0", "intkey", "12");
	int64_t iv = 0;
	db.Incrby("0", "intkey", 2, iv);
	CHECK_FATAL( iv != 14, "Incrby failed");
	double dv;
	db.IncrbyFloat("0", "intkey", 1.23, dv);
	CHECK_FATAL( dv != 15.23, "IncrbyFloat failed");
}

void test_strings_exists(Ardb& db)
{
	db.Del("0", "intkey1");
	CHECK_FATAL( db.Exists("0", "intkey1"), "Exists intkey1 failed");
	db.Set("0", "intkey1", "123");
	CHECK_FATAL( db.Exists("0", "intkey1") == false, "Exists intkey failed");
}

void test_strings_setnx(Ardb& db)
{
	db.Set("0", "intkey1", "123");
	CHECK_FATAL(db.SetNX("0", "intkey1", "2345") != 0, "SetNX intkey failed");
	db.Del("0", "intkey1");
	CHECK_FATAL( db.SetNX("0", "intkey1", "2345") == 0, "SetNX intkey failed");
}

void test_strings_expire(Ardb& db)
{
	ValueObject v;
	db.Set("0", "intkey1", "123");
	db.Expire("0", "intkey1", 1);
	CHECK_FATAL(db.Exists("0", "intkey1") == false, "Expire intkey1 failed");
	sleep(2);
	CHECK_FATAL(db.Exists("0", "intkey1") == true, "Expire intkey failed");
}

void test_strings_bitcount(Ardb& db)
{
	db.Set("0", "intkey1", "foobar");
	int bitcount = db.BitCount("0", "intkey1", 0, 0);
	CHECK_FATAL(bitcount != 4, "bitcount intkey1 failed:%d", bitcount);
	bitcount = db.BitCount("0", "intkey1", 0, -1);
	CHECK_FATAL(bitcount!= 26, "bitcount intkey1 failed:%d", bitcount);
	bitcount = db.BitCount("0", "intkey1", 1, 1);
	CHECK_FATAL(bitcount != 6, "bitcount intkey1 failed:%d", bitcount);
}

void test_strings_setgetbit(Ardb& db)
{
	ValueObject v;
	db.Del("0", "mykey");
	int ret = db.SetBit("0", "mykey", 7, 1);
	CHECK_FATAL(ret != 0, "setbit mykey failed:%d", ret);
	ret = db.GetBit("0", "mykey", 7);
	CHECK_FATAL(ret != 1, "getbit mykey failed:%d", ret);
	ret = db.SetBit("0", "mykey", 7, 0);
	CHECK_FATAL( ret != 1, "setbit mykey failed:%d", ret);
	ret = db.GetBit("0", "mykey", 7);
	CHECK_FATAL(ret != 0, "getbit mykey failed:%d", ret);
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

