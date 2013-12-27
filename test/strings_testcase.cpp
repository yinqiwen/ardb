/*
 * strings.testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: yinqiwen
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_strings_append(Ardb& db)
{
	DBID dbid = 0;
	std::string v;
	//append
	db.Set(dbid, "skey", "abc");
	db.Append(dbid, "skey", "abc");
	int ret = db.Get(dbid, "skey", v);
	CHECK_FATAL( ret != 0, "Failed to get skey.");
	CHECK_FATAL( v != "abcabc", "Invalid str:%s", v.c_str());
}

void test_strings_getrange(Ardb& db)
{
	DBID dbid = 0;
	std::string v;
	db.Set(dbid, "skey", "abcabc");
	db.GetRange(dbid, "skey", 4, -1, v);
	CHECK_FATAL( v != "bc", "GetRange failed");
}

void test_strings_setrange(Ardb& db)
{
	DBID dbid = 0;
	std::string v;
	db.Set(dbid, "skey", "abcabc");
	db.SetRange(dbid, "skey", 3, "12345");
	db.Get(dbid, "skey", v);
	CHECK_FATAL(v != "abc12345", "SetRange failed:%s", v.c_str());
}

void test_strings_getset(Ardb& db)
{
	DBID dbid = 0;
	std::string v;
	db.Set(dbid, "skey", "abcabc");
	db.GetSet(dbid, "skey", "edfgth", v);
	CHECK_FATAL( v != "abcabc", "GetSet failed:%s", v.c_str());
}

void test_strings_strlen(Ardb& db)
{
	DBID dbid = 0;
	db.Set(dbid, "skey", "abcabcabc");
	int len = db.Strlen(dbid, "skey");
	CHECK_FATAL( len != 9, "Strlen failed:%d", len);
}

void test_strings_decr(Ardb& db)
{
	DBID dbid = 0;
	db.Set(dbid, "intkey", "10");
	int64_t iv = 0;
	db.Decr(dbid, "intkey", iv);
	CHECK_FATAL(iv != 9, "Decr1 failed %"PRId64, iv);
	db.Decrby(dbid, "intkey", 2, iv);
	CHECK_FATAL( iv != 7, "Decrby failed: %"PRId64, iv);
}

void test_strings_incr(Ardb& db)
{
	DBID dbid = 0;
	db.Set(dbid, "intkey", "12");
	int64_t iv = 0;
	db.Incrby(dbid, "intkey", 2, iv);
	CHECK_FATAL( iv != 14, "Incrby failed");
	double dv;
	db.IncrbyFloat(dbid, "intkey", 1.23, dv);
	CHECK_FATAL( dv != 15.23, "IncrbyFloat failed");
}

void test_strings_exists(Ardb& db)
{
	DBID dbid = 0;
	db.Del(dbid, "intkey1");
	CHECK_FATAL( db.Exists(dbid, "intkey1"), "Exists intkey1 failed");
	db.Set(dbid, "intkey1", "123");
	CHECK_FATAL( db.Exists(dbid, "intkey1") == false, "Exists intkey failed");
}

void test_strings_setnx(Ardb& db)
{
	DBID dbid = 0;
	db.Set(dbid, "intkey1", "123");
	CHECK_FATAL(db.SetNX(dbid, "intkey1", "2345") == 0, "SetNX intkey failed");
	db.Del(dbid, "intkey1");
	CHECK_FATAL(db.SetNX(dbid, "intkey1", "2345") != 0, "SetNX intkey failed");
}

void test_strings_expire(Ardb& db)
{
	DBID dbid = 0;
	ValueData v;
	db.Set(dbid, "intkey1", "123");
	db.Expire(dbid, "intkey1", 1);
	CHECK_FATAL(db.Exists(dbid, "intkey1") == false, "Expire intkey1 failed");
	sleep(2);
	CHECK_FATAL(db.Exists(dbid, "intkey1") == true, "Expire intkey failed");
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
}

