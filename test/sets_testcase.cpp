/*
 * sets_testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: yinqiwen
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_set_saddrem(Ardb& db)
{
	DBID dbid = 0;
	db.SClear(dbid, "myset");
	db.SAdd(dbid, "myset", "123");
	db.SAdd(dbid, "myset", "123");
	db.SAdd(dbid, "myset", "1231");
	CHECK_FATAL( db.SCard(dbid, "myset") != 2,
	        "sadd myset failed:%d", db.SCard(dbid, "myset"));
	db.SRem(dbid, "myset", "1231");
	CHECK_FATAL( db.SCard(dbid, "myset") != 1,
	        "srem myset failed:%d", db.SCard(dbid, "myset"));
}

void test_set_member(Ardb& db)
{
	DBID dbid = 0;
	std::string str;
	db.SClear(dbid, "myset");
	db.SAdd(dbid, "myset", "v1");
	db.SAdd(dbid, "myset", "v2");
	db.SAdd(dbid, "myset", "v3");
	CHECK_FATAL(db.SIsMember(dbid, "myset", "v0") != false,
	        "SIsMember myset failed:");
	ValueArray members;
	db.SMembers(dbid, "myset", members);
	CHECK_FATAL( members.size() != 3, "SMembers myset failed:");
	CHECK_FATAL( members[0].ToString(str) != "v1", "SMembers myset failed:");
}

void test_set_diff(Ardb& db)
{
	DBID dbid = 0;
	db.SClear(dbid, "myset1");
	db.SClear(dbid, "myset2");
	db.SClear(dbid, "myset3");
	db.SAdd(dbid, "myset1", "a");
	db.SAdd(dbid, "myset1", "b");
	db.SAdd(dbid, "myset1", "c");
	db.SAdd(dbid, "myset1", "d");
	db.SAdd(dbid, "myset2", "c");
	db.SAdd(dbid, "myset3", "a");
	db.SAdd(dbid, "myset3", "c");
	db.SAdd(dbid, "myset3", "e");

	std::string str;
	SliceArray keys;
	keys.push_back("myset1");
	keys.push_back("myset2");
	keys.push_back("myset3");
	ValueArray values;
	db.SDiff(dbid, keys, values);
	CHECK_FATAL(values.size() != 2, "Sdiff failed:");
	CHECK_FATAL(values.begin()->ToString(str) != "b", "Sdiff store failed:");
	//CHECK_FATAL(FATAL, values[1] != "d") << "Sdiff store failed:";
	int len = db.SDiffStore(dbid, "myset2", keys);
	CHECK_FATAL(len != 2, "SDiffStore myset2 failed:%d", len);
	len = db.SCard(dbid, "myset2");
	CHECK_FATAL(len != 2, "SDiffStore myset2 failed:%d", len);
}

void test_set_inter(Ardb& db)
{
	DBID dbid = 0;
	db.SClear(dbid, "myset1");
	db.SClear(dbid, "myset2");
	db.SClear(dbid, "myset3");
	db.SAdd(dbid, "myset1", "a");
	db.SAdd(dbid, "myset1", "b");
	db.SAdd(dbid, "myset1", "c");
	db.SAdd(dbid, "myset1", "d");
	db.SAdd(dbid, "myset2", "c");
	db.SAdd(dbid, "myset3", "a");
	db.SAdd(dbid, "myset3", "c");
	db.SAdd(dbid, "myset3", "e");

	SliceArray keys;
	keys.push_back("myset1");
	keys.push_back("myset2");
	keys.push_back("myset3");
	ValueArray values;
	db.SInter(dbid, keys, values);
	std::string str;
	CHECK_FATAL( values.size() != 1, "Sinter failed:");
	CHECK_FATAL(values.begin()->ToString(str) != "c", "Sinter store failed.");
	db.SInterStore(dbid, "myset4", keys);
	CHECK_FATAL( db.SCard(dbid, "myset4") != 1, "SInterStore myset4 failed");
}

void test_set_union(Ardb& db)
{
	DBID dbid = 0;
	db.SClear(dbid, "myset1");
	db.SClear(dbid, "myset2");
	db.SClear(dbid, "myset3");
	db.SAdd(dbid, "myset1", "a");
	db.SAdd(dbid, "myset1", "b");
	db.SAdd(dbid, "myset1", "c");
	db.SAdd(dbid, "myset1", "d");
	db.SAdd(dbid, "myset2", "c");
	db.SAdd(dbid, "myset3", "a");
	db.SAdd(dbid, "myset3", "c");
	db.SAdd(dbid, "myset3", "e");

	SliceArray keys;
	keys.push_back("myset1");
	keys.push_back("myset2");
	keys.push_back("myset3");
	ValueArray values;
	db.SUnion(dbid, keys, values);
	CHECK_FATAL(values.size() != 5, "SUnion failed:");
	std::string str;
	CHECK_FATAL( values.begin()->ToString(str) != "a", "SUnion store failed:");
	db.SUnionStore(dbid, "myset2", keys);
	CHECK_FATAL(db.SCard(dbid, "myset2") != 5, "SUnionStore myset2 failed:");
}

void test_set_expire(Ardb& db)
{
	DBID dbid = 0;
	db.SClear(dbid, "myset");
	db.SAdd(dbid, "myset", "123");
	db.Expire(dbid, "myset", 1);
	CHECK_FATAL(db.Exists(dbid, "myset") == false, "Expire myset failed");
	sleep(2);
	CHECK_FATAL(db.Exists(dbid, "myset") == true, "Expire myset failed");
}

void test_sets(Ardb& db)
{
	test_set_saddrem(db);
	test_set_member(db);
	test_set_diff(db);
	test_set_inter(db);
	test_set_union(db);
	test_set_expire(db);
}

