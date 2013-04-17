/*
 * sets_testcase.cpp
 *
 *  Created on: 2"0"13-4-9
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_set_saddrem(Ardb& db)
{
	db.SClear("0", "myset");
	db.SAdd("0", "myset", "123");
	db.SAdd("0", "myset", "123");
	db.SAdd("0", "myset", "1231");
	CHECK_FATAL( db.SCard("0", "myset") != 2,
			"sadd myset failed:Dd", db.SCard("0", "myset"));
	db.SRem("0", "myset", "1231");
	CHECK_FATAL( db.SCard("0", "myset") != 1,
			"srem myset failed:%d", db.SCard("0", "myset"));
}

void test_set_member(Ardb& db)
{
	db.SClear("0", "myset");
	db.SAdd("0", "myset", "v1");
	db.SAdd("0", "myset", "v2");
	db.SAdd("0", "myset", "v3");
	CHECK_FATAL(db.SIsMember("0", "myset", "v0") != false,
			"SIsMember myset failed:");
	ValueArray members;
	db.SMembers("0", "myset", members);
	CHECK_FATAL( members.size() != 3, "SMembers myset failed:");
	CHECK_FATAL( members[0].ToString() != "v1", "SMembers myset failed:");
}

void test_set_diff(Ardb& db)
{
	db.SClear("0", "myset1");
	db.SClear("0", "myset2");
	db.SClear("0", "myset3");
	db.SAdd("0", "myset1", "a");
	db.SAdd("0", "myset1", "b");
	db.SAdd("0", "myset1", "c");
	db.SAdd("0", "myset1", "d");
	db.SAdd("0", "myset2", "c");
	db.SAdd("0", "myset3", "a");
	db.SAdd("0", "myset3", "c");
	db.SAdd("0", "myset3", "e");

	SliceArray keys;
	keys.push_back("myset1");
	keys.push_back("myset2");
	keys.push_back("myset3");
	ValueSet values;
	db.SDiff("0", keys, values);
	CHECK_FATAL( values.size() != 2, "Sdiff failed:");
	CHECK_FATAL(values.begin()->ToString() != "b", "Sdiff store failed:");
	//CHECK_FATAL(FATAL, values[1] != "d") << "Sdiff store failed:";
	int len = db.SDiffStore("0", "myset2", keys);
	CHECK_FATAL(len != 2, "SDiffStore myset2 failed:%d", len);
	len = db.SCard("0", "myset2");
	CHECK_FATAL(len != 2, "SDiffStore myset2 failed:%d", len);
}

void test_set_inter(Ardb& db)
{
	db.SClear("0", "myset1");
	db.SClear("0", "myset2");
	db.SClear("0", "myset3");
	db.SAdd("0", "myset1", "a");
	db.SAdd("0", "myset1", "b");
	db.SAdd("0", "myset1", "c");
	db.SAdd("0", "myset1", "d");
	db.SAdd("0", "myset2", "c");
	db.SAdd("0", "myset3", "a");
	db.SAdd("0", "myset3", "c");
	db.SAdd("0", "myset3", "e");

	SliceArray keys;
	keys.push_back("myset1");
	keys.push_back("myset2");
	keys.push_back("myset3");
	ValueSet values;
	db.SInter("0", keys, values);
	CHECK_FATAL( values.size() != 1, "Sinter failed:");
	CHECK_FATAL(values.begin()->ToString() != "c", "Sinter store failed:");
	db.SInterStore("0", "myset2", keys);
	CHECK_FATAL( db.SCard("0", "myset2") != 1, "SInterStore myset2 failed:");
}

void test_set_union(Ardb& db)
{
	db.SClear("0", "myset1");
	db.SClear("0", "myset2");
	db.SClear("0", "myset3");
	db.SAdd("0", "myset1", "a");
	db.SAdd("0", "myset1", "b");
	db.SAdd("0", "myset1", "c");
	db.SAdd("0", "myset1", "d");
	db.SAdd("0", "myset2", "c");
	db.SAdd("0", "myset3", "a");
	db.SAdd("0", "myset3", "c");
	db.SAdd("0", "myset3", "e");

	SliceArray keys;
	keys.push_back("myset1");
	keys.push_back("myset2");
	keys.push_back("myset3");
	ValueSet values;
	db.SUnion("0", keys, values);
	CHECK_FATAL(values.size() != 5, "SUnion failed:");
	CHECK_FATAL( values.begin()->ToString() != "a", "SUnion store failed:");
	db.SUnionStore("0", "myset2", keys);
	CHECK_FATAL(db.SCard("0", "myset2") != 5, "SUnionStore myset2 failed:");
}

void test_sets(Ardb& db)
{
	test_set_saddrem(db);
	test_set_member(db);
	test_set_diff(db);
	test_set_inter(db);
	test_set_union(db);
}

