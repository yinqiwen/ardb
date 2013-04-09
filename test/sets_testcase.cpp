/*
 * sets_testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: wqy
 */
#include "rddb.hpp"
#include <string>
#include <glog/logging.h>

using namespace rddb;

void test_set_saddrem(RDDB& db)
{
	db.SClear(0, "myset");
	db.SAdd(0, "myset", "123");
	db.SAdd(0, "myset", "123");
	db.SAdd(0, "myset", "1231");
	LOG_IF(FATAL, db.SCard(0, "myset") != 2) << "sadd myset failed:"
														<< db.SCard(0, "myset");
	db.SRem(0, "myset", "1231");
	LOG_IF(FATAL, db.SCard(0, "myset") != 1) << "srem myset failed:"
														<< db.SCard(0, "myset");
}

void test_set_member(RDDB& db)
{
	db.SClear(0, "myset");
	db.SAdd(0, "myset", "v1");
	db.SAdd(0, "myset", "v2");
	db.SAdd(0, "myset", "v3");
	LOG_IF(FATAL, db.SIsMember(0, "myset", "v0") != false)
																	<< "SIsMember myset failed:";
	StringArray members;
	db.SMembers(0, "myset", members);
	LOG_IF(FATAL, members.size() != 3) << "SMembers myset failed:";
	LOG_IF(FATAL, members[0] != "v1") << "SMembers myset failed:";
}

void test_set_diff(RDDB& db)
{
	db.SClear(0, "myset1");
	db.SClear(0, "myset2");
	db.SClear(0, "myset3");
	db.SAdd(0, "myset1", "a");
	db.SAdd(0, "myset1", "b");
	db.SAdd(0, "myset1", "c");
	db.SAdd(0, "myset1", "d");
	db.SAdd(0, "myset2", "c");
	db.SAdd(0, "myset3", "a");
	db.SAdd(0, "myset3", "c");
	db.SAdd(0, "myset3", "e");

	SliceArray keys;
	keys.push_back("myset1");
	keys.push_back("myset2");
	keys.push_back("myset3");
	StringArray values;
	db.SDiff(0, keys, values);
	LOG_IF(FATAL, values.size() != 2) << "Sdiff failed:";
	LOG_IF(FATAL, values[0] != "b") << "Sdiff store failed:";
	LOG_IF(FATAL, values[1] != "d") << "Sdiff store failed:";
	db.SDiffStore(0, "myset2", keys);
	LOG_IF(FATAL, db.SCard(0, "myset2") != 2) << "SDiffStore myset2 failed:";
}

void test_set_inter(RDDB& db)
{
	db.SClear(0, "myset1");
	db.SClear(0, "myset2");
	db.SClear(0, "myset3");
	db.SAdd(0, "myset1", "a");
	db.SAdd(0, "myset1", "b");
	db.SAdd(0, "myset1", "c");
	db.SAdd(0, "myset1", "d");
	db.SAdd(0, "myset2", "c");
	db.SAdd(0, "myset3", "a");
	db.SAdd(0, "myset3", "c");
	db.SAdd(0, "myset3", "e");

	SliceArray keys;
	keys.push_back("myset1");
	keys.push_back("myset2");
	keys.push_back("myset3");
	StringArray values;
	db.SInter(0, keys, values);
	LOG_IF(FATAL, values.size() != 1) << "Sinter failed:";
	LOG_IF(FATAL, values[0] != "c") << "Sinter store failed:";
	db.SInterStore(0, "myset2", keys);
	LOG_IF(FATAL, db.SCard(0, "myset2") != 1) << "SInterStore myset2 failed:";
}

void test_set_union(RDDB& db)
{
	db.SClear(0, "myset1");
	db.SClear(0, "myset2");
	db.SClear(0, "myset3");
	db.SAdd(0, "myset1", "a");
	db.SAdd(0, "myset1", "b");
	db.SAdd(0, "myset1", "c");
	db.SAdd(0, "myset1", "d");
	db.SAdd(0, "myset2", "c");
	db.SAdd(0, "myset3", "a");
	db.SAdd(0, "myset3", "c");
	db.SAdd(0, "myset3", "e");

	SliceArray keys;
	keys.push_back("myset1");
	keys.push_back("myset2");
	keys.push_back("myset3");
	StringArray values;
	db.SUnion(0, keys, values);
	LOG_IF(FATAL, values.size() != 5) << "SUnion failed:";
	LOG_IF(FATAL, values[0] != "a") << "SUnion store failed:";
	db.SUnionStore(0, "myset2", keys);
	LOG_IF(FATAL, db.SCard(0, "myset2") != 5) << "SUnionStore myset2 failed:";
}

void test_sets(RDDB& db)
{
	test_set_saddrem(db);
	test_set_member(db);
	test_set_diff(db);
	test_set_inter(db);
	test_set_union(db);
}

