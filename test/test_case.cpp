/*
 * test_case.cpp
 *
 *  Created on: 2013-4-4
 *      Author: wqy
 */
#include "strings_testcase.cpp"
#include "lists_testcase.cpp"
#include "sets_testcase.cpp"
#include "hash_testcase.cpp"
#include "zset_testcase.cpp"

void test_type(Ardb& db)
{
	db.SAdd(0, "myset", "123");
	db.LPush(0, "mylist", "value0");
	db.ZAdd(0, "myzset1", 1, "one");
	db.HSet(0, "myhash", "field1", "value1");
	db.Set(0, "skey", "abc");

	LOG_IF(FATAL, db.Type(0, "myset") != SET_ELEMENT) << "type failed.";
	LOG_IF(FATAL, db.Type(0, "mylist") != LIST_META) << "type failed.";
	LOG_IF(FATAL, db.Type(0, "myzset1") != ZSET_ELEMENT_SCORE)
																		<< "type failed.";
	LOG_IF(FATAL, db.Type(0, "myhash") != HASH_FIELD) << "type failed.";
	LOG_IF(FATAL, db.Type(0, "skey") != KV) << "type failed.";
}

void test_all(Ardb& db)
{
	test_strings(db);
	test_hashs(db);
	test_lists(db);
	test_sets(db);
	test_zsets(db);
	test_type(db);
}
