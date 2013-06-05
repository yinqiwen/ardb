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
#include "table_testcase.cpp"
#include "bitset_testcase.cpp"

void test_type(Ardb& db)
{
	DBID dbid = 0;
	db.SAdd(dbid, "myset", "123");
	db.LPush(dbid, "mylist", "value0");
	db.ZAdd(dbid, "myzset1", 1, "one");
	db.HSet(dbid, "myhash", "field1", "value1");
	db.Set(dbid, "skey", "abc");
	db.SetBit(dbid, "mybits", 1, 1);

	CHECK_FATAL( db.Type(dbid, "myset") != SET_ELEMENT, "type failed.");
	CHECK_FATAL( db.Type(dbid, "mylist") != LIST_META, "type failed.");
	CHECK_FATAL( db.Type(dbid, "myzset1") != ZSET_ELEMENT_SCORE, "type failed.");
	CHECK_FATAL( db.Type(dbid, "myhash") != HASH_FIELD, "type failed.");
	CHECK_FATAL( db.Type(dbid, "skey") != KV, "type failed.");
	CHECK_FATAL( db.Type(dbid, "mybits") != BITSET_META, "type failed.");
}

void test_all(Ardb& db)
{
	test_strings(db);
	test_hashs(db);
	test_lists(db);
	test_sets(db);
	test_zsets(db);
	test_tables(db);
	test_bitsets(db);
	test_type(db);
	//db.PrintDB(dbid);
}
