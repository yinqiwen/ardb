/*
 * test_case.cpp
 *
 *  Created on: 2013-4-4
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

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
	CHECK_FATAL( db.Type(dbid, "myzset1") != ZSET_ELEMENT_SCORE,
	        "type failed.");
	CHECK_FATAL( db.Type(dbid, "myhash") != HASH_FIELD, "type failed.");
	CHECK_FATAL( db.Type(dbid, "skey") != KV, "type failed.");
	CHECK_FATAL( db.Type(dbid, "mybits") != BITSET_META, "type failed.");
}

void test_sort_list(Ardb& db)
{
	DBID dbid = 0;
	db.LClear(dbid, "mylist");
	db.RPush(dbid, "mylist", "100");
	db.RPush(dbid, "mylist", "10");
	db.RPush(dbid, "mylist", "9");
	db.RPush(dbid, "mylist", "1000");

	StringArray args;
	ValueArray vs;
	db.Sort(dbid, "mylist", args, vs);
	CHECK_FATAL(vs.size() != 4, "sort result size error:%zu", vs.size());
	CHECK_FATAL(vs[0].v.int_v != 9, "sort result[0]:%"PRId64, vs[0].v.int_v);
	CHECK_FATAL(vs[1].v.int_v != 10, "sort result[0]:%"PRId64, vs[1].v.int_v);
	CHECK_FATAL(vs[2].v.int_v != 100, "sort result[0]:%"PRId64, vs[2].v.int_v);
	CHECK_FATAL(vs[3].v.int_v != 1000, "sort result[0]:%"PRId64, vs[3].v.int_v);

	vs.clear();
	args.push_back("LIMIT");
	args.push_back("1");
	args.push_back("2");
	db.Sort(dbid, "mylist", args, vs);
	CHECK_FATAL(vs.size() != 2, "sort result size error:%zu", vs.size());
	CHECK_FATAL(vs[0].v.int_v != 10, "sort result[0]:%lld", vs[0].v.int_v);
	CHECK_FATAL(vs[1].v.int_v != 100, "sort result[0]:%lld", vs[1].v.int_v);

	vs.clear();
	args.clear();
	args.push_back("by");
	args.push_back("weight_*");
	db.Set(dbid, "weight_100", "1000");
	db.Set(dbid, "weight_10", "900");
	db.Set(dbid, "weight_9", "800");
	db.Set(dbid, "weight_1000", "700");
	db.Sort(dbid, "mylist", args, vs);
	CHECK_FATAL(vs.size() != 4, "sort result size error:%zu", vs.size());
	CHECK_FATAL(vs[0].v.int_v != 1000, "sort result[0]:%lld", vs[0].v.int_v);
	CHECK_FATAL(vs[1].v.int_v != 9, "sort result[0]:%lld", vs[1].v.int_v);
	CHECK_FATAL(vs[2].v.int_v != 10, "sort result[0]:%lld", vs[2].v.int_v);
	CHECK_FATAL(vs[3].v.int_v != 100, "sort result[0]:%lld", vs[3].v.int_v);

	db.HSet(dbid, "myhash", "field_100", "hash100");
	db.HSet(dbid, "myhash", "field_10", "hash10");
	db.HSet(dbid, "myhash", "field_9", "hash9");
	db.HSet(dbid, "myhash", "field_1000", "hash1000");
	args.push_back("get");
	args.push_back("myhash->field_*");
	args.push_back("get");
	args.push_back("#");

	vs.clear();
	db.Sort(dbid, "mylist", args, vs);
	std::string str;
	CHECK_FATAL(vs.size() != 8, "sort result size error:%zu", vs.size());
	CHECK_FATAL(vs[0].ToString(str) != "hash1000", "sort result[0]:%s", str.c_str());
	CHECK_FATAL(vs[2].ToString(str) != "hash9", "sort result[2]:%s", str.c_str());
	CHECK_FATAL(vs[4].ToString(str) != "hash10", "sort result[4]:%s", str.c_str());
	CHECK_FATAL(vs[6].ToString(str) != "hash100", "sort result[6]:%s", str.c_str());
	CHECK_FATAL(vs[1].v.int_v != 1000, "sort result[1]:%"PRId64, vs[1].v.int_v);
	CHECK_FATAL(vs[3].v.int_v != 9, "sort result[3]:%"PRId64, vs[3].v.int_v);
	CHECK_FATAL(vs[5].v.int_v != 10, "sort result[5]:%"PRId64, vs[5].v.int_v);
	CHECK_FATAL(vs[7].v.int_v != 100, "sort result[7]:%"PRId64, vs[7].v.int_v);
}


void test_sort_set(Ardb& db)
{
	DBID dbid = 0;
	db.SClear(dbid, "myset");
	db.SAdd(dbid, "myset", "ab3");
	db.SAdd(dbid, "myset", "ab2");
	db.SAdd(dbid, "myset", "ab1");
	db.SAdd(dbid, "myset", "ab4");

	StringArray args;
	ValueArray vs;
	db.Sort(dbid, "myset", args, vs);
	std::string str;
	CHECK_FATAL(vs.size() != 4, "sort result size error:%zu", vs.size());
	CHECK_FATAL(vs[0].ToString(str) != "ab1", "sort result[0]:%s", str.c_str());
	CHECK_FATAL(vs[1].ToString(str) != "ab2", "sort result[1]:%s", str.c_str());
	CHECK_FATAL(vs[2].ToString(str) != "ab3", "sort result[2]:%s", str.c_str());
	CHECK_FATAL(vs[3].ToString(str) != "ab4", "sort result[3]:%s", str.c_str());

	vs.clear();
	args.push_back("LIMIT");
	args.push_back("1");
	args.push_back("2");
	db.Sort(dbid, "myset", args, vs);
	CHECK_FATAL(vs.size() != 2, "sort result size error:%zu", vs.size());
	CHECK_FATAL(vs[0].ToString(str) != "ab2", "sort result[0]:%s", str.c_str());
	CHECK_FATAL(vs[1].ToString(str) != "ab3", "sort result[1]:%s", str.c_str());

	vs.clear();
	args.clear();
	args.push_back("by");
	args.push_back("weight_*");
	db.Set(dbid, "weight_ab1", "1000");
	db.Set(dbid, "weight_ab2", "900");
	db.Set(dbid, "weight_ab3", "800");
	db.Set(dbid, "weight_ab4", "700");
	db.Sort(dbid, "myset", args, vs);
	CHECK_FATAL(vs.size() != 4, "sort result size error:%zu", vs.size());
	CHECK_FATAL(vs[0].ToString(str) != "ab4", "sort result[0]:%s", str.c_str());
	CHECK_FATAL(vs[1].ToString(str) != "ab3", "sort result[1]:%s", str.c_str());
	CHECK_FATAL(vs[2].ToString(str) != "ab2", "sort result[2]:%s", str.c_str());
	CHECK_FATAL(vs[3].ToString(str) != "ab1", "sort result[3]:%s", str.c_str());

	db.HSet(dbid, "myhash_ab1", "field", "hash100");
	db.HSet(dbid, "myhash_ab2", "field", "hash10");
	db.HSet(dbid, "myhash_ab3", "field", "hash9");
	db.HSet(dbid, "myhash_ab4", "field", "hash1000");
	args.push_back("get");
	args.push_back("myhash_*->field");
	args.push_back("get");
	args.push_back("#");

	vs.clear();
	db.Sort(dbid, "myset", args, vs);
	CHECK_FATAL(vs.size() != 8, "sort result size error:%zu", vs.size());
	CHECK_FATAL(vs[0].ToString(str) != "hash1000", "sort result[0]:%s", str.c_str());
	CHECK_FATAL(vs[2].ToString(str) != "hash9", "sort result[2]:%s", str.c_str());
	CHECK_FATAL(vs[4].ToString(str) != "hash10", "sort result[4]:%s", str.c_str());
	CHECK_FATAL(vs[6].ToString(str) != "hash100", "sort result[6]:%s", str.c_str());
	CHECK_FATAL(vs[1].ToString(str) != "ab4", "sort result[1]:%s", str.c_str());
	CHECK_FATAL(vs[3].ToString(str) != "ab3", "sort result[3]:%s", str.c_str());
	CHECK_FATAL(vs[5].ToString(str) != "ab2", "sort result[5]:%s", str.c_str());
	CHECK_FATAL(vs[7].ToString(str) != "ab1", "sort result[7]:%s", str.c_str());
}

void test_misc(Ardb& db)
{
	test_type(db);
	test_sort_list(db);
	test_sort_set(db);
}
