/*
 * strings.testcase.cpp
 *
 *  Created on: 2013-4-9
 *      Author: yinqiwen
 */
#include "db.hpp"
#include <string>

using namespace ardb;

void test_bitcount(Ardb& db)
{
	DBID dbid = 0;
	db.Del(dbid, "mybits");
	db.SetBit(dbid, "mybits", 7, 1);
	int bitcount = db.BitCount(dbid, "mybits", 0, -1);
	CHECK_FATAL(bitcount != 1, "bitcount mybits failed:%d", bitcount);
	db.SetBit(dbid, "mybits", 4098, 1);
	bitcount = db.BitCount(dbid, "mybits", 0, -1);
	CHECK_FATAL(bitcount!= 2, "bitcount mybits failed:%d", bitcount);
	bitcount = db.BitCount(dbid, "mybits", 1, 9);
	CHECK_FATAL(bitcount != 1, "bitcount mybits failed:%d", bitcount);
}

void test_setgetbit(Ardb& db)
{
	DBID dbid = 0;
	db.Del(dbid, "mybits");
	int ret = db.SetBit(dbid, "mybits", 7, 1);
	CHECK_FATAL(ret != 0, "setbit mybits failed:%d", ret);
	ret = db.GetBit(dbid, "mybits", 7);
	CHECK_FATAL(ret != 1, "getbit mybits failed:%d", ret);
	ret = db.SetBit(dbid, "mybits", 7, 0);
	CHECK_FATAL( ret != 1, "setbit mybits failed:%d", ret);
	ret = db.GetBit(dbid, "mybits", 7);
	CHECK_FATAL(ret != 0, "getbit mybits failed:%d", ret);
}

void test_bitop(Ardb& db)
{
	DBID dbid = 0;
	db.Del(dbid, "mybits1");
	db.Del(dbid, "mybits2");
	for (uint32 i = 1000; i < 5000; i++)
	{
		db.SetBit(dbid, "mybits1", i, 1);
	}

	for (uint32 i = 4000; i < 6000; i++)
	{
		db.SetBit(dbid, "mybits2", i, 1);
	}

	SliceArray keys;
	keys.push_back("mybits1");
	keys.push_back("mybits2");
	int ret = db.BitOPCount(dbid, "and", keys);
	CHECK_FATAL(ret != 1000, "bitopcount and keys failed:%d", ret);
	ret = db.BitOPCount(dbid, "or", keys);
	CHECK_FATAL( ret != 5000, "bitopcount or keys failed:%d", ret);
	ret = db.BitOPCount(dbid, "xor", keys);
	CHECK_FATAL( ret != 4000, "bitopcount xor keys failed:%d", ret);
}

void test_bitsets(Ardb& db)
{
	test_bitcount(db);
	test_setgetbit(db);
	test_bitop(db);
}

