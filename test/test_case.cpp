/*
 * test_case.cpp
 *
 *  Created on: 2013-4-4
 *      Author: wqy
 */
#include "rddb.hpp"
#include <string>
#include <glog/logging.h>

using namespace rddb;

void test_strings(RDDB& db)
{
	ValueObject v;
	//append
	db.Set(0, "skey", "abc");
	db.Append(0, "skey", "abc");
	int ret = db.Get(0, "skey", v);
	LOG_IF(FATAL, ret != 0) << "Failed to get skey.";
	LOG_IF(FATAL, strcmp(v.v.raw->AsString().c_str(), "abcabc") != 0)
																				<< "Invalid str:"
																				<< v.v.raw->AsString();
	v.Clear();
	db.GetRange(0, "skey", 4, -1, v);
	LOG_IF(FATAL, strcmp(v.v.raw->AsString().c_str(), "bc") != 0)
																			<< "GetRange failed";
	db.SetRange(0, "skey", 3, "12345");
	v.Clear();
	db.Get(0, "skey", v);
	LOG_IF(FATAL, strcmp(v.v.raw->AsString().c_str(), "abc12345") != 0)
																				<< "SetRange failed:"
																				<< v.v.raw->AsString();
	v.Clear();
	db.GetSet(0, "skey", "edfgth", v);
	LOG_IF(FATAL, strcmp(v.v.raw->AsString().c_str(), "abc12345") != 0)
																				<< "GetSet failed";
	int len = db.Strlen(0, "skey");
	LOG_IF(FATAL, len != 6) << "Strlen failed";
	db.Set(0, "intkey", "010");
	int64_t iv = 0;
	db.Decr(0, "intkey", iv);
	LOG_IF(FATAL, iv != 9) << "Decr failed";
	db.Decrby(0, "intkey", 2, iv);
	LOG_IF(FATAL, iv != 7) << "Decrby failed";
	db.Incr(0, "intkey", iv);
	LOG_IF(FATAL, iv != 8) << "Incrby failed";
	db.Incrby(0, "intkey", 2, iv);
	LOG_IF(FATAL, iv != 10) << "Incrby failed";
	double dv;
	db.IncrbyFloat(0, "intkey", 1.23, dv);
	LOG_IF(FATAL, dv != 11.23) << "IncrbyFloat failed";

	LOG_IF(FATAL, db.Exists(0, "intkey1")) << "Exists intkey1 failed";
	LOG_IF(FATAL, db.Exists(0, "intkey") == false) << "Exists intkey failed";
	LOG_IF(FATAL, db.SetNX(0, "intkey", "2345") != 0) << "SetNX intkey failed";
	LOG_IF(FATAL, db.SetNX(0, "intkey1", "2345") != 1)
																<< "SetNX intkey2 failed";
	db.Del(0, "intkey1");

	db.Expire(0, "intkey", 10);
	ret = db.TTL(0, "intkey");
	LOG_IF(FATAL, ret != 10) << "Expire/TTL intkey failed:" << ret;
	db.Persist(0, "intkey");
	ret = db.TTL(0, "intkey");
	LOG_IF(FATAL, ret != 0) << "Persist intkey failed:" << ret;
}

void test_sets(RDDB& db)
{

}

void test_zsets(RDDB& db)
{

}

void test_lists(RDDB& db)
{

}

void test_hashs(RDDB& db)
{

}

