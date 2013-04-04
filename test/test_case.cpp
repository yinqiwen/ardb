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
	int len = db.Strlen(0, "skey");
	LOG_IF(FATAL, len != 6) << "Strlen failed";
	db.Set(0, "intkey", "10");
	int64_t iv = 0;
	db.Decr(0, "intkey", iv);
	LOG_IF(FATAL, iv != 9) << "Decr failed";
	db.Decrby(0, "intkey", 2, iv);
	LOG_IF(FATAL, iv != 7) << "Decrby failed";
}
