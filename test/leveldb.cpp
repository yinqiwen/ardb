/*
 * leveldb.cpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */

#include "rddb.hpp"
#include "engine/leveldb_engine.hpp"
#include <string>
#include <iostream>
#include <glog/logging.h>

using namespace rddb;

int main(int argc, char** argv)
{
	google::InitGoogleLogging(argv[0]);
	LOG(INFO)<< "Found " << 1 << " cookies";
//	LevelDBEngineFactory factory("/tmp/rddb/test");
//	RDDB db(&factory);
//	Slice key("key");
//	Slice field("filed");
//	Slice value("123345");
//	db.HSet(0, key, field, value);
	return 0;
}
