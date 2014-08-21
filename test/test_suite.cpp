/*
 * leveldb.cpp
 *
 *  Created on: 2013-3-27
 *      Author: yinqiwen
 */

#if defined __USE_LMDB__
#include "engine/lmdb_engine.hpp"
#define _DB_PATH "LMDB"
typedef ardb::LMDBEngineFactory SelectedDBEngineFactory;
#elif defined __USE_ROCKSDB__
#define _DB_PATH "RocksDB"
#include "engine/rocksdb_engine.hpp"
typedef ardb::RocksDBEngineFactory SelectedDBEngineFactory;
#else
#include "engine/leveldb_engine.hpp"
typedef ardb::LevelDBEngineFactory SelectedDBEngineFactory;
#define _DB_PATH "LevelDB"
#endif
#include <string>
#include <iostream>
#include "string_test.cpp"
#include "list_test.cpp"
#include "hash_test.cpp"
#include "set_test.cpp"
#include "zset_test.cpp"
#include "keys_test.cpp"
#include "bitset_test.cpp"
#include "misc_test.cpp"
#include "script_test.cpp"
#include "geo_test.cpp"
using namespace ardb;


int main(int argc, char** argv)
{
	Properties cfg;
	std::string path = "/tmp/ardb/";
	conf_set(cfg, "data-dir", "/tmp/ardb/");
	ArdbLogger::SetLogLevel("INFO");
	SelectedDBEngineFactory engine(cfg);
	std::cout << "ARDB Test(" << engine.GetName() << ")" << std::endl;
	Ardb db(engine);
	ArdbConfig conf;
	conf.Parse(cfg);
	if(0 != db.Init(conf))
	{
	    return 0;
	}

	test_string(db);
	test_list(db);
	test_hash(db);
	test_set(db);
	test_zset(db);
	test_keys(db);
	test_bitset(db);
	test_misc(db);
	test_scripts(db);
	test_geo(db);

	return 0;
}
