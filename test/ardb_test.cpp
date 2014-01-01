/*
 * leveldb.cpp
 *
 *  Created on: 2013-3-27
 *      Author: yinqiwen
 */

#include "test_common.hpp"
#ifdef __USE_KYOTOCABINET__
#include "engine/kyotocabinet_engine.hpp"
typedef ardb::KCDBEngineFactory SelectedDBEngineFactory;
#define _DB_PATH "KyotoCabinet"
#elif defined __USE_LMDB__
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
#include "test_case.cpp"
using namespace ardb;

void string_to_string_array(const std::string& str, StringArray& array)
{
	array.clear();
	std::vector<std::string> strs = split_string(str, " ");
	std::vector<std::string>::iterator it = strs.begin();
	while(it != strs.end())
	{
		array.push_back(*it);
		it++;
	}
}

void strings_to_slices(StringArray& strs, SliceArray& array)
{
	StringArray::iterator it = strs.begin();
	while(it != strs.end())
	{
		array.push_back(*it);
		it++;
	}
}

int main(int argc, char** argv)
{
	Properties cfg;
	cfg["data-dir"] = "/tmp/ardb/";
	cfg["data-dir"].append(_DB_PATH);
	SelectedDBEngineFactory engine(cfg);
	std::cout << "ARDB Test(" << engine.GetName() << ")" << std::endl;
	Ardb db(&engine);
	ArdbConfig config;
	config.hash_max_ziplist_entries = 16;
	config.zset_max_ziplist_entries = 16;
	config.set_max_ziplist_entries = 16;
	config.list_max_ziplist_entries = 16;
	db.Init(config);
	test_all(db);
	return 0;
}
