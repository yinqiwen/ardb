/*
 * leveldb.cpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */

#include "leveldb/db.h"
#include <string>
#include <iostream>

int main(int argc, char** argv)
{
	leveldb::DB* db;
	leveldb::Options options;
	options.create_if_missing = true;
	leveldb::Status status = leveldb::DB::Open(options, "./testdb", &db);
	std::string value = "1";
	for (int i = 0; i < 100000; i++)
	{
		char tmp[100];
		sprintf(tmp, "%d", i);
		db->Put(leveldb::WriteOptions(), tmp, value);
	}
	std::cout << "Start seek" << std::endl;
	leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
	it->Seek("1234506");
	if ( it->Valid())
	{
		std::cout << it->key().ToString() << ": " << it->value().ToString() << std::endl;
	}
	std::cout << "End seek" << std::endl;

	return 0;

}
