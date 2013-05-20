/*
 * leveldb.cpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */

#include "ardb.hpp"
#include "engine/leveldb_engine.hpp"
#include <string>
#include <iostream>
#include "test_case.cpp"

using namespace ardb;

int main(int argc, char** argv)
{
	Properties cfg;
	cfg["dir"] = "/tmp/ardb/test";
	std::cout<<"ARDB Test(LevelDB)"<<std::endl;
	LevelDBEngineFactory factory(cfg);
	Ardb db(&factory, cfg["dir"]);
	test_all(db);
	return 0;
}
