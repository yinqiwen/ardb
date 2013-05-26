/*
 * lmdb.cpp
 *
 *  Created on: 2013-5-10
 *      Author: wqy
 */
#include "ardb.hpp"
#include "engine/lmdb_engine.hpp"
#include <string>
#include <iostream>
#include "test_case.cpp"

using namespace ardb;

int main(int argc, char** argv)
{
	Properties cfg;
	cfg["dir"] = "/tmp/ardb/lmdb/test";
	std::cout << "ARDB Test(LMDB)" << std::endl;
	LMDBEngineFactory factory(cfg);
	Ardb db(&factory, cfg["dir"]);
	db.Init();
	test_all(db);
	return 0;
}

