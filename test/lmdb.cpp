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
	LMDBEngineFactory factory(cfg);
	Ardb db(&factory);
	test_all(db);
	return 0;
}



