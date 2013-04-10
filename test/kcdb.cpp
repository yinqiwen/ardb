/*
 * leveldb.cpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */

#include "ardb.hpp"
#include "engine/kyotocabinet_engine.hpp"
#include <string>
#include <iostream>
#include <glog/logging.h>
#include "test_case.cpp"
#include <iostream>
using namespace std;

using namespace ardb;
using namespace kyotocabinet;

int main(int argc, char** argv)
{
	google::InitGoogleLogging(argv[0]);
	KCDBEngineFactory factory("/tmp/ardb/kc/test");
	Ardb db(&factory);
	test_all(db);

//// create the database object
//	TreeDB db;
//
//	// open the database
//	if (!db.open("casket.kct", TreeDB::OWRITER | TreeDB::OCREATE))
//	{
//		cerr << "open error: " << db.error().name() << endl;
//	}
//
//	// store records
//	if (!db.set("foo", "hop") || !db.set("bar", "step")
//			|| !db.set("baz", "jump"))
//	{
//		cerr << "set error: " << db.error().name() << endl;
//	}
//
//	// retrieve a record
//	string value;
//	if (db.get("foo", &value))
//	{
//		cout << value << endl;
//	} else
//	{
//		cerr << "get error: " << db.error().name() << endl;
//	}
//
//	// traverse records
//	DB::Cursor* cur = db.cursor();
//	bool ret = cur->jump("bas");
//	cout << ret << endl;
//	string ckey, cvalue;
//	while (cur->get(&ckey, &cvalue, true))
//	{
//		cout << ckey << ":" << cvalue << endl;
//	}
//	delete cur;
//
//	// close the database
//	if (!db.close())
//	{
//		cerr << "close error: " << db.error().name() << endl;
//	}

	return 0;
}
