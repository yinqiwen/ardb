/*
 * table.cpp
 *
 *  Created on: 2013-4-13
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string>

using namespace ardb;

void test_table_insert_get(Ardb& db)
{
	DBID dbid = 0;
	db.TClear(dbid, "mytable");
	SliceArray array;
	array.push_back("key1");
	array.push_back("key2");
	array.push_back("key3");
	db.TCreate(dbid, "mytable", array);

	TableInsertOptions insert_options;
	insert_options.nvs["key1"] = "1";
	insert_options.nvs["key2"] = "2";
	insert_options.nvs["key3"] = "3";

	insert_options.nvs["name"] = "ardb";
	insert_options.nvs["age"] = "20";
	insert_options.nvs["birth"] = "1999";
	std::string err;
	db.TInsert(dbid, "mytable", insert_options, false, err);

	ValueArray result;
	TableQueryOptions options;
	options.names.push_back("birth");
	options.names.push_back("name");
	Condition cond("key2", CMP_LESS_EQ, "2");
	options.conds.push_back(cond);
	db.TGet(dbid, "mytable",options, result, err);

	std::string str;
	CHECK_FATAL( result.size() != 2, "%d", result.size());
	CHECK_FATAL( result[0].ToString(str) != "1999", "%s", result[0].ToString(str).c_str());
	CHECK_FATAL( result[1].ToString(str) != "ardb", "%s", result[0].ToString(str).c_str());
}

void test_table_update(Ardb& db)
{
	DBID dbid = 0;
	db.TClear(dbid, "mytable");
	SliceArray array;
	array.push_back("key1");
	array.push_back("key2");
	array.push_back("key3");
	db.TCreate(dbid, "mytable", array);

	TableInsertOptions insert_options;
	insert_options.nvs["key1"] = "10";
	insert_options.nvs["key2"] = "20";
	insert_options.nvs["key3"] = "30";
	insert_options.nvs["name"] = "ardb";
	insert_options.nvs["age"] = "20";
	insert_options.nvs["birth"] = "1999";
	std::string err;
	db.TInsert(dbid, "mytable", insert_options, false, err);
	TableUpdateOptions update_options;
	Condition cond("key2", CMP_GREATE, "5");
	update_options.conds.push_back(cond);
	update_options.colnvs["name"] = "newdb";
	update_options.colnvs["age"] = "30";
	update_options.colnvs["birth"] = "2000";
	db.TUpdate(dbid, "mytable", update_options);

	TableQueryOptions options;
	options.names.push_back("age");
	options.names.push_back("name");
	ValueArray result;
	Condition xcond("key2", CMP_GREATE, "2");
	options.conds.push_back(xcond);
	db.TGet(dbid, "mytable", options, result, err);

	std::string str;
	CHECK_FATAL( result.size() != 2, "%d", result.size());
	CHECK_FATAL( result[0].ToString(str) != "30", "%s", result[0].ToString(str).c_str());
	CHECK_FATAL( result[1].ToString(str) != "newdb", "%s", result[0].ToString(str).c_str());
}

void test_tables(Ardb& db)
{
	test_table_insert_get(db);
	test_table_update(db);
}
