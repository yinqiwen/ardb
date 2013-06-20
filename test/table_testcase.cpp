/*
 * table.cpp
 *
 *  Created on: 2013-4-13
 *      Author: yinqiwen
 */
#include "test_common.hpp"

void test_table_insert_get(Ardb& db)
{
	DBID dbid = 0;
	db.TClear(dbid, "mytable");
	StringArray strs;
	string_to_string_array("key1 key2 key3", strs);
	SliceArray array;
	strings_to_slices(strs, array);
	db.TCreate(dbid, "mytable", array);

	TableInsertOptions insert_options;
	string_to_string_array("6 key1 1 key2 2 key3 3 name ardb age 20 birth 1999",
	        strs);
	TableInsertOptions::Parse(strs, 0, insert_options);
	std::string err;
	db.TInsert(dbid, "mytable", insert_options, false, err);

	ValueArray result;
	TableQueryOptions options;
	string_to_string_array("birth,name,age where key2<2", strs);
	TableQueryOptions::Parse(strs, 0, options);
	db.TGet(dbid, "mytable", options, result, err);
	CHECK_FATAL( result.size() != 0, "%zu", result.size());

	string_to_string_array("4 key2 birth name age where key2<=2", strs);
	TableQueryOptions::Parse(strs, 0, options);
	db.TGet(dbid, "mytable", options, result, err);
	std::string str;
	CHECK_FATAL( result.size() != 4, "%zu", result.size());
	CHECK_FATAL( result[0].ToString(str) != "2",
	        "%s", result[0].ToString(str).c_str());
	CHECK_FATAL( result[1].ToString(str) != "1999",
	        "%s", result[1].ToString(str).c_str());
	CHECK_FATAL( result[2].ToString(str) != "ardb",
	        "%s", result[2].ToString(str).c_str());
	CHECK_FATAL( result[3].ToString(str) != "20",
	        "%s", result[3].ToString(str).c_str());
}

void test_table_update(Ardb& db)
{
	DBID dbid = 0;
	db.TClear(dbid, "mytable");
	StringArray strs;
	string_to_string_array("key1 key2 key3", strs);
	SliceArray array;
	strings_to_slices(strs, array);
	db.TCreate(dbid, "mytable", array);

	TableInsertOptions insert_options;
	string_to_string_array(
	        "6 key1 10 key2 20 key3 30 name ardb age 20 birth 1999", strs);
	TableInsertOptions::Parse(strs, 0, insert_options);
	std::string err;
	db.TInsert(dbid, "mytable", insert_options, false, err);
	TableUpdateOptions update_options;
	string_to_string_array("name=newdb,age=30,birth=2000 where key2>5", strs);
	TableUpdateOptions::Parse(strs, 0, update_options);
	db.TUpdate(dbid, "mytable", update_options);

	TableQueryOptions options;
	string_to_string_array("2 age name where age>=20", strs);
	TableQueryOptions::Parse(strs, 0, options);
	ValueArray result;
	db.TGet(dbid, "mytable", options, result, err);
	std::string str;
	CHECK_FATAL( result.size() != 2, "%zu", result.size());
	CHECK_FATAL( result[0].ToString(str) != "30",
	        "%s", result[0].ToString(str).c_str());
	CHECK_FATAL( result[1].ToString(str) != "newdb",
	        "%s", result[1].ToString(str).c_str());
}

void test_table_replace(Ardb& db)
{
	DBID dbid = 0;
	db.TClear(dbid, "mytable");
	StringArray strs;
	string_to_string_array("key1 key2 key3", strs);
	SliceArray array;
	strings_to_slices(strs, array);
	db.TCreate(dbid, "mytable", array);

	TableInsertOptions insert_options;
	string_to_string_array("6 key1 1 key2 2 key3 3 name ardb age 20 birth 1999",
	        strs);
	TableInsertOptions::Parse(strs, 0, insert_options);
	std::string err;
	db.TInsert(dbid, "mytable", insert_options, false, err);
	string_to_string_array(
	        "6 key1 1 key2 2 key3 3 name unknown age 21 birth 1989", strs);
	TableInsertOptions::Parse(strs, 0, insert_options);
	db.TInsert(dbid, "mytable", insert_options, true, err);

	ValueArray result;
	TableQueryOptions options;
	string_to_string_array("3 birth name age where key2<=2", strs);
	TableQueryOptions::Parse(strs, 0, options);
	db.TGet(dbid, "mytable", options, result, err);

	std::string str;
	CHECK_FATAL( result.size() != 3, "%zu", result.size());
	CHECK_FATAL( result[0].ToString(str) != "1989",
	        "%s", result[0].ToString(str).c_str());
	CHECK_FATAL( result[1].ToString(str) != "unknown",
	        "%s", result[1].ToString(str).c_str());
	CHECK_FATAL( result[2].ToString(str) != "21",
	        "%s", result[2].ToString(str).c_str());
}

void test_table_get(Ardb& db)
{
	DBID dbid = 0;
	db.TClear(dbid, "mytable");
	StringArray strs;
	string_to_string_array("key1 key2 key3", strs);
	SliceArray array;
	strings_to_slices(strs, array);
	db.TCreate(dbid, "mytable", array);

	TableInsertOptions insert_options;
	uint32 total_age = 0;
	for (uint32 i = 0; i < 101; i++)
	{
		strs.clear();
		char tmp[1024];
		sprintf(tmp, "6 key1 %u key2 %u key3 3 name XXX age %u birth %u", i,
		        i + 1, 20 + i, 1980 + i);
		total_age += (20 + i);

		string_to_string_array(tmp, strs);
		TableInsertOptions::Parse(strs, 0, insert_options);
		std::string err;
		db.TInsert(dbid, "mytable", insert_options, false, err);
	}

	int count = db.TCount(dbid, "mytable");
	CHECK_FATAL(count != 101, "%d", count);

	std::string err;
	std::string str;
	ValueArray result;
	TableQueryOptions options;
	string_to_string_array("1 age aggregate sum", strs);
	TableQueryOptions::Parse(strs, 0, options);
	db.TGet(dbid, "mytable", options, result, err);
	CHECK_FATAL( result.size() != 1, "%zu  %s", result.size(), err.c_str());
	CHECK_FATAL( result[0].ToString(str) != "7070",
	        "%s", result[0].ToString(str).c_str());

	string_to_string_array("3 key1 name age orderby key1 desc where key1<=50",
	        strs);
	TableQueryOptions::Parse(strs, 0, options);
	db.TGet(dbid, "mytable", options, result, err);
	CHECK_FATAL( result.size() != 51*3, "%zu", result.size());
	CHECK_FATAL( result[0].ToString(str) != "50",
	        "%s", result[0].ToString(str).c_str());

	string_to_string_array("1 birth aggregate count where key1<=3", strs);
	TableQueryOptions::Parse(strs, 0, options);
	db.TGet(dbid, "mytable", options, result, err);
	CHECK_FATAL( result.size() != 1, "%zu", result.size());
	CHECK_FATAL( result[0].ToString(str) != "4",
	        "%s", result[0].ToString(str).c_str());
}

void test_table_delcol(Ardb& db)
{
	DBID dbid = 0;
	db.TClear(dbid, "mytable");
	StringArray strs;
	string_to_string_array("key1 key2 key3", strs);
	SliceArray array;
	strings_to_slices(strs, array);
	db.TCreate(dbid, "mytable", array);

	TableInsertOptions insert_options;
	uint32 total_age = 0;
	for (uint32 i = 0; i < 101; i++)
	{
		strs.clear();
		char tmp[1024];
		sprintf(tmp, "6 key1 %u key2 %u key3 3 name XXX age %u birth %u", i,
		        i + 1, 20 + i, 1980 + i);
		total_age += (20 + i);

		string_to_string_array(tmp, strs);
		TableInsertOptions::Parse(strs, 0, insert_options);
		std::string err;
		db.TInsert(dbid, "mytable", insert_options, false, err);
	}

	db.TDelCol(dbid, "mytable", "age");

	std::string err;
	std::string str;
	string_to_string_array("1 age aggregate count", strs);
	ValueArray result;
	TableQueryOptions options;
	TableQueryOptions::Parse(strs, 0, options);
	db.TGet(dbid, "mytable", options, result, err);
	CHECK_FATAL( result.size() != 1, "%zu", result.size());
	CHECK_FATAL( result[0].ToString(str) != "0",
	        "%s", result[0].ToString(str).c_str());
}

void test_table_getall(Ardb& db)
{
	DBID dbid = 0;
	db.TClear(dbid, "mytable");
	StringArray strs;
	string_to_string_array("key1 key2 key3", strs);
	SliceArray array;
	strings_to_slices(strs, array);
	db.TCreate(dbid, "mytable", array);

	TableInsertOptions insert_options;
	uint32 total_age = 0;
	for (uint32 i = 0; i < 1001; i++)
	{
		strs.clear();
		char tmp[1024];
		sprintf(tmp, "6 key1 %u key2 %u key3 3 name XXX age %u birth %u", i,
		        i + 1, 20 + i, 1980 + i);
		total_age += (20 + i);

		string_to_string_array(tmp, strs);
		TableInsertOptions::Parse(strs, 0, insert_options);
		std::string err;
		db.TInsert(dbid, "mytable", insert_options, false, err);
	}

	std::string str;
	ValueArray result;
	db.TGetAll(dbid, "mytable", result);
	CHECK_FATAL( result.size() != 6006, "%zu", result.size());
	CHECK_FATAL( result[1].ToString(str) != "1",
	        "%s", result[1].ToString(str).c_str());
	CHECK_FATAL( result[5].ToString(str) != "XXX",
	        "%s", result[5].ToString(str).c_str());
	CHECK_FATAL( result[6004].ToString(str) != "2980",
	        "%s", result[6005].ToString(str).c_str());
}

void test_table_create_index(Ardb& db)
{
	DBID dbid = 0;
	db.TClear(dbid, "mytable");
	StringArray strs;
	string_to_string_array("key1 key2 key3", strs);
	SliceArray array;
	strings_to_slices(strs, array);
	db.TCreate(dbid, "mytable", array);

	TableInsertOptions insert_options;
	uint32 total_age = 0;
	for (uint32 i = 0; i < 20001; i++)
	{
		strs.clear();
		char tmp[1024];
		sprintf(tmp, "6 key1 %u key2 %u key3 3 name XXX age %u birth %u", i,
		        i + 1, 20 + i, 1980 + i);
		total_age += (20 + i);

		string_to_string_array(tmp, strs);
		TableInsertOptions::Parse(strs, 0, insert_options);
		std::string err;
		db.TInsert(dbid, "mytable", insert_options, false, err);
	}

	std::string str, err;
	ValueArray result;
	string_to_string_array("2 key1 age where birth=11980", strs);
	TableQueryOptions options;
	TableQueryOptions::Parse(strs, 0, options);
	uint64 start = get_current_epoch_millis();
	db.TGet(dbid, "mytable", options, result, err);
	uint64 end = get_current_epoch_millis();
	CHECK_FATAL( result.size() != 2, "%zu", result.size());
	CHECK_FATAL( result[0].ToString(str) != "10000",
	        "%s", result[0].ToString(str).c_str());
	CHECK_FATAL( result[1].ToString(str) != "10020",
	        "%s", result[1].ToString(str).c_str());
	CHECK_FATAL((end-start) < 10, "%"PRIu64, (end-start));
	db.TCreateIndex(dbid, "mytable", "birth");
	start = get_current_epoch_millis();
	db.TGet(dbid, "mytable", options, result, err);
	end = get_current_epoch_millis();
	CHECK_FATAL( result.size() != 2, "%zu", result.size());
	CHECK_FATAL( result[0].ToString(str) != "10000",
	        "%s", result[0].ToString(str).c_str());
	CHECK_FATAL( result[1].ToString(str) != "10020",
	        "%s", result[1].ToString(str).c_str());
	CHECK_FATAL((end-start) > 10, "%"PRIu64, (end-start));
}

void test_table_expire(Ardb& db)
{
	DBID dbid = 0;
	db.TClear(dbid, "mytable");
	StringArray strs;
	string_to_string_array("key1 key2 key3", strs);
	SliceArray array;
	strings_to_slices(strs, array);
	db.TCreate(dbid, "mytable", array);
	db.Expire(dbid, "mytable", 1);
	CHECK_FATAL(db.Exists(dbid, "mytable") == false, "Expire mytable failed");
	sleep(2);
	CHECK_FATAL(db.Exists(dbid, "mytable") == true, "Expire mytable failed");
}

void test_tables(Ardb& db)
{
	test_table_insert_get(db);
	test_table_update(db);
	test_table_replace(db);
	test_table_get(db);
	test_table_delcol(db);
	test_table_getall(db);
	test_table_create_index(db);
	test_table_expire(db);
}
