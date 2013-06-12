/*
 * test_case.cpp
 *
 *  Created on: 2013-4-4
 *      Author: wqy
 */
#include "strings_testcase.cpp"
#include "lists_testcase.cpp"
#include "sets_testcase.cpp"
#include "hash_testcase.cpp"
#include "zset_testcase.cpp"
#include "table_testcase.cpp"
#include "bitset_testcase.cpp"
#include "misc_testcase.cpp"

void test_all(Ardb& db)
{
	test_strings(db);
	test_hashs(db);
	test_lists(db);
	test_sets(db);
	test_zsets(db);
	test_tables(db);
	test_bitsets(db);
	test_misc(db);
}
