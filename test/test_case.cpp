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

void test_all(RDDB& db)
{
	test_strings(db);
	test_hashs(db);
	test_lists(db);
	test_sets(db);
	test_zsets(db);
}
