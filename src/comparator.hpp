/*
 * comparator.hpp
 *
 *  Created on: 2013-4-10
 *      Author: wqy
 */

#ifndef COMPARATOR_HPP_
#define COMPARATOR_HPP_

#include "ardb.hpp"

#define COMPARE_EXIST(a, b)  do{ \
	if(!a && !b) return 0;\
	if(a != b) return COMPARE_NUMBER(a,b); \
}while(0)

#define RETURN_NONEQ_RESULT(a, b)  do{ \
	if(a != b) return COMPARE_NUMBER(a,b); \
}while(0)

namespace ardb
{
	 int ardb_compare_keys(const char* akbuf, size_t aksiz,
			const char* bkbuf, size_t bksiz);
}

#endif /* COMPARATOR_HPP_ */
