/*
 * strings.cpp
 *
 *  Created on: 2013-3-29
 *      Author: wqy
 */
#include "rddb.hpp"

namespace rddb
{
	int RDDB::Append(DBID db, const void* key, int keysize, const char* value,
			int valuesize)
	{
		std::string strvalue;
		Get(db, key, keysize, &strvalue);
		strvalue.append(value, valuesize);
		int ret = Set(db, key, keysize, strvalue.c_str(), strvalue.size());
		if (0 == ret)
		{
			return strvalue.size();
		}
		return ret;
	}

	int RDDB::Decr(DBID db, const void* key, int keysize, int64_t& value)
	{
		std::string strvalue;
		if (Get(db, key, keysize, &strvalue) < 0)
		{
			return -1;
		}
		if (str_toint64(strvalue.c_str(), value))
		{
			value--;
			return value;
		} else
		{
			return -1;
		}
	}

}

