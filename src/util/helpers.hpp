/*
 * helpers.hpp
 *
 *  Created on: 2013-3-28
 *      Author: wqy
 */

#ifndef HELPERS_HPP_
#define HELPERS_HPP_
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <stdlib.h>
#include <string>
#include "common.hpp"
#include "time_helper.hpp"
#include "file_helper.hpp"
#include "string_helper.hpp"
#include "math_helper.hpp"
#include "network_helper.hpp"

namespace ardb
{
	inline bool raw_toint64(const void* raw, uint32_t len, int64_t& value)
	{
		if (NULL == raw)
		{
			return false;
		}
		const char* str = (const char*) raw;
		char *endptr = NULL;
		long long int val = strtoll(str, &endptr, 10);
		if (NULL == endptr || (endptr - str) != len)
		{
			return false;
		}

		value = val;
		return true;
	}


	inline bool raw_todouble(const void* raw, uint32_t len, double& value)
	{
		if (NULL == raw)
		{
			return false;
		}
		const char* str = (const char*) raw;
		char *endptr = NULL;
		double val = strtod(str, &endptr);
		if (NULL == endptr || (endptr - str) != len)
		{
			return false;
		}

		value = val;
		return true;
	}
}

#endif /* HELPERS_HPP_ */
