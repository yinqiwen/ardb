/*
 * math_helper.cpp
 *
 *  Created on: 2011-5-1
 *      Author: wqy
 */
#include "util/math_helper.hpp"
#include <stdlib.h>
#include <time.h>

namespace ardb
{
	uint32 upper_power_of_two(uint32 v)
	{
		v--;
		v |= v >> 1;
		v |= v >> 2;
		v |= v >> 4;
		v |= v >> 8;
		v |= v >> 16;
		v++;
		return v;
	}

	int32 random_int32()
	{
		srandom(time(NULL));
		return random();
	}

	static const uint64 P12 = 10000L * 10000L * 10000L;
	static const uint64 P11 = 1000000L * 100000L;
	static const uint64 P10 = 100000L * 100000L;
	static const uint64 P09 = 100000L * 10000L;
	static const uint64 P08 = 10000L * 10000L;
	static const uint64 P07 = 10000L * 1000L;
	static const uint64 P06 = 1000L * 1000L;
	uint32 digits10(uint64 v)
	{
		if (v < 10)
			return 1;
		if (v < 100)
			return 2;
		if (v < 1000)
			return 3;
		if (v < P12)
		{
			if (v < P08)
			{
				if (v < P06)
				{
					if (v < 10000)
						return 4;
					return 5 + (v >= 100000);
				}
				return 7 + (v >= P07);
			}
			if (v < P10)
			{
				return 9 + (v >= P09);
			}
			return 11 + (v >= P11);
		}
		return 12 + digits10(v / P12);
	}
}
