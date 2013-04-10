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
	inline int ardb_compare_keys(const char* akbuf, size_t aksiz,
	        const char* bkbuf, size_t bksiz)
	{
		Buffer ak_buf(const_cast<char*>(akbuf), 0, aksiz);
		Buffer bk_buf(const_cast<char*>(bkbuf), 0, bksiz);
		uint8_t at, bt;
		bool found_a = BufferHelper::ReadFixUInt8(ak_buf, at);
		bool found_b = BufferHelper::ReadFixUInt8(bk_buf, bt);
		COMPARE_EXIST(found_a, found_b);
		RETURN_NONEQ_RESULT(at, bt);
		int ret = 0;
		Slice akey, bkey;
		found_a = BufferHelper::ReadVarSlice(ak_buf, akey);
		found_b = BufferHelper::ReadVarSlice(bk_buf, bkey);
		COMPARE_EXIST(found_a, found_b);
		int ret = akey.compare(bkey);
		if (ret != 0)
		{
			return ret;
		}

		switch (at)
		{
			case ZSET_ELEMENT_SCORE:
			case SET_ELEMENT:
			case HASH_FIELD:
			{
				Slice af, bf;
				found_a = BufferHelper::ReadVarSlice(ak_buf, af);
				found_b = BufferHelper::ReadVarSlice(bk_buf, bf);
				COMPARE_EXIST(found_a, found_b);
				ret = af.compare(bf);
				break;
			}
			case LIST_ELEMENT:
			{
				double ascore, bscore;
				found_a = BufferHelper::ReadVarDouble(ak_buf, ascore);
				found_b = BufferHelper::ReadVarDouble(ak_buf, bscore);
				COMPARE_EXIST(found_a, found_b);
				ret = COMPARE_NUMBER(ascore, bscore);
				break;
			}
			case ZSET_ELEMENT:
			{
				double ascore, bscore;
				found_a = BufferHelper::ReadVarDouble(ak_buf, ascore);
				found_b = BufferHelper::ReadVarDouble(ak_buf, bscore);
				ret = COMPARE_NUMBER(ascore, bscore);
				if (ret == 0)
				{
					Slice af, bf;
					found_a = BufferHelper::ReadVarSlice(ak_buf, af);
					found_b = BufferHelper::ReadVarSlice(bk_buf, bf);
					COMPARE_EXIST(found_a, found_b);
					ret = af.compare(bf);
				}
				break;
			}
			case SET_META:
			case ZSET_META:
			case LIST_META:
			default:
			{
				break;
			}
		}
		return ret;
	}
}

#endif /* COMPARATOR_HPP_ */
