/*
 * comparator.hpp
 *
 *  Created on: 2013-4-10
 *      Author: wqy
 */

#ifndef COMPARATOR_HPP_
#define COMPARATOR_HPP_

#include "ardb.hpp"

#define COMPARE_NUMBER(a, b)  (a == b?0:(a>b?1:-1))

namespace ardb
{
	inline int ardb_compare_keys(const char* akbuf, size_t aksiz,
			const char* bkbuf, size_t bksiz)
	{
		Slice a(akbuf, aksiz);
		Slice b(bkbuf, bksiz);
		KeyType at, bt;
		if (peek_key_type(a, at) && peek_key_type(b, bt))
		{
			if (at != bt)
			{
				return COMPARE_NUMBER(at, bt);
			}
		}

		KeyObject* ak = decode_key(a);
		KeyObject* bk = decode_key(b);
		if (NULL == ak && NULL == bk)
		{
			return 0;
		}
		if (NULL != ak && NULL == bk)
		{
			DELETE(ak);
			return 1;
		}
		if (NULL == ak && NULL != bk)
		{
			DELETE(bk);
			return -1;
		}
		if(ak->type != bk->type){
			return COMPARE_NUMBER(ak->type, bk->type);
		}
		int ret = 0;
		ret = ak->key.compare(bk->key);
		if (ret != 0)
		{
			DELETE(ak);
			DELETE(bk);
			return ret;
		}
		switch (ak->type)
		{
			case HASH_FIELD:
			{
				HashKeyObject* hak = (HashKeyObject*) ak;
				HashKeyObject* hbk = (HashKeyObject*) bk;
				ret = hak->field.compare(hbk->field);
				break;
			}
			case LIST_ELEMENT:
			{
				ListKeyObject* lak = (ListKeyObject*) ak;
				ListKeyObject* lbk = (ListKeyObject*) bk;
				ret = COMPARE_NUMBER(lak->score, lbk->score);
				break;
			}
			case SET_ELEMENT:
			{
				SetKeyObject* sak = (SetKeyObject*) ak;
				SetKeyObject* sbk = (SetKeyObject*) bk;
				ret = sak->value.compare(sbk->value);
				break;
			}
			case ZSET_ELEMENT:
			{
				ZSetKeyObject* zak = (ZSetKeyObject*) ak;
				ZSetKeyObject* zbk = (ZSetKeyObject*) bk;
				ret = COMPARE_NUMBER(zak->score, zbk->score);
				if (ret == 0)
				{
					ret = zak->value.compare(zbk->value);
				}
				break;
			}
			case ZSET_ELEMENT_SCORE:
			{
				ZSetScoreKeyObject* zak = (ZSetScoreKeyObject*) ak;
				ZSetScoreKeyObject* zbk = (ZSetScoreKeyObject*) bk;
				ret = zak->value.compare(zbk->value);
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
		DELETE(ak);
		DELETE(bk);
		return ret;
	}
}

#endif /* COMPARATOR_HPP_ */
