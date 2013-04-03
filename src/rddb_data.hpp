/*
 * rddb_data.hpp
 *
 *  Created on: 2013-4-3
 *      Author: wqy
 */

#ifndef RDDB_DATA_HPP_
#define RDDB_DATA_HPP_
#include <stdint.h>
#include <map>
#include <list>
#include <string>
#include "slice.hpp"
#include "util/buffer_helper.hpp"

namespace rddb
{
	enum KeyType
	{
		KV = 0,
		SET_META = 1,
		SET_ELEMENT = 2,
		ZSET_META = 3,
		ZSET_ELEMENT_SCORE = 4,
		ZSET_ELEMENT = 5,
		HASH_META = 6,
		HASH_FIELD = 7,
		LIST_META = 8,
		LIST_ELEMENT = 9,

	};

	struct KeyObject
	{
			KeyType type;
			Slice key;

			KeyObject(const Slice& k, KeyType T = KV) :
					type(T), key(k)
			{
			}
	};

	struct ZSetKeyObject: public KeyObject
	{
			Slice value;
			double score;
			ZSetKeyObject(const Slice& k, const Slice& v, double s) :
					KeyObject(k, SET_ELEMENT), value(v), score(s)
			{
			}
	};
	struct ZSetScoreKeyObject: public KeyObject
	{
			Slice value;
			ZSetScoreKeyObject(const Slice& k, const Slice& v) :
					KeyObject(k, ZSET_ELEMENT_SCORE), value(v)
			{
			}
	};

	struct ZSetMetaValue
	{
			uint32_t size;
			ZSetMetaValue() :
					size(0)
			{
			}
	};

	struct SetKeyObject: public KeyObject
	{
			Slice value;
			SetKeyObject(const Slice& k, const Slice& v) :
					KeyObject(k, SET_ELEMENT), value(v)
			{
			}
	};

	struct SetMetaValue
	{
			uint32_t size;
			SetMetaValue() :
					size(0)
			{
			}
	};

	struct HashKeyObject: public KeyObject
	{
			Slice field;
			HashKeyObject(const Slice& k, const Slice& f) :
					KeyObject(k, HASH_FIELD), field(f)
			{
			}
	};

	struct ListKeyObject: public KeyObject
	{
			int32_t score;
			ListKeyObject(const Slice& k, int32_t s) :
					KeyObject(k, LIST_ELEMENT), score(s)
			{
			}
	};

	struct ListMetaValue
	{
			uint32_t size;
			int32_t min_score;
			int32_t max_score;
			ListMetaValue() :
					size(0), min_score(0), max_score(0)
			{
			}
	};

    void encode_key(Buffer& buf, const KeyObject& key);
	KeyObject* decode_key(const Slice& key);

}

#endif /* RDDB_DATA_HPP_ */
