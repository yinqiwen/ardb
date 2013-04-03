/*
 * rddb_data.cpp
 *
 *  Created on: 2013-4-3
 *      Author: wqy
 */
#include "rddb_data.hpp"

namespace rddb
{
	void encode_key(Buffer& buf, const KeyObject& key)
	{
		BufferHelper::WriteFixUInt8(buf, key.type);
		BufferHelper::WriteVarSlice(buf, key.key);
		switch (key.type)
		{
			case HASH_FIELD:
			{
				const HashKeyObject& hk = (const HashKeyObject&) key;
				BufferHelper::WriteVarSlice(buf, hk.field);
				break;
			}
			case LIST_ELEMENT:
			{
				const ListKeyObject& lk = (const ListKeyObject&) key;
				BufferHelper::WriteVarInt32(buf, lk.score);
				break;
			}
			case SET_ELEMENT:
			{
				const SetKeyObject& sk = (const SetKeyObject&) key;
				BufferHelper::WriteVarSlice(buf, sk.value);
				break;
			}
			case ZSET_ELEMENT:
			{
				const ZSetKeyObject& sk = (const ZSetKeyObject&) key;
				BufferHelper::WriteVarSlice(buf, sk.value);
				BufferHelper::WriteVarDouble(buf, sk.score);
				break;
			}
			case ZSET_ELEMENT_SCORE:
			{
				const ZSetScoreKeyObject& zk = (const ZSetScoreKeyObject&) key;
				BufferHelper::WriteVarSlice(buf, zk.value);
				break;
			}
			case LIST_META:
			case ZSET_META:
			case SET_META:
			default:
			{
				break;
			}
		}
	}

	KeyObject* decode_key(const Slice& key)
	{
		Buffer buf(const_cast<char*>(key.data()), 0, key.size());
		uint8_t type;
		if (!BufferHelper::ReadFixUInt8(buf, type))
		{
			return NULL;
		}
		Slice keystr;
		if (!BufferHelper::ReadVarSlice(buf, keystr))
		{
			return NULL;
		}
		switch (type)
		{
			case HASH_FIELD:
			{
				Slice field;
				if (!BufferHelper::ReadVarSlice(buf, field))
				{
					return NULL;
				}
				return new HashKeyObject(keystr, field);
			}
			case LIST_ELEMENT:
			{
				int32_t score;
				if (!BufferHelper::ReadVarInt32(buf, score))
				{
					return NULL;
				}
				return new ListKeyObject(keystr, score);
			}

			case SET_ELEMENT:
			{
				Slice value;
				if (!BufferHelper::ReadVarSlice(buf, value))
				{
					return NULL;
				}
				return new SetKeyObject(keystr, value);
			}
			case ZSET_ELEMENT:
			{
				Slice value;
				double score;
				if (!BufferHelper::ReadVarSlice(buf, value)
						|| !BufferHelper::ReadVarDouble(buf, score))
				{
					return NULL;
				}
				return new ZSetKeyObject(keystr, value, score);
			}
			case ZSET_ELEMENT_SCORE:
			{
				Slice value;
				if (!BufferHelper::ReadVarSlice(buf, value))
				{
					return NULL;
				}
				return new ZSetScoreKeyObject(keystr, value);
			}
			case SET_META:
			case ZSET_META:
			case LIST_META:
			default:
			{
				return new KeyObject(keystr, (KeyType) type);
			}
		}
	}
}

