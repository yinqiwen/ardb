/*
 * ardb_data.cpp
 *
 *  Created on: 2013-4-3
 *      Author: wqy
 */
#include "ardb_data.hpp"
#include "util/helpers.hpp"

namespace ardb
{
	SetKeyObject::SetKeyObject(const Slice& k, const ValueObject& v) :
			KeyObject(k, SET_ELEMENT), value(v)
	{

	}
	SetKeyObject::SetKeyObject(const Slice& k, const Slice& v) :
			KeyObject(k, SET_ELEMENT)
	{
		smart_fill_value(v, value);
	}

	ZSetKeyObject::ZSetKeyObject(const Slice& k, const ValueObject& v, double s) :
			KeyObject(k, ZSET_ELEMENT), value(v), score(s)
	{

	}

	ZSetKeyObject::ZSetKeyObject(const Slice& k, const Slice& v, double s) :
			KeyObject(k, ZSET_ELEMENT), score(s)
	{
		smart_fill_value(v, value);
	}

	ZSetScoreKeyObject::ZSetScoreKeyObject(const Slice& k, const ValueObject& v) :
			KeyObject(k, ZSET_ELEMENT_SCORE), value(v)
	{

	}

	ZSetScoreKeyObject::ZSetScoreKeyObject(const Slice& k, const Slice& v) :
			KeyObject(k, ZSET_ELEMENT_SCORE)
	{
		smart_fill_value(v, value);
	}

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
				BufferHelper::WriteVarDouble(buf, lk.score);
				break;
			}
			case SET_ELEMENT:
			{
				const SetKeyObject& sk = (const SetKeyObject&) key;
				encode_value(buf, sk.value);
				break;
			}
			case ZSET_ELEMENT:
			{
				const ZSetKeyObject& sk = (const ZSetKeyObject&) key;
				BufferHelper::WriteVarDouble(buf, sk.score);
				encode_value(buf, sk.value);
				break;
			}
			case ZSET_ELEMENT_SCORE:
			{
				const ZSetScoreKeyObject& zk = (const ZSetScoreKeyObject&) key;
				encode_value(buf, zk.value);
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

	bool peek_key_type(const Slice& key, KeyType& type)
	{
		Buffer buf(const_cast<char*>(key.data()), 0, key.size());
		uint8_t t;
		if (!BufferHelper::ReadFixUInt8(buf, t))
		{
			return false;
		}
		type = (KeyType) t;
		return true;
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
				double score;
				if (!BufferHelper::ReadVarDouble(buf, score))
				{
					return NULL;
				}
				return new ListKeyObject(keystr, score);
			}

			case SET_ELEMENT:
			{
				SetKeyObject* sk = new SetKeyObject(keystr, Slice());
				if (!decode_value(buf, sk->value))
				{
					DELETE(sk);
					return NULL;
				}
				return sk;
			}
			case ZSET_ELEMENT:
			{
				ZSetKeyObject* zsk = new ZSetKeyObject(keystr, Slice(), 0);
				double score;
				if (!decode_value(buf, zsk->value)
				        || !BufferHelper::ReadVarDouble(buf, score))
				{
					DELETE(zsk);
					return NULL;
				}
				zsk->score = score;
				return zsk;
			}
			case ZSET_ELEMENT_SCORE:
			{
				ZSetScoreKeyObject* zsk = new ZSetScoreKeyObject(keystr,
				        Slice());
				if (!decode_value(buf, zsk->value))
				{
					DELETE(zsk);
					return NULL;
				}
				return zsk;
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

	int value_convert_to_raw(ValueObject& v)
	{
		if (v.type != RAW)
		{
			int64_t iv = v.v.int_v;
			double dv = v.v.double_v;
			v.type = RAW;
			v.v.raw = new Buffer(16);
			if (v.type == INTEGER)
			{
				v.v.raw->Printf("%lld", iv);
			}
			else if (v.type == DOUBLE)
			{
				double min = -4503599627370495; /* (2^52)-1 */
				double max = 4503599627370496; /* -(2^52) */
				iv = (int64_t) dv;
				if (dv > min && dv < max && dv == ((double) iv))
				{
					v.v.raw->Printf("%lld", iv);
				}
				else
				{
					v.v.raw->Printf("%.17g", dv);
				}
			}
			return 0;
		}
		return -1;
	}

	int value_convert_to_number(ValueObject& v)
	{
		if (v.type != RAW)
		{
			return 0;
		}
		int64_t intv;
		double dv;
		if (raw_toint64(v.v.raw->GetRawReadBuffer(), v.v.raw->ReadableBytes(),
		        intv))
		{
			v.Clear();
			v.type = INTEGER;
			v.v.int_v = intv;
			return 1;
		}
		else if (raw_todouble(v.v.raw->GetRawReadBuffer(),
		        v.v.raw->ReadableBytes(), dv))
		{
			v.Clear();
			v.type = DOUBLE;
			v.v.double_v = dv;
			return 1;
		}
		return -1;
	}

	void encode_value(Buffer& buf, const ValueObject& value)
	{
		BufferHelper::WriteFixUInt8(buf, value.type);
		switch (value.type)
		{
			case EMPTY:
			{
				break;
			}
			case INTEGER:
			{
				BufferHelper::WriteVarInt64(buf, value.v.int_v);
				break;
			}
			case DOUBLE:
			{
				BufferHelper::WriteVarDouble(buf, value.v.double_v);
				break;
			}
			default:
			{
				if (NULL != value.v.raw)
				{
					BufferHelper::WriteVarUInt32(buf,
					        value.v.raw->ReadableBytes());
					buf.Write(value.v.raw, value.v.raw->ReadableBytes());
				}
				else
				{
					BufferHelper::WriteVarInt64(buf, 0);
				}
				break;
			}
		}
		if (value.expire > 0)
		{
			BufferHelper::WriteVarUInt64(buf, value.expire);
		}
	}

	bool decode_value(Buffer& buf, ValueObject& value)
	{
		if (!BufferHelper::ReadFixUInt8(buf, value.type))
		{
			return false;
		}
		switch (value.type)
		{
			case EMPTY:
			{
				break;
			}
			case INTEGER:
			{
				if (!BufferHelper::ReadVarInt64(buf, value.v.int_v))
				{
					return false;
				}
				break;
			}
			case DOUBLE:
			{
				if (!BufferHelper::ReadVarDouble(buf, value.v.double_v))
				{
					return false;
				}
				break;
			}
			default:
			{
				uint32_t len;
				if (!BufferHelper::ReadVarUInt32(buf, len)
				        || buf.ReadableBytes() < len)
				{
					return false;
				}

				value.v.raw = new Buffer(len);
				buf.Read(value.v.raw, len);
				break;
			}
		}
		value.expire = 0;
		BufferHelper::ReadVarUInt64(buf, value.expire);
		return true;
	}
	void smart_fill_value(const Slice& value, ValueObject& valueobject)
	{
		if (value.empty())
		{
			valueobject.type = EMPTY;
			return;
		}
		int64_t intv;
		double doublev;
		if (raw_toint64(value.data(), value.size(), intv))
		{
			valueobject.type = INTEGER;
			valueobject.v.int_v = intv;
		}
		else if (raw_todouble(value.data(), value.size(), doublev))
		{
			valueobject.type = DOUBLE;
			valueobject.v.double_v = doublev;
		}
		else
		{
			valueobject.type = RAW;
			char* v = const_cast<char*>(value.data());
			valueobject.v.raw = new Buffer(v, 0, value.size());
		}
	}
	void fill_raw_value(const Slice& value, ValueObject& valueobject)
	{
		valueobject.type = RAW;
		char* v = const_cast<char*>(value.data());
		valueobject.v.raw = new Buffer(v, 0, value.size());
	}

}

