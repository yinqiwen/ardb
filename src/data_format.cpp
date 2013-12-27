/*
 * data_format.cpp
 *
 *  Created on: 2013Äê12ÔÂ24ÈÕ
 *      Author: wqy
 */
#include "data_format.hpp"

#define  GET_KEY_TYPE(KEY, TYPE)   do{ \
		Iterator* iter = FindValue(KEY, true);  \
		if (NULL != iter && iter->Valid()) \
		{                                  \
			Slice tmp = iter->Key();       \
			KeyObject* k = decode_key(tmp, &KEY); \
			if(NULL != k && k->key.compare(KEY.key) == 0)\
			{                                            \
				TYPE = k->type;                          \
	        }                                            \
	        DELETE(k);                                   \
	    }\
	    DELETE(iter);\
}while(0)

namespace ardb
{
	std::string ValueData::ToString(std::string& str) const
	{
		switch (type)
		{
			case INTEGER_VALUE:
			{
				char tmp[100];
				sprintf(tmp, "%lld", integer_value);
				str = tmp;
				break;
			}
			case DOUBLE_VALUE:
			{
				fast_dtoa(double_value, 10, str);
				break;
			}
			case BYTES_VALUE:
			{
				str = bytes_value;
				break;
			}
			default:
			{
				break;
			}
		}
		return str;
	}

	bool ValueData::Encode(Buffer& buf) const
	{
		BufferHelper::WriteFixUInt8(buf, type);
		switch (type)
		{
			case DOUBLE_VALUE:
			{
				BufferHelper::WriteFixDouble(buf, double_value);
				break;
			}
			case INTEGER_VALUE:
			{
				BufferHelper::WriteVarInt64(buf, integer_value);
				break;
			}
			case BYTES_VALUE:
			{
				BufferHelper::WriteVarString(buf, bytes_value);
				break;
			}
			default:
			{
				break;
			}
		}
		return true;
	}
	bool ValueData::Decode(Buffer& buf)
	{
		if (!BufferHelper::ReadFixUInt8(buf, type))
		{
			return false;
		}
		bool ret = false;
		switch (type)
		{
			case DOUBLE_VALUE:
			{
				ret = BufferHelper::ReadFixDouble(buf, double_value);
				break;
			}
			case INTEGER_VALUE:
			{
				ret = BufferHelper::ReadVarInt64(buf, integer_value);
				break;
			}
			case BYTES_VALUE:
			{
				ret = BufferHelper::ReadVarString(buf, bytes_value);
				break;
			}
			default:
			{
				break;
			}
		}
		return ret;
	}

	int ValueData::IncrbyFloat(double value)
	{
		if (type == BYTES_VALUE)
		{
			return -1;
		}
		if (type == EMPTY_VALUE || type == DOUBLE_VALUE)
		{
			type = DOUBLE_VALUE;
			double_value += value;
			return 0;
		}
		if (type == INTEGER_VALUE)
		{
			type = DOUBLE_VALUE;
			double_value = (integer_value + value);
			return 0;
		}
		return -1;
	}

	int ValueData::Incrby(int64 value)
	{
		if (type == BYTES_VALUE)
		{
			return -1;
		}
		if (type == EMPTY_VALUE || type == INTEGER_VALUE)
		{
			type = INTEGER_VALUE;
			integer_value += value;
			return 0;
		}
		if (type == DOUBLE_VALUE)
		{
			double_value += value;
			return 0;
		}
		return -1;
	}
	ValueData& ValueData::operator+=(const ValueData& other)
	{
		if (type == EMPTY_VALUE)
		{
			type = INTEGER_VALUE;
			integer_value = 0;
		}
		if (other.type == DOUBLE_VALUE || type == DOUBLE_VALUE)
		{
			double tmp = NumberValue();
			tmp += other.NumberValue();
			type = DOUBLE_VALUE;
			double_value = tmp;
			return *this;
		}
		if ((other.type == INTEGER_VALUE || other.type == EMPTY_VALUE)
		        && type == INTEGER_VALUE)
		{
			integer_value += other.integer_value;
			return *this;
		}
		return *this;
	}
	ValueData& ValueData::operator/=(uint32 i)
	{
		if (type == EMPTY_VALUE)
		{
			type = INTEGER_VALUE;
			integer_value = 0;
		}
		if (type == BYTES_VALUE)
		{
			return *this;
		}
		double tmp = NumberValue();
		type = DOUBLE_VALUE;
		if (i != 0)
		{
			double_value = tmp / i;
		} else
		{
			double_value = DBL_MAX;
		}
		return *this;
	}

	double ValueData::NumberValue() const
	{
		switch (type)
		{
			case INTEGER_VALUE:
			{
				return integer_value;
			}
			case DOUBLE_VALUE:
			{
				return double_value;
			}
			default:
			{
				return NAN;
			}
		}
	}

	int ValueData::ToBytes()
	{
		if (type == EMPTY_VALUE)
		{
			return -1;
		}
		ToString(bytes_value);
		return -1;
	}

	int ValueData::ToNumber()
	{
		if (type == EMPTY_VALUE)
		{
			return -1;
		}
		if (type != BYTES_VALUE)
		{
			return 0;
		}
		if (raw_toint64(bytes_value.data(), bytes_value.size(), integer_value))
		{
			bytes_value.clear();
			type = INTEGER_VALUE;
			return 1;
		} else if (raw_todouble(bytes_value.data(), bytes_value.size(),
		        double_value))
		{
			bytes_value.clear();
			type = DOUBLE_VALUE;
			return 1;
		}
		return -1;
	}

	void ValueData::SetValue(const Slice& value, bool auto_convert)
	{
		if (!auto_convert)
		{
			type = BYTES_VALUE;
			bytes_value.assign(value.data(), value.size());
		} else
		{
			if (value.empty())
			{
				type = EMPTY_VALUE;
				return;
			}
			char first_char = value.data()[0];
			if (first_char != '+' && value.data()[0] != '-'
			        && (first_char < '0' || first_char > '9'))
			{
				type = BYTES_VALUE;
				bytes_value.assign(value.data(), value.size());
			} else
			{
				if (raw_toint64(value.data(), value.size(), integer_value))
				{
					type = INTEGER_VALUE;
				} else if (raw_todouble(value.data(), value.size(),
				        double_value))
				{
					type = DOUBLE_VALUE;
				} else
				{
					type = BYTES_VALUE;
					bytes_value.assign(value.data(), value.size());
				}
			}
		}
	}

	int ValueData::Compare(const ValueData& other) const
	{
		if (type != other.type)
		{
			return COMPARE_NUMBER(type, other.type);
		}
		switch (type)
		{
			case EMPTY_VALUE:
			{
				return 0;
			}
			case INTEGER_VALUE:
			{
				return COMPARE_NUMBER(integer_value, other.integer_value);
			}
			case DOUBLE_VALUE:
			{
				return COMPARE_NUMBER(double_value, other.double_value);
			}
			default:
			{
				return bytes_value.compare(other.bytes_value);
			}
		}
	}

	void ValueData::Clear()
	{
		integer_value = 0;
		double_value = 0;
		bytes_value.clear();
		type = EMPTY_VALUE;
	}

	ZSetKeyObject::ZSetKeyObject(const Slice& k, const ValueData& v, double s,
	        DBID id) :
			KeyObject(k, ZSET_ELEMENT, id)
	{
		e.score = s;
		e.value = v;
	}

	ZSetKeyObject::ZSetKeyObject(const Slice& k, const Slice& v, double s,
	        DBID id) :
			KeyObject(k, ZSET_ELEMENT, id)
	{
		e.score = s;
		e.value.SetValue(v, true);
	}

	ZSetScoreKeyObject::ZSetScoreKeyObject(const Slice& k, const ValueData& v,
	        DBID id) :
			KeyObject(k, ZSET_ELEMENT_SCORE, id), value(v)
	{

	}

	ZSetScoreKeyObject::ZSetScoreKeyObject(const Slice& k, const Slice& v,
	        DBID id) :
			KeyObject(k, ZSET_ELEMENT_SCORE, id)
	{
		value.SetValue(v, true);
	}

	int ardb_compare_keys(const char* akbuf, size_t aksiz, const char* bkbuf,
	        size_t bksiz)
	{
		Buffer ak_buf(const_cast<char*>(akbuf), 0, aksiz);
		Buffer bk_buf(const_cast<char*>(bkbuf), 0, bksiz);
		uint32 aheader, bheader;
		bool found_a = BufferHelper::ReadFixUInt32(ak_buf, aheader);
		bool found_b = BufferHelper::ReadFixUInt32(bk_buf, bheader);
		COMPARE_EXIST(found_a, found_b);
		uint32 adb = aheader >> 8;
		uint32 bdb = bheader >> 8;
		RETURN_NONEQ_RESULT(adb, bdb);
		uint8 at = aheader & 0xFF;
		uint8 bt = bheader & 0xFF;
		RETURN_NONEQ_RESULT(at, bt);
		uint32 akeysize, bkeysize;
		found_a = BufferHelper::ReadVarUInt32(ak_buf, akeysize);
		found_b = BufferHelper::ReadVarUInt32(bk_buf, bkeysize);
		COMPARE_EXIST(found_a, found_b);
		RETURN_NONEQ_RESULT(akeysize, bkeysize);

		Slice akey(ak_buf.GetRawReadBuffer(), akeysize);
		Slice bkey(bk_buf.GetRawReadBuffer(), bkeysize);
		found_a = ak_buf.ReadableBytes() >= akeysize;
		found_b = bk_buf.ReadableBytes() >= bkeysize;
		COMPARE_EXIST(found_a, found_b);
		ak_buf.SkipBytes(akeysize);
		bk_buf.SkipBytes(bkeysize);
		int ret = 0;
		if (at != KEY_EXPIRATION_ELEMENT)
		{
			ret = COMPARE_SLICE(akey, bkey);
		}
		if (ret != 0)
		{
			return ret;
		}
		switch (at)
		{
			case HASH_FIELD:
			{
				Slice af, bf;
				found_a = BufferHelper::ReadVarSlice(ak_buf, af);
				found_b = BufferHelper::ReadVarSlice(bk_buf, bf);
				COMPARE_EXIST(found_a, found_b);
				RETURN_NONEQ_RESULT(af.size(), bf.size());
				ret = COMPARE_SLICE(af, bf);
				break;
			}
			case LIST_ELEMENT:
			{
				float ascore, bscore;
				found_a = BufferHelper::ReadFixFloat(ak_buf, ascore);
				found_b = BufferHelper::ReadFixFloat(bk_buf, bscore);
				COMPARE_EXIST(found_a, found_b);
				ret = COMPARE_NUMBER(ascore, bscore);
				break;
			}
			case ZSET_ELEMENT_SCORE:
			case SET_ELEMENT:
			{
				ValueData av, bv;
				found_a = decode_value(ak_buf, av);
				found_b = decode_value(bk_buf, bv);
				COMPARE_EXIST(found_a, found_b);
				ret = av.Compare(bv);
				break;
			}
			case ZSET_ELEMENT:
			{
				double ascore, bscore;
				found_a = BufferHelper::ReadFixDouble(ak_buf, ascore);
				found_b = BufferHelper::ReadFixDouble(bk_buf, bscore);
				ret = COMPARE_NUMBER(ascore, bscore);
				if (ret == 0)
				{
					ValueData av, bv;
					found_a = decode_value(ak_buf, av);
					found_b = decode_value(bk_buf, bv);
					COMPARE_EXIST(found_a, found_b);
					ret = av.Compare(bv);
				}
				break;
			}
			case KEY_EXPIRATION_ELEMENT:
			{
				uint64 aexpire, bexpire;
				found_a = BufferHelper::ReadVarUInt64(ak_buf, aexpire);
				found_b = BufferHelper::ReadVarUInt64(bk_buf, bexpire);
				COMPARE_EXIST(found_a, found_b);
				ret = COMPARE_NUMBER(aexpire, bexpire);
				if (ret == 0)
				{
					ret = COMPARE_SLICE(akey, bkey);
				}
				break;
			}
			case BITSET_ELEMENT:
			{
				uint64 aindex, bindex;
				found_a = BufferHelper::ReadVarUInt64(ak_buf, aindex);
				found_b = BufferHelper::ReadVarUInt64(bk_buf, bindex);
				COMPARE_EXIST(found_a, found_b);
				ret = COMPARE_NUMBER(aindex, bindex);
				break;
			}
			case SET_META:
			case ZSET_META:
			case LIST_META:
			case BITSET_META:
			case SCRIPT:
			default:
			{
				break;
			}
		}
		return ret;
	}

	void encode_key(Buffer& buf, const KeyObject& key)
	{
		uint32 header = (uint32) (key.db << 8) + key.type;
		BufferHelper::WriteFixUInt32(buf, header);
		BufferHelper::WriteVarSlice(buf, key.key);
		switch (key.type)
		{
			case HASH_FIELD:
			{
				const HashKeyObject& hk = (const HashKeyObject&) key;
				encode_value(buf, hk.field);
				break;
			}
			case LIST_ELEMENT:
			{
				const ListKeyObject& lk = (const ListKeyObject&) key;
				BufferHelper::WriteFixFloat(buf, lk.score);
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
				BufferHelper::WriteFixDouble(buf, sk.e.score);
				encode_value(buf, sk.e.value);
				break;
			}
			case ZSET_ELEMENT_SCORE:
			{
				const ZSetScoreKeyObject& zk = (const ZSetScoreKeyObject&) key;
				encode_value(buf, zk.value);
				break;
			}
			case BITSET_ELEMENT:
			{
				const BitSetKeyObject& bk = (const BitSetKeyObject&) key;
				BufferHelper::WriteVarUInt64(buf, bk.index);
				break;
			}
			case KEY_EXPIRATION_ELEMENT:
			{
				const ExpireKeyObject& bk = (const ExpireKeyObject&) key;
				BufferHelper::WriteVarUInt64(buf, bk.expireat);
				break;
			}
			case LIST_META:
			case ZSET_META:
			case SET_META:
			case BITSET_META:
			case SCRIPT:
			default:
			{
				break;
			}
		}
	}

	bool peek_dbkey_header(const Slice& key, DBID& db, KeyType& type)
	{
		Buffer buf(const_cast<char*>(key.data()), 0, key.size());
		uint32 header;
		if (!BufferHelper::ReadFixUInt32(buf, header))
		{
			return false;
		}
		type = (KeyType) (header & 0xFF);
		db = header >> 8;
		return true;
	}

	KeyObject* decode_key(const Slice& key, KeyObject* expected)
	{
		Buffer buf(const_cast<char*>(key.data()), 0, key.size());
		uint32 header;
		if (!BufferHelper::ReadFixUInt32(buf, header))
		{
			return NULL;
		}
		uint8 type = header & 0xFF;
		uint32 db = header >> 8;
		if (NULL != expected)
		{
			if (type != expected->type || db != expected->db)
			{
				return NULL;
			}
		}
		Slice keystr;
		if (!BufferHelper::ReadVarSlice(buf, keystr))
		{
			return NULL;
		}
		if (NULL != expected)
		{
			if (keystr != expected->key)
			{
				return NULL;
			}
		}
		switch (type)
		{
			case HASH_FIELD:
			{
				HashKeyObject* hk = new HashKeyObject(keystr, Slice(), db);
				if (!decode_value(buf, hk->field))
				{
					DELETE(hk);
					return NULL;
				}
				return hk;
			}
			case LIST_ELEMENT:
			{
				float score;
				if (!BufferHelper::ReadFixFloat(buf, score))
				{
					return NULL;
				}
				return new ListKeyObject(keystr, score, db);
			}

			case SET_ELEMENT:
			{
				SetKeyObject* sk = new SetKeyObject(keystr, Slice(), db);
				if (!decode_value(buf, sk->value))
				{
					DELETE(sk);
					return NULL;
				}
				return sk;
			}
			case ZSET_ELEMENT:
			{
				ZSetKeyObject* zsk = new ZSetKeyObject(keystr, Slice(), 0, db);
				double score;
				if (!BufferHelper::ReadFixDouble(buf, score)
				        || !decode_value(buf, zsk->e.value))
				{
					DELETE(zsk);
					return NULL;
				}
				zsk->e.score = score;
				return zsk;
			}
			case ZSET_ELEMENT_SCORE:
			{
				ZSetScoreKeyObject* zsk = new ZSetScoreKeyObject(keystr,
				        Slice(), db);
				if (!decode_value(buf, zsk->value))
				{
					DELETE(zsk);
					return NULL;
				}
				return zsk;
			}
			case BITSET_ELEMENT:
			{
				uint64 index;
				if (!BufferHelper::ReadVarUInt64(buf, index))
				{
					return NULL;
				}
				return new BitSetKeyObject(keystr, index, db);
			}
			case KEY_EXPIRATION_ELEMENT:
			{
				uint64 ts;
				if (!BufferHelper::ReadVarUInt64(buf, ts))
				{
					return NULL;
				}
				return new ExpireKeyObject(keystr, ts, db);
			}
			case SET_META:
			case ZSET_META:
			case LIST_META:
			case BITSET_META:
			case KEY_META:
			case SCRIPT:
			default:
			{
				return new KeyObject(keystr, (KeyType) type, db);
			}
		}
	}

	void encode_meta(Buffer& buf, CommonMetaValue& meta)
	{
		BufferHelper::WriteFixUInt8(buf, meta.header.version);
		BufferHelper::WriteFixUInt8(buf, meta.header.type);
		BufferHelper::WriteVarUInt64(buf, meta.header.expireat);
		switch (meta.header.type)
		{
			case STRING_META:
			{
				StringMetaValue& bmeta = (StringMetaValue&) meta;
				bmeta.Encode(buf);
				break;
			}
			case BITSET_META:
			{
				BitSetMetaValue& bmeta = (BitSetMetaValue&) meta;
				bmeta.Encode(buf);
				break;
			}
			case HASH_META:
			{
				HashMetaValue & bmeta = (HashMetaValue&) meta;
				bmeta.Encode(buf);
				break;
			}
			case LIST_META:
			{
				ListMetaValue & bmeta = (ListMetaValue&) meta;
				bmeta.Encode(buf);
				break;
			}
			case ZSET_META:
			{
				ZSetMetaValue & bmeta = (ZSetMetaValue&) meta;
				bmeta.Encode(buf);
				break;
			}
			case SET_META:
			{
				SetMetaValue & bmeta = (SetMetaValue&) meta;
				bmeta.Encode(buf);
				break;
			}
			default:
			{
				ERROR_LOG("Unsupported meta type:%d", meta.header.type);
				break;
			}
		}
	}

	CommonMetaValue* decode_meta(const char* data, size_t size, bool only_head)
	{
		MetaValueHeader header;
		Buffer msgbuf(const_cast<char*>(data), 0, size);
		uint8 type;
		if (BufferHelper::ReadFixUInt8(msgbuf, header.version)
		        && BufferHelper::ReadFixUInt8(msgbuf, type)
		        && BufferHelper::ReadVarUInt64(msgbuf, header.expireat))
		{
			header.type = (KeyType) type;
			if (only_head)
			{
				CommonMetaValue* meta = NULL;
				NEW(meta, CommonMetaValue);
				meta->header = header;
				return meta;
			}
		} else
		{
			return NULL;
		}
//		msgpack::unpacked msg;
//		size_t offset = msgbuf.GetReadIndex();
//		msgpack::unpack(&msg, data, size, &offset);
//		msgpack::object obj = msg.get();
		CommonMetaValue* meta = NULL;
		switch (header.type)
		{
			case STRING_META:
			{
				StringMetaValue * bmeta = new StringMetaValue;
				bmeta->Decode(msgbuf);
				meta = bmeta;
				break;
			}
			case BITSET_META:
			{
				BitSetMetaValue* bmeta = new BitSetMetaValue;
				bmeta->Decode(msgbuf);
				meta = bmeta;
				break;
			}
			case HASH_META:
			{
				HashMetaValue* bmeta = new HashMetaValue;
				bmeta->Decode(msgbuf);
				meta = bmeta;
				break;
			}
			case LIST_META:
			{
				ListMetaValue* bmeta = new ListMetaValue;
				bmeta->Decode(msgbuf);
				meta = bmeta;
				break;
			}
			case ZSET_META:
			{
				ZSetMetaValue* bmeta = new ZSetMetaValue;
				bmeta->Decode(msgbuf);
				meta = bmeta;
				break;
			}
			case SET_META:
			{
				SetMetaValue* smeta = new SetMetaValue;
				smeta->Decode(msgbuf);
				meta = smeta;
				break;
			}
			default:
			{
				ERROR_LOG("Unsupported meta type:%d", header.type);
				return NULL;
			}
		}
		if (NULL != meta)
		{
			meta->header = header;
		}
		return meta;
	}

	ValueObject* decode_value_obj(KeyType type, const char* data, size_t size)
	{
		Buffer buffer(const_cast<char*>(data), 0, size);
		switch (type)
		{
			case KEY_META:
			{
				return decode_meta(data, size, false);
			}

			case HASH_FIELD:
			case LIST_ELEMENT:
			case ZSET_ELEMENT_SCORE:
			case SCRIPT:
			{
				CommonValueObject* obj = new CommonValueObject;
				obj->Decode(buffer);
				return obj;
			}
			case BITSET_ELEMENT:
			{
				BitSetElementValue* obj = new BitSetElementValue;
				obj->Decode(buffer);
				return obj;
			}
			case ZSET_ELEMENT:
			case KEY_EXPIRATION_ELEMENT:
			case SET_ELEMENT:
			default:
			{
				return new EmptyValueObject;
			}
		}
	}

	void encode_value(Buffer& buf, const ValueData& value)
	{
		value.Encode(buf);
	}

	bool decode_value(Buffer& buf, ValueData& value)
	{
		return value.Decode(buf);
	}

	void next_key(const Slice& key, std::string& next)
	{
		next.assign(key.data(), key.size());
		for (uint32 i = next.size(); i > 0; i--)
		{
			if (next[i - 1] < 0x7F)
			{
				next[i - 1] = next[i - 1] + 1;
				return;
			}
		}
		next.append("\0", 1);
	}

	SetKeyObject::SetKeyObject(const Slice& k, const ValueData& v, DBID id) :
			KeyObject(k, SET_ELEMENT, id), value(v)
	{

	}
	SetKeyObject::SetKeyObject(const Slice& k, const Slice& v, DBID id) :
			KeyObject(k, SET_ELEMENT, id)
	{
		value.SetValue(v, true);
	}

}

