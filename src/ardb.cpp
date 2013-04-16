/*
 * ardb.cpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string.h>
#include "comparator.hpp"

#define  GET_KEY_TYPE(DB, KEY, TYPE)   do{ \
		Iterator* iter = FindValue(DB, KEY);  \
		if (NULL != iter && iter->Valid()) \
		{                                  \
			Slice tmp = iter->Key();       \
			KeyObject* k = decode_key(tmp); \
			if(NULL != k && k->key.compare(KEY.key) == 0)\
			{                                            \
				TYPE = k->type;                          \
	         }                                           \
	    }\
	    DELETE(iter);\
}while(0)

namespace ardb
{
	int ardb_compare_keys(const char* akbuf, size_t aksiz, const char* bkbuf,
	        size_t bksiz)
	{
		Buffer ak_buf(const_cast<char*>(akbuf), 0, aksiz);
		Buffer bk_buf(const_cast<char*>(bkbuf), 0, bksiz);
		uint8_t at, bt;
		bool found_a = BufferHelper::ReadFixUInt8(ak_buf, at);
		bool found_b = BufferHelper::ReadFixUInt8(bk_buf, bt);
		COMPARE_EXIST(found_a, found_b);
		RETURN_NONEQ_RESULT(at, bt);
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
				found_b = BufferHelper::ReadVarDouble(bk_buf, bscore);
				COMPARE_EXIST(found_a, found_b);
				ret = COMPARE_NUMBER(ascore, bscore);
				break;
			}
			case ZSET_ELEMENT_SCORE:
			case SET_ELEMENT:
			{
				ValueObject av, bv;
				found_a = decode_value(ak_buf, av, false);
				found_b = decode_value(bk_buf, bv, false);
				COMPARE_EXIST(found_a, found_b);
				//DEBUG_LOG("#####A=%s b=%s", av.ToString().c_str(), bv.ToString().c_str());
				ret = av.Compare(bv);
				break;
			}
			case ZSET_ELEMENT:
			{
				double ascore, bscore;
				found_a = BufferHelper::ReadVarDouble(ak_buf, ascore);
				found_b = BufferHelper::ReadVarDouble(bk_buf, bscore);
				ret = COMPARE_NUMBER(ascore, bscore);
				if (ret == 0)
				{
					ValueObject av, bv;
					found_a = decode_value(ak_buf, av);
					found_b = decode_value(bk_buf, bv);
					COMPARE_EXIST(found_a, found_b);
					ret = av.Compare(bv);
				}
				break;
			}
			case TABLE_INDEX:
			{
				Slice af, bf;
				found_a = BufferHelper::ReadVarSlice(ak_buf, af);
				found_b = BufferHelper::ReadVarSlice(bk_buf, bf);
				COMPARE_EXIST(found_a, found_b);
				ret = af.compare(bf);
				if (ret == 0)
				{
					ValueObject kav, kbv;
					found_a = decode_value(ak_buf, kav);
					found_b = decode_value(bk_buf, kbv);
					COMPARE_EXIST(found_a, found_b);
					ret = kav.Compare(kbv);
					if(ret != 0)
					{
						break;
					}
					uint32 alen, blen;
					found_a = BufferHelper::ReadVarUInt32(ak_buf, alen);
					found_b = BufferHelper::ReadVarUInt32(bk_buf, blen);
					COMPARE_EXIST(found_a, found_b);
					ret = COMPARE_NUMBER(alen, blen);
					if (ret == 0)
					{
						for (uint32 i = 0; i < alen; i++)
						{
							ValueObject av, bv;
							found_a = decode_value(ak_buf, av);
							found_b = decode_value(bk_buf, bv);
							COMPARE_EXIST(found_a, found_b);
							ret = av.Compare(bv);
							if (ret != 0)
							{
								break;
							}
						}
					}
				}
				break;
			}
			case TABLE_COL:
			{
				Slice af, bf;
				found_a = BufferHelper::ReadVarSlice(ak_buf, af);
				found_b = BufferHelper::ReadVarSlice(bk_buf, bf);
				COMPARE_EXIST(found_a, found_b);
				ret = af.compare(bf);
				if (ret == 0)
				{
					uint32 alen, blen;
					found_a = BufferHelper::ReadVarUInt32(ak_buf, alen);
					found_b = BufferHelper::ReadVarUInt32(bk_buf, blen);
					COMPARE_EXIST(found_a, found_b);
					ret = COMPARE_NUMBER(alen, blen);
					if (ret == 0)
					{
						for (uint32 i = 0; i < alen; i++)
						{
							ValueObject av, bv;
							found_a = decode_value(ak_buf, av);
							found_b = decode_value(bk_buf, bv);
							COMPARE_EXIST(found_a, found_b);
							ret = av.Compare(bv);
							if (ret != 0)
							{
								break;
							}
						}
					}
				}
				break;
			}
			case SET_META:
			case ZSET_META:
			case LIST_META:
			case TABLE_META:
			default:
			{
				break;
			}
		}
		return ret;
	}

	size_t Ardb::RealPosition(Buffer* buf, int pos)
	{
		if (pos < 0)
		{
			pos = buf->ReadableBytes() + pos;
		}
		if (pos >= buf->ReadableBytes())
		{
			pos = buf->ReadableBytes() - 1;
		}
		if (pos < 0)
		{
			pos = 0;
		}
		return pos;
	}

	Ardb::Ardb(KeyValueEngineFactory* engine) :
			m_engine_factory(engine)
	{
	}

	void Ardb::Walk(const DBID& db, KeyObject& key, bool reverse,
	        WalkHandler* handler)
	{
		bool isFirstElement = true;
		Iterator* iter = FindValue(db, key);
		if(NULL != iter && !iter->Valid() && reverse){
			iter->SeekToLast();
			isFirstElement = false;
		}
		uint32 cursor = 0;
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != key.type
			        || kk->key.compare(key.key) != 0)
			{
				DELETE(kk);
				if (reverse && isFirstElement)
				{
					iter->Prev();
					isFirstElement = false;
					continue;
				}
				break;
			}
			ValueObject v;
			Buffer readbuf(const_cast<char*>(iter->Value().data()), 0,
			        iter->Value().size());
			decode_value(readbuf, v, false);
			int ret = handler->OnKeyValue(kk, &v, cursor++);
			DELETE(kk);
			if (ret < 0)
			{
				break;
			}
			if (reverse)
			{
				iter->Prev();
			}
			else
			{
				iter->Next();
			}
		}
		DELETE(iter);
	}

	KeyValueEngine* Ardb::GetDB(const DBID& db)
	{
		KeyValueEngineTable::iterator found = m_engine_table.find(db);
		if (found != m_engine_table.end())
		{
			return found->second;
		}
		KeyValueEngine* engine = m_engine_factory->CreateDB(db);
		if (NULL != engine)
		{
			m_engine_table[db] = engine;
		}
		return engine;
	}

	int Ardb::Type(const DBID& db, const Slice& key)
	{
		if (Exists(db, key))
		{
			return KV;
		}

		int type = -1;
		Slice empty;
		SetKeyObject sk(key, empty);
		GET_KEY_TYPE(db, sk, type);
		if (type < 0)
		{
			ZSetScoreKeyObject zk(key, empty);
			GET_KEY_TYPE(db, zk, type);
			if (type < 0)
			{
				HashKeyObject hk(key, empty);
				GET_KEY_TYPE(db, hk, type);
				if (type < 0)
				{
					KeyObject lk(key, LIST_META);
					GET_KEY_TYPE(db, lk, type);
					if(type < 0)
					{
						KeyObject tk(key, TABLE_META);
						GET_KEY_TYPE(db, tk, type);
					}
				}
			}
		}
		return type;
	}

	int Ardb::Sort(const DBID& db, const Slice& key, const StringArray& args,
	        StringArray& values)
	{
		struct SortOptions
		{
				std::string by;
				bool with_limit;
				StringArray get_patterns;
				bool is_desc;
				bool with_alpha;
				std::string store_dst;
		};

		return 0;
	}

	void Ardb::PrintDB(const DBID& db)
	{
		Slice empty;
		KeyObject start(empty);
		Iterator* iter = FindValue(db, start);
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			Slice tmpval = iter->Value();
			KeyObject* kk = decode_key(tmpkey);
			ValueObject v;
			Buffer readbuf(const_cast<char*>(iter->Value().data()), 0,
			        iter->Value().size());
			decode_value(readbuf, v, false);
			if (NULL != kk)
			{
				DEBUG_LOG(
				        "[%d]Key=%s, Value=%s", kk->type, kk->key.data(), v.ToString().c_str());
			}
			iter->Next();
		}
		DELETE(iter);

	}

}

