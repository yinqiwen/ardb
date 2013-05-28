/*
 * ardb.cpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string.h>
#include <sstream>
#include "comparator.hpp"
#include "util/thread/thread.hpp"

#define  GET_KEY_TYPE(KEY, TYPE)   do{ \
		Iterator* iter = FindValue(KEY, true);  \
		if (NULL != iter && iter->Valid()) \
		{                                  \
			Slice tmp = iter->Key();       \
			KeyObject* k = decode_key(tmp, &KEY); \
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
				RETURN_NONEQ_RESULT(af.size(), bf.size());
				ret = af.compare(bf);
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
				found_a = BufferHelper::ReadFixDouble(ak_buf, ascore);
				found_b = BufferHelper::ReadFixDouble(bk_buf, bscore);
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
					if (ret != 0)
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
							return ret;
						}
					}
				}
				Slice af, bf;
				found_a = BufferHelper::ReadVarSlice(ak_buf, af);
				found_b = BufferHelper::ReadVarSlice(bk_buf, bf);
				COMPARE_EXIST(found_a, found_b);
				ret = COMPARE_NUMBER(af.size(), bf.size());
				if (ret != 0)
				{
					return ret;
				}
				ret = af.compare(bf);
				break;
			}
			case SET_META:
			case ZSET_META:
			case LIST_META:
			case TABLE_META:
			case TABLE_SCHEMA:
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

	//static const char* REPO_NAME = "data";
	Ardb::Ardb(KeyValueEngineFactory* engine, const std::string& path,
			bool multi_thread) :
			m_engine_factory(engine), m_engine(NULL), m_key_watcher(NULL), m_raw_key_listener(
					NULL), m_path(path)
	{
		m_key_locker.enable = multi_thread;
	}

	bool Ardb::Init()
	{
		if (NULL == m_engine)
		{
			INFO_LOG("Start init storage engine.");
			m_engine = m_engine_factory->CreateDB(
					m_engine_factory->GetName().c_str());

			KeyObject verkey(Slice(), KEY_END, 0xFFFFFF);
			ValueObject ver;
			if (0 == GetValue(verkey, &ver, NULL))
			{
				if (ver.v.int_v != ARDB_FORMAT_VERSION)
				{
					ERROR_LOG(
							"Incompatible data format version:%d in DB", ver.v.int_v);
					return false;
				}
			} else
			{
				ver.v.int_v = ARDB_FORMAT_VERSION;
				ver.type = INTEGER;
				SetValue(verkey, ver);
			}
			if (NULL != m_engine)
			{
				INFO_LOG("Init storage engine success.");
			}
		}
		return m_engine != NULL;
	}

	Ardb::~Ardb()
	{
		if (NULL != m_engine)
		{
			m_engine_factory->CloseDB(m_engine);
		}
	}

	void Ardb::Walk(KeyObject& key, bool reverse, WalkHandler* handler)
	{
		bool isFirstElement = true;
		Iterator* iter = FindValue(key);
		if (NULL != iter && !iter->Valid() && reverse)
		{
			iter->SeekToLast();
			isFirstElement = false;
		}
		uint32 cursor = 0;
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey, &key);
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
			} else
			{
				iter->Next();
			}
		}
		DELETE(iter);
	}

	KeyValueEngine* Ardb::GetEngine()
	{
		return m_engine;
	}

	int Ardb::RawSet(const Slice& key, const Slice& value)
	{
		int ret = GetEngine()->Put(key, value);
		if (ret == 0 && NULL != m_raw_key_listener)
		{
			m_raw_key_listener->OnKeyUpdated(key, value);
		}
		return ret;
	}
	int Ardb::RawDel(const Slice& key)
	{
		int ret = GetEngine()->Del(key);
		if (ret == 0 && NULL != m_raw_key_listener)
		{
			m_raw_key_listener->OnKeyDeleted(key);
		}
		return ret;
	}

	int Ardb::RawGet(const Slice& key, std::string* value)
	{
		return GetEngine()->Get(key, value);
	}

	int Ardb::Type(const DBID& db, const Slice& key)
	{
		if (Exists(db, key))
		{
			return KV;
		}
		int type = -1;
		Slice empty;
		SetKeyObject sk(key, empty, db);
		GET_KEY_TYPE(sk, type);
		if (type < 0)
		{
			ZSetScoreKeyObject zk(key, empty, db);
			GET_KEY_TYPE( zk, type);
			if (type < 0)
			{
				HashKeyObject hk(key, empty, db);
				GET_KEY_TYPE( hk, type);
				if (type < 0)
				{
					KeyObject lk(key, LIST_META, db);
					GET_KEY_TYPE( lk, type);
					if (type < 0)
					{
						KeyObject tk(key, TABLE_META, db);
						GET_KEY_TYPE(tk, type);
					}
				}
			}
		}
		return type;
	}

	void Ardb::VisitDB(const DBID& db, RawValueVisitor* visitor)
	{
		KeyObject key(Slice(), KV, db);
		Iterator* iter = FindValue(key);
		while (NULL != iter && iter->Valid())
		{
			DBID tmpdb;
			KeyType t;
			if (!peek_dbkey_header(iter->Key(), tmpdb, t) || tmpdb != db)
			{
				break;
			}
			visitor->OnRawKeyValue(iter->Key(), iter->Value());
			iter->Next();
		}
		DELETE(iter);
	}
	void Ardb::VisitAllDB(RawValueVisitor* visitor)
	{
		Slice empty;
		Iterator* iter = GetEngine()->Find(empty, false);
		while (NULL != iter && iter->Valid())
		{
			visitor->OnRawKeyValue(iter->Key(), iter->Value());
			iter->Next();
		}
		DELETE(iter);
	}

	void Ardb::PrintDB(const DBID& db)
	{
		Slice empty;
		KeyObject start(empty, KV, db);
		Iterator* iter = FindValue(start);
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			Slice tmpval = iter->Value();
			KeyObject* kk = decode_key(tmpkey, NULL);
			if(kk->db != db)
			{
				DELETE(kk);
				break;
			}
			ValueObject v;
			Buffer readbuf(const_cast<char*>(iter->Value().data()), 0,
					iter->Value().size());
			decode_value(readbuf, v, false);
			if (NULL != kk)
			{
				std::string str;
				DEBUG_LOG(
						"[%d]Key=%s, Value=%s", kk->type, kk->key.data(), v.ToString(str).c_str());
			}
			DELETE(kk);
			iter->Next();
		}
		DELETE(iter);
	}

	int Ardb::CompactAll()
	{
		struct CompactTask: public Thread
		{
				Ardb* adb;
				CompactTask(Ardb* db) :
						adb(db)
				{
				}
				void Run()
				{
					adb->GetEngine()->CompactRange(Slice(), Slice());
					delete this;
				}
		};
		/*
		 * Start a background thread to compact kvs
		 */
		Thread* t = new CompactTask(this);
		t->Start();
		return 0;
		return 0;
	}

	int Ardb::CompactDB(const DBID& db)
	{
		struct CompactTask: public Thread
		{
				Ardb* adb;
				DBID dbid;
				CompactTask(Ardb* db, DBID id) :
						adb(db), dbid(id)
				{
				}
				void Run()
				{
					KeyObject start(Slice(), KV, dbid);
					KeyObject end(Slice(), KV, dbid + 1);
					Buffer sbuf, ebuf;
					encode_key(sbuf, start);
					encode_key(ebuf, end);
					adb->GetEngine()->CompactRange(sbuf.AsString(),
							ebuf.AsString());
					delete this;
				}
		};
		/*
		 * Start a background thread to compact kvs
		 */
		Thread* t = new CompactTask(this, db);
		t->Start();
		return 0;
	}

	int Ardb::FlushDB(const DBID& db)
	{
		struct VisitorTask: public RawValueVisitor, public Thread
		{
				Ardb* adb;
				DBID dbid;
				uint32 count;
				VisitorTask(Ardb* db, DBID id) :
						adb(db), dbid(id), count(0)
				{
				}
				int OnRawKeyValue(const Slice& key, const Slice& value)
				{
					if (count % 100 == 0)
					{
						if (count > 0)
						{
							adb->GetEngine()->CommitBatchWrite();
						}
						adb->GetEngine()->BeginBatchWrite();
					}
					count++;
					adb->RawDel(key);
					return 0;
				}
				void Run()
				{
					adb->VisitDB(dbid, this);
					adb->GetEngine()->CommitBatchWrite();
					KeyObject start(Slice(), KV, dbid);
					KeyObject end(Slice(), KV, dbid + 1);
					Buffer sbuf, ebuf;
					encode_key(sbuf, start);
					encode_key(ebuf, end);
					adb->GetEngine()->CompactRange(sbuf.AsString(),
							ebuf.AsString());
					delete this;
				}
		};
		/*
		 * Start a background thread to delete kvs
		 */
		Thread* t = new VisitorTask(this, db);
		t->Start();
		return 0;
	}

	int Ardb::FlushAll()
	{
		struct VisitorTask: public RawValueVisitor, public Thread
		{
				Ardb* db;
				uint32 count;
				VisitorTask(Ardb* adb) :
						db(adb), count(0)
				{
				}
				int OnRawKeyValue(const Slice& key, const Slice& value)
				{
					if (count % 100 == 0)
					{
						if (count > 0)
						{
							db->GetEngine()->CommitBatchWrite();
						}
						db->GetEngine()->BeginBatchWrite();
					}
					count++;
					db->RawDel(key);
					return 0;
				}
				void Run()
				{
					db->VisitAllDB(this);
					db->GetEngine()->CommitBatchWrite();
					db->GetEngine()->CompactRange(Slice(), Slice());
					delete this;
				}
		};
		/*
		 * Start a background thread to delete kvs
		 */
		Thread* t = new VisitorTask(this);
		t->Start();
		return 0;
	}
}

