/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 * 
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 * 
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
 *THE POSSIBILITY OF SUCH DAMAGE.
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
				ValueObject av, bv;
				found_a = decode_value(ak_buf, av, false);
				found_b = decode_value(bk_buf, bv, false);
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
				ret = COMPARE_SLICE(af, bf);
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
				Slice af, bf;
				found_a = BufferHelper::ReadVarSlice(ak_buf, af);
				found_b = BufferHelper::ReadVarSlice(bk_buf, bf);
				COMPARE_EXIST(found_a, found_b);
				ret = COMPARE_SLICE(af, bf);
				if (ret != 0)
				{
					return ret;
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
							return ret;
						}
					}
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
			case TABLE_META:
			case TABLE_SCHEMA:
			case BITSET_META:
			case KEY_EXPIRATION_MAPPING:
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
		if (pos > 0 && (uint32) pos >= buf->ReadableBytes())
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
	Ardb::Ardb(KeyValueEngineFactory* engine, bool multi_thread) :
			m_engine_factory(engine), m_engine(NULL), m_key_watcher(NULL), m_raw_key_listener(
			NULL), m_expire_check_thread(NULL), m_min_expireat(0)
	{
		m_key_locker.enable = multi_thread;
	}

	bool Ardb::Init(uint32 check_expire_period)
	{
		if (NULL == m_engine)
		{
			INFO_LOG("Start init storage engine.");
			m_engine = m_engine_factory->CreateDB(
					m_engine_factory->GetName().c_str());

			KeyObject verkey(Slice(), KEY_END, 0xFFFFFF);
			ValueObject ver;
			if (0 == GetValue(verkey, &ver))
			{
				if (ver.v.int_v != ARDB_FORMAT_VERSION)
				{
					ERROR_LOG("Incompatible data format version:%d in DB",
							ver.v.int_v);
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

				//launch a threading task to check expired keys
				struct ExpireCheckThread: public Thread
				{
						Ardb* adb;
						volatile bool running;
						uint32 check_period;
						ExpireCheckThread(Ardb* db, uint32 period) :
								adb(db), running(true), check_period(period)
						{
						}
						void Run()
						{
							while (running)
							{
								DBID firstDB = 0, lastDB = 0;

								if (0 == adb->LastDB(lastDB)
										&& 0 == adb->FirstDB(firstDB))
								{
									for (DBID db = firstDB; db <= lastDB; db++)
									{
										if (adb->DBExist(db))
										{
											adb->CheckExpireKey(db);
										}
									}
								}
								uint64 end = get_current_epoch_micros();
								uint64 sleep = check_period;
								if (adb->m_min_expireat > end)
								{
									sleep = (adb->m_min_expireat - end) / 1000;
									if (sleep > check_period)
									{
										sleep = check_period;
									}
								}
								Thread::Sleep(sleep);
							}
						}
						~ExpireCheckThread()
						{
							running = false;
						}
				};
				m_expire_check_thread = new ExpireCheckThread(this,
						check_expire_period);
				m_expire_check_thread->Start();
			}
		}
		return m_engine != NULL;
	}

	Ardb::~Ardb()
	{
		DELETE(m_expire_check_thread);
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
		if (GetValue(db, key, NULL) == 0)
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
			GET_KEY_TYPE(zk, type);
			if (type < 0)
			{
				HashKeyObject hk(key, empty, db);
				GET_KEY_TYPE(hk, type);
				if (type < 0)
				{
					KeyObject lk(key, LIST_META, db);
					GET_KEY_TYPE(lk, type);
					if (type < 0)
					{
						KeyObject tk(key, TABLE_META, db);
						GET_KEY_TYPE(tk, type);
						if (type < 0)
						{
							KeyObject bk(key, BITSET_META, db);
							GET_KEY_TYPE(bk, type);
						}
					}
				}
			}
		}
		return type;
	}

	void Ardb::VisitDB(const DBID& db, RawValueVisitor* visitor, Iterator* iter)
	{
		Iterator* current_iter = iter;
		if (NULL == current_iter)
		{
			current_iter = NewIterator(db);
		}
		while (NULL != current_iter && current_iter->Valid())
		{
			DBID tmpdb;
			KeyType t;
			if (!peek_dbkey_header(current_iter->Key(), tmpdb, t)
					|| tmpdb != db)
			{
				break;
			}
			int ret = visitor->OnRawKeyValue(current_iter->Key(),
					current_iter->Value());
			current_iter->Next();
			if (ret == -1)
			{
				break;
			}
		}
		if (NULL == iter)
		{
			DELETE(current_iter);
		}
	}
	void Ardb::VisitAllDB(RawValueVisitor* visitor, Iterator* iter)
	{
		Iterator* current_iter = iter;
		if (NULL == current_iter)
		{
			current_iter = NewIterator();
		}
		while (NULL != current_iter && current_iter->Valid())
		{
			int ret = visitor->OnRawKeyValue(current_iter->Key(),
					current_iter->Value());
			current_iter->Next();
			if (ret == -1)
			{
				break;
			}
		}
		if (NULL == iter)
		{
			DELETE(current_iter);
		}
	}

	Iterator* Ardb::NewIterator()
	{
		Slice empty;
		return GetEngine()->Find(empty, false);
	}
	Iterator* Ardb::NewIterator(const DBID& db)
	{
		KeyObject key(Slice(), KV, db);
		return FindValue(key);
	}

	void Ardb::PrintDB(const DBID& db)
	{
		Slice empty;
		KeyObject start(empty, KV, db);
		Iterator* iter = FindValue(start);
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey, NULL);
			if (kk->db != db)
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
				DEBUG_LOG("[%d]Key=%s, Value=%s", kk->type, kk->key.data(),
						v.ToString(str).c_str());
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

	int Ardb::FirstDB(DBID& db)
	{
		int ret = -1;
		Iterator* iter = NewIterator();
		iter->SeekToFirst();
		if (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey, NULL);
			if (NULL != kk)
			{
				db = kk->db;
				ret = 0;
			}
		}
		DELETE(iter);
		return ret;
	}

	int Ardb::LastDB(DBID& db)
	{
		int ret = -1;
		Iterator* iter = NewIterator();
		iter->SeekToLast();
		if (NULL != iter && iter->Valid())
		{
			//Skip last KEY_END entry
			iter->Prev();
		}
		if (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey, NULL);
			if (NULL != kk)
			{
				db = kk->db;
				ret = 0;
			}
		}
		DELETE(iter);
		return ret;
	}

	bool Ardb::DBExist(const DBID& db)
	{
		KeyObject start(Slice(), KV, db);
		Iterator* iter = FindValue(start, false);
		bool found = false;
		if (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey, NULL);
			if (NULL != kk && kk->db == db)
			{
				found = true;
			}
		}
		DELETE(iter);
		return found;
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
				VisitorTask(Ardb* db, DBID id) :
						adb(db), dbid(id)
				{
				}
				int OnRawKeyValue(const Slice& key, const Slice& value)
				{
					adb->RawDel(key);
					return 0;
				}
				void Run()
				{
					adb->GetEngine()->BeginBatchWrite();
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
				VisitorTask(Ardb* adb) :
						db(adb)
				{
				}
				int OnRawKeyValue(const Slice& key, const Slice& value)
				{
					db->RawDel(key);
					return 0;
				}
				void Run()
				{
					db->GetEngine()->BeginBatchWrite();
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

