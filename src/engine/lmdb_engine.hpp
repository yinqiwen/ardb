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

#ifndef LMDB_ENGINE_HPP_
#define LMDB_ENGINE_HPP_

#include "lmdb.h"
#include "db.hpp"
#include "util/config_helper.hpp"
#include "util/thread/thread_local.hpp"
#include <stack>

namespace ardb
{
	class LMDBEngine;
	class LMDBIterator: public Iterator
	{
		private:
			LMDBEngine *m_engine;
			MDB_cursor * m_cursor;
			MDB_val m_key;
			MDB_val m_value;
			bool m_valid;
			void Next();
			void Prev();
			Slice Key() const;
			Slice Value() const;
			bool Valid();
			void SeekToFirst();
			void SeekToLast();
			void Seek(const Slice& target);
			friend class LMDBEngine;
		public:
			LMDBIterator(LMDBEngine * e, MDB_cursor* iter, bool valid = true) :
					m_engine(e), m_cursor(iter), m_valid(valid)
			{
				if (valid)
				{
					int rc = mdb_cursor_get(m_cursor, &m_key, &m_value,
					        MDB_GET_CURRENT);
					m_valid = rc == 0;
				}
			}
			~LMDBIterator();
	};

	struct LMDBConfig
	{
			std::string path;
			LMDBConfig()
			{
			}
	};
	class LMDBEngineFactory;
	class LMDBEngine: public KeyValueEngine
	{
		private:
			MDB_env *m_env;
			MDB_dbi m_dbi;
			struct BatchHolder
			{
					MDB_txn *txn;
					uint32 ref;
					StringSet dels;
					std::map<std::string, std::string> inserts;
					void ReleaseRef()
					{
						if (ref > 0)
							ref--;
					}
					void AddRef()
					{
						ref++;
					}
					bool EmptyRef()
					{
						return ref == 0;
					}
					void Del(const Slice& key)
					{
						std::string str(key.data(), key.size());
						dels.insert(str);

					}
					void Put(const Slice& key, const Slice& value)
					{
						std::string str(key.data(), key.size());
						std::string v(value.data(), value.size());
						dels.erase(str);
						inserts[str] = v;
					}
					void Clear()
					{
						if (NULL != txn)
						{
							mdb_txn_abort(txn);
							txn = NULL;
						}
						ref = 0;
						dels.clear();
						inserts.clear();
					}
					BatchHolder() :
							txn(NULL), ref(0)
					{
					}
			};
			ThreadLocal<BatchHolder> m_batch_local;
			std::string m_db_path;

			LMDBConfig m_cfg;
			friend class LMDBIterator;
			int FlushWriteBatch(BatchHolder& holder);
		public:
			LMDBEngine();
			~LMDBEngine();
			int Init(const LMDBConfig& cfg, MDB_env *env,
			        const std::string& name);
			int Put(const Slice& key, const Slice& value);
			int Get(const Slice& key, std::string* value, bool fill_cache);
			int Del(const Slice& key);
			int BeginBatchWrite();
			int CommitBatchWrite();
			int DiscardBatchWrite();
			Iterator* Find(const Slice& findkey, bool cache);
			void Close();
			void Clear();

	};

	class LMDBEngineFactory: public KeyValueEngineFactory
	{
		private:
			LMDBConfig m_cfg;
			MDB_env *m_env;
			bool m_env_opened;
			static void ParseConfig(const Properties& props, LMDBConfig& cfg);
		public:
			LMDBEngineFactory(const Properties& cfg);
			KeyValueEngine* CreateDB(const std::string& name);
			void DestroyDB(KeyValueEngine* engine);
			void CloseDB(KeyValueEngine* engine);
			const std::string GetName()
			{
				return "LMDB";
			}
			~LMDBEngineFactory();
	};
}

#endif /* LMDB_ENGINE_HPP_ */
