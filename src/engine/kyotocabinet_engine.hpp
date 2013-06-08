/*
 * kyotocabinet_engine.hpp
 *
 *  Created on: 2013-4-10
 *      Author: wqy
 */

#ifndef KYOTOCABINET_ENGINE_HPP_
#define KYOTOCABINET_ENGINE_HPP_

/*
 * leveldb_engine.hpp
 *
 *  Created on: 2013-3-31
 *      Author: wqy
 */

#include <kcpolydb.h>
#include <stack>
#include "ardb.hpp"
#include "util/config_helper.hpp"
#include "util/thread/thread_local.hpp"

namespace ardb
{
	class KCDBIterator: public Iterator
	{
		private:
			kyotocabinet::DB::Cursor* m_cursor;
			bool m_valid;
			Buffer m_key_buffer;
			Buffer m_value_buffer;
			void Next();
			void Prev();
			Slice Key() const;
			Slice Value() const;
			bool Valid();
			void SeekToFirst();
			void SeekToLast();
		public:
			KCDBIterator(kyotocabinet::DB::Cursor* cursor, bool valid = true) :
					m_cursor(cursor), m_valid(valid)
			{
			}
			~KCDBIterator()
			{
				DELETE(m_cursor);
			}
	};

	class KCDBComparator: public kyotocabinet::Comparator
	{
			int32_t compare(const char* akbuf, size_t aksiz, const char* bkbuf,
					size_t bksiz);
	};

	struct KCDBConfig
	{
			std::string path;
	};

	class KCDBEngine: public KeyValueEngine
	{
		private:
			kyotocabinet::TreeDB* m_db;
			KCDBComparator m_comparator;

			struct BatchHolder
			{
					StringSet batch_del;
					StringStringMap batch_put;
					uint32 ref;
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
					void Put(const Slice& key, const Slice& value)
					{
						std::string keystr(key.data(), key.size());
						std::string valstr(value.data(), value.size());
						batch_del.erase(keystr);
						batch_put.insert(
								StringStringMap::value_type(keystr, valstr));
					}
					void Del(const Slice& key)
					{
						std::string keystr(key.data(), key.size());
						batch_del.insert(keystr);
						batch_put.erase(keystr);
					}
					void Clear()
					{
						batch_del.clear();
						batch_put.clear();
						ref = 0;
					}
					BatchHolder() :
							ref(0)
					{
					}
			};
			ThreadLocal<BatchHolder> m_batch_local;
			int FlushWriteBatch(BatchHolder& holder);
		public:
			KCDBEngine();
			~KCDBEngine();
			void Clear();
			void Close();
			int Init(const KCDBConfig& cfg);
			int Put(const Slice& key, const Slice& value);
			int Get(const Slice& key, std::string* value);
			int Del(const Slice& key);
			int BeginBatchWrite();
			int CommitBatchWrite();
			int DiscardBatchWrite();
			Iterator* Find(const Slice& findkey, bool cache);
	};

	class KCDBEngineFactory: public KeyValueEngineFactory
	{
		private:
			KCDBConfig m_cfg;
			static void ParseConfig(const Properties& props, KCDBConfig& cfg);
		public:
			KCDBEngineFactory(const Properties& cfg);
			KeyValueEngine* CreateDB(const std::string& db);
			void DestroyDB(KeyValueEngine* engine);
			void CloseDB(KeyValueEngine* engine);
			const std::string GetName()
			{
				return "KyotoCabinet";
			}
	};
}

#endif /* KYOTOCABINET_ENGINE_HPP_ */
