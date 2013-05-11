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
			std::stack<bool> m_batch_stack;
			std::set<std::string> m_bulk_del_keys;
			std::map<std::string, std::string> m_bulk_set_kvs;
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
			Iterator* Find(const Slice& findkey);
	};

	class KCDBEngineFactory: public KeyValueEngineFactory
	{
		private:
			KCDBConfig m_cfg;
			static void ParseConfig(const Properties& props, KCDBConfig& cfg);
		public:
			KCDBEngineFactory(const Properties& cfg);
			KeyValueEngine* CreateDB(const DBID& db);
			void DestroyDB(KeyValueEngine* engine);
			void CloseDB(KeyValueEngine* engine);
	};
}

#endif /* KYOTOCABINET_ENGINE_HPP_ */
