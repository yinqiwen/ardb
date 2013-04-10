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
			int Init(const std::string& path);
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
			std::string m_base_path;
		public:
			KCDBEngineFactory(const std::string& basepath);
			KeyValueEngine* CreateDB(DBID db);
			void DestroyDB(KeyValueEngine* engine);
	};
}

#endif /* KYOTOCABINET_ENGINE_HPP_ */
