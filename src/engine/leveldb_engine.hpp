/*
 * leveldb_engine.hpp
 *
 *  Created on: 2013-3-31
 *      Author: wqy
 */

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/comparator.h"
#include "ardb.hpp"
#include "util/config_helper.hpp"
#include <stack>

namespace ardb
{
	class LevelDBIterator: public Iterator
	{
		private:
			leveldb::Iterator* m_iter;
			void Next();
			void Prev();
			Slice Key() const;
			Slice Value() const;
			bool Valid();
			void SeekToFirst();
			void SeekToLast();
		public:
			LevelDBIterator(leveldb::Iterator* iter) :
					m_iter(iter)
			{

			}
			~LevelDBIterator()
			{
				delete m_iter;
			}
	};

	class LevelDBComparator: public leveldb::Comparator
	{
		public:
			// Three-way comparison.  Returns value:
			//   < 0 iff "a" < "b",
			//   == 0 iff "a" == "b",
			//   > 0 iff "a" > "b"
			int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const;

			// The name of the comparator.  Used to check for comparator
			// mismatches (i.e., a DB created with one comparator is
			// accessed using a different comparator.
			//
			// The client of this package should switch to a new name whenever
			// the comparator implementation changes in a way that will cause
			// the relative ordering of any two keys to change.
			//
			// Names starting with "leveldb." are reserved and should not be used
			// by any clients of this package.
			const char* Name() const
			{
				return "ARDBLevelDB";
			}

			// Advanced functions: these are used to reduce the space requirements
			// for internal data structures like index blocks.

			// If *start < limit, changes *start to a short string in [start,limit).
			// Simple comparator implementations may return with *start unchanged,
			// i.e., an implementation of this method that does nothing is correct.
			void FindShortestSeparator(std::string* start,
					const leveldb::Slice& limit) const;

			// Changes *key to a short string >= *key.
			// Simple comparator implementations may return with *key unchanged,
			// i.e., an implementation of this method that does nothing is correct.
			void FindShortSuccessor(std::string* key) const;
	};

	struct LevelDBConfig
	{
			std::string path;
			int64 block_cache_size;
			int64 write_buffer_size;
			int64 max_open_files;
			int64 block_size;
			int64 block_restart_interval;
			int64 bloom_bits;
			int64 batch_commit_watermark;
			LevelDBConfig() :
					block_cache_size(0), write_buffer_size(0), max_open_files(
							10240), block_size(0), block_restart_interval(0), bloom_bits(
							10), batch_commit_watermark(30)
			{
			}
	};
	class LevelDBEngineFactory;
	class LevelDBEngine: public KeyValueEngine
	{
		private:
			leveldb::DB* m_db;
			LevelDBComparator m_comparator;
			leveldb::WriteBatch m_batch;
			std::stack<bool> m_batch_stack;
			std::string m_db_path;
			uint32 m_batch_size;

			LevelDBConfig m_cfg;
			friend class LevelDBEngineFactory;
			int FlushWriteBatch();
		public:
			LevelDBEngine();
			~LevelDBEngine();
			int Init(const LevelDBConfig& cfg);
			int Put(const Slice& key, const Slice& value);
			int Get(const Slice& key, std::string* value);
			int Del(const Slice& key);
			int BeginBatchWrite();
			int CommitBatchWrite();
			int DiscardBatchWrite();
			Iterator* Find(const Slice& findkey);
	};

	class LevelDBEngineFactory: public KeyValueEngineFactory
	{
		private:
			LevelDBConfig m_cfg;
			static void ParseConfig(const Properties& props,
					LevelDBConfig& cfg);
		public:
			LevelDBEngineFactory(const Properties& cfg);
			KeyValueEngine* CreateDB(const DBID& db);
			void DestroyDB(KeyValueEngine* engine);
			void CloseDB(KeyValueEngine* engine);
			void ListAllDB(DBIDSet& dbs);
	};
}

