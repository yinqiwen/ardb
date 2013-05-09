/*
 * lmdb_engine.hpp
 *
 *  Created on: 2013-5-9
 *      Author: wqy
 */

#ifndef LMDB_ENGINE_HPP_
#define LMDB_ENGINE_HPP_

#include "lmdb.h"
#include "ardb.hpp"
#include "util/config_helper.hpp"
#include <stack>

namespace ardb
{
	class LMDBIterator: public Iterator
	{
		private:
			MDB_cursor * m_cursor;
			void Next();
			void Prev();
			Slice Key() const;
			Slice Value() const;
			bool Valid();
			void SeekToFirst();
			void SeekToLast();
		public:
			LMDBIterator(MDB_cursor* iter) :
					m_cursor(iter)
			{

			}
			~LMDBIterator()
			{
				mdb_cursor_close(m_cursor);
			}
	};

	struct LMDBConfig
	{
			std::string path;
			int64 block_cache_size;
			int64 write_buffer_size;
			int64 max_open_files;
			int64 block_size;
			int64 block_restart_interval;
			int64 bloom_bits;
			int64 batch_commit_watermark;
			LMDBConfig() :
					block_cache_size(0), write_buffer_size(0), max_open_files(
					        10240), block_size(0), block_restart_interval(0), bloom_bits(
					        10), batch_commit_watermark(30)
			{
			}
	};
	class LMDBEngineFactory;
	class LMDBEngine: public KeyValueEngine
	{
		private:
			MDB_env *m_env;
			MDB_dbi m_dbi;
			MDB_txn *m_txn;
			std::stack<bool> m_batch_stack;
			std::string m_db_path;
			uint32 m_batch_size;

			LMDBConfig m_cfg;
			friend class LevelDBEngineFactory;
			int FlushWriteBatch();
		public:
			LMDBEngine();
			~LMDBEngine();
			int Init(const LMDBConfig& cfg, MDB_env *env);
			int Put(const Slice& key, const Slice& value);
			int Get(const Slice& key, std::string* value);
			int Del(const Slice& key);
			int BeginBatchWrite();
			int CommitBatchWrite();
			int DiscardBatchWrite();
			Iterator* Find(const Slice& findkey);
			void Close();
			void Clear();

	};

	class LMDBEngineFactory: public KeyValueEngineFactory
	{
		private:
			LMDBConfig m_cfg;
			MDB_env *m_env;
			static void ParseConfig(const Properties& props, LMDBConfig& cfg);
		public:
			LMDBEngineFactory(const Properties& cfg);
			KeyValueEngine* CreateDB(const DBID& db);
			void DestroyDB(KeyValueEngine* engine);
			void CloseDB(KeyValueEngine* engine);
			void ListAllDB(DBIDSet& dbs);
	};
}

#endif /* LMDB_ENGINE_HPP_ */
