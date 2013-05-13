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
		public:
			LMDBIterator(LMDBEngine *engine, MDB_cursor* iter,
			        bool valid = true) :
					m_engine(engine), m_cursor(iter), m_valid(valid)
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
			int64 max_db;
			LMDBConfig() :
					max_db(1024)
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
			uint32 m_batch_size;
			std::stack<bool> m_batch_stack;
			std::string m_db_path;

			LMDBConfig m_cfg;
			friend class LevelDBEngineFactory;
			int FlushWriteBatch();
		public:
			LMDBEngine();
			~LMDBEngine();
			int Init(const LMDBConfig& cfg, MDB_env *env, const DBID& db);
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
			bool m_env_opened;
			static void ParseConfig(const Properties& props, LMDBConfig& cfg);
		public:
			LMDBEngineFactory(const Properties& cfg);
			KeyValueEngine* CreateDB(const DBID& db);
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
