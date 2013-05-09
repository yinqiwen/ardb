/*
 * lmdb_engine.cpp
 *
 *  Created on: 2013-5-9
 *      Author: wqy
 */

#include "lmdb_engine.hpp"
#include "ardb.hpp"
#include "ardb_data.hpp"
#include "comparator.hpp"
#include "util/helpers.hpp"
#include <string.h>

namespace ardb
{

	LMDBEngineFactory::LMDBEngineFactory(const Properties& props) :
			m_env(NULL)
	{
		ParseConfig(props, m_cfg);
		mdb_env_create(&m_env);
	}

	void LMDBEngineFactory::ParseConfig(const Properties& props,
	        LMDBConfig& cfg)
	{
		cfg.path = ".";
		conf_get_string(props, "dir", cfg.path);
	}
	KeyValueEngine* LMDBEngineFactory::CreateDB(const DBID& db)
	{
		LMDBEngine* engine = new LMDBEngine();
		char tmp[m_cfg.path.size() + db.size() + 10];
		sprintf(tmp, "%s/%s", m_cfg.path.c_str(), db.c_str());
		LMDBConfig cfg = m_cfg;
		cfg.path = tmp;
		if (engine->Init(cfg, m_env) != 0)
		{
			DELETE(engine);
			return NULL;
		}
		DEBUG_LOG("Create DB:%s at path:%s success", db.c_str(), tmp);
		return engine;
	}

	void LMDBEngineFactory::ListAllDB(DBIDSet& dbs)
	{
		std::deque<std::string> dirs;
		list_subfiles(m_cfg.path, dirs);
		std::deque<std::string>::iterator it = dirs.begin();
		while (it != dirs.end())
		{
			dbs.insert(*it);
			it++;
		}
	}

	void LMDBEngineFactory::CloseDB(KeyValueEngine* engine)
	{
		DELETE(engine);
	}

	void LMDBEngineFactory::DestroyDB(KeyValueEngine* engine)
	{
		KCDBEngine* kcdb = (KCDBEngine*) engine;
		if (NULL != kcdb)
		{
			kcdb->Clear();
		}
		DELETE(engine);
	}

	LMDBEngine::LMDBEngine() :
			m_env(NULL), m_dbi(0), m_txn(NULL)
	{

	}

	LMDBEngine::~LMDBEngine()
	{
		Close();
		//DELETE(m_db);
	}

	void LMDBEngine::Clear()
	{
		if (0 != m_dbi)
		{
			//m_db->clear();
		}
	}
	void LMDBEngine::Close()
	{
		if (0 != m_dbi)
		{
			//m_db->close();
		}
	}

	int LMDBEngine::Init(const LMDBConfig& cfg, MDB_env *env)
	{
		m_env = env;
		make_file(cfg.path);
		mdb_env_open(env, cfg.path.c_str(), 0, 0664);

		return 0;
	}

	int LMDBEngine::BeginBatchWrite()
	{
		mdb_txn_begin(m_env, NULL, 0, &m_txn);
		return 0;
	}
	int LMDBEngine::CommitBatchWrite()
	{
		mdb_txn_commit(m_txn);
		m_txn = NULL;
		return 0;
	}
	int LMDBEngine::DiscardBatchWrite()
	{
		mdb_txn_abort(m_txn);
		m_txn = NULL;
		return 0;
	}

	int LMDBEngine::Put(const Slice& key, const Slice& value)
	{
		return 0;
	}
	int LMDBEngine::Get(const Slice& key, std::string* value)
	{
		return 0;
	}
	int LMDBEngine::Del(const Slice& key)
	{
		return 0;
	}

	Iterator* LMDBEngine::Find(const Slice& findkey)
	{
		return NULL;
	}

	void LMDBIterator::SeekToFirst()
	{

	}
	void LMDBIterator::SeekToLast()
	{

	}

	void LMDBIterator::Next()
	{

	}
	void LMDBIterator::Prev()
	{

	}
	Slice LMDBIterator::Key() const
	{

	}
	Slice LMDBIterator::Value() const
	{

	}
	bool LMDBIterator::Valid()
	{
		return 0;
	}
}

