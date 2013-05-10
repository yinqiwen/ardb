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
	static int LMDBCompareFunc(const MDB_val *a, const MDB_val *b)
	{
		return ardb_compare_keys((const char*) a->mv_data, a->mv_size,
				(const char*) b->mv_data, b->mv_size);
	}
	LMDBEngineFactory::LMDBEngineFactory(const Properties& props) :
			m_env(NULL), m_env_opened(false)
	{
		ParseConfig(props, m_cfg);
		mdb_env_create(&m_env);
		mdb_env_set_maxdbs(m_env, m_cfg.max_db);
	}

	LMDBEngineFactory::~LMDBEngineFactory()
	{
		mdb_env_close(m_env);
	}

	void LMDBEngineFactory::ParseConfig(const Properties& props,
			LMDBConfig& cfg)
	{
		cfg.path = ".";
		conf_get_string(props, "dir", cfg.path);
	}
	KeyValueEngine* LMDBEngineFactory::CreateDB(const DBID& db)
	{
		if (!m_env_opened)
		{
			make_dir(m_cfg.path);
			int rc = mdb_env_open(m_env, m_cfg.path.c_str(), 0, 0664);
			if (rc != 0)
			{
				ERROR_LOG("Failed to open mdb:%s\n", mdb_strerror(rc));
				return NULL;
			}
		}
		LMDBEngine* engine = new LMDBEngine();
		LMDBConfig cfg = m_cfg;
		if (engine->Init(cfg, m_env, db) != 0)
		{
			DELETE(engine);
			return NULL;
		}
		DEBUG_LOG(
				"Create DB:%s at path:%s success", db.c_str(), cfg.path.c_str());
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
		LMDBEngine* kcdb = (LMDBEngine*) engine;
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

	int LMDBEngine::Init(const LMDBConfig& cfg, MDB_env *env, const DBID& db)
	{
		m_env = env;
		MDB_txn *txn;
		int rc = mdb_txn_begin(env, NULL, 0, &txn);
		rc = mdb_open(txn, db.c_str(), MDB_CREATE, &m_dbi);
		if (rc != 0)
		{
			ERROR_LOG(
					"Failed to open mdb:%s for reason:%s\n", db.c_str(), mdb_strerror(rc));
			return -1;
		}
		mdb_set_compare(txn, m_dbi, LMDBCompareFunc);
		mdb_txn_commit(txn);
		return 0;
	}

	int LMDBEngine::BeginBatchWrite()
	{
		m_batch_stack.push(true);
		if (NULL == m_txn)
		{
			mdb_txn_begin(m_env, NULL, 0, &m_txn);
		}
		return 0;
	}
	int LMDBEngine::CommitBatchWrite()
	{
		if (!m_batch_stack.empty())
		{
			m_batch_stack.pop();
		}
		if (m_batch_stack.empty() && NULL != m_txn)
		{
			mdb_txn_commit(m_txn);
			m_txn = NULL;
		}
		return 0;
	}
	int LMDBEngine::DiscardBatchWrite()
	{
		if (!m_batch_stack.empty())
		{
			m_batch_stack.pop();
		}
		if (NULL != m_txn)
		{
			mdb_txn_abort(m_txn);
			m_txn = NULL;
		}
		return 0;
	}

	int LMDBEngine::Put(const Slice& key, const Slice& value)
	{
		MDB_val k, v;
		k.mv_data = const_cast<char*>(key.data());
		k.mv_size = key.size();
		v.mv_data = const_cast<char*>(value.data());
		v.mv_size = value.size();
		if (NULL != m_txn)
		{
			mdb_put(m_txn, m_dbi, &k, &v, 0);
		} else
		{
			MDB_txn *txn;
			mdb_txn_begin(m_env, NULL, 0, &txn);
			mdb_put(txn, m_dbi, &k, &v, 0);
			mdb_txn_commit(txn);
		}
		return 0;
	}
	int LMDBEngine::Get(const Slice& key, std::string* value)
	{
		MDB_val k, v;
		k.mv_data = const_cast<char*>(key.data());
		k.mv_size = key.size();
		int rc;
		if (NULL != m_txn)
		{
			rc = mdb_get(m_txn, m_dbi, &k, &v);
		} else
		{
			MDB_txn *txn;
			mdb_txn_begin(m_env, NULL, 0, &txn);
			rc = mdb_get(txn, m_dbi, &k, &v);
			mdb_txn_commit(txn);
		}
		if (0 == rc && NULL != value && NULL != v.mv_data)
		{
			value->assign((const char*) v.mv_data, v.mv_size);
		}
		return rc;
	}
	int LMDBEngine::Del(const Slice& key)
	{
		MDB_val k;
		k.mv_data = const_cast<char*>(key.data());
		k.mv_size = key.size();
		if (NULL != m_txn)
		{
			mdb_del(m_txn, m_dbi, &k, NULL);
		} else
		{
			MDB_txn *txn;
			mdb_txn_begin(m_env, NULL, 0, &txn);
			mdb_del(txn, m_dbi, &k, NULL);
			mdb_txn_commit(txn);
		}
		return 0;
	}

	Iterator* LMDBEngine::Find(const Slice& findkey)
	{
		MDB_val k, data;
		k.mv_data = const_cast<char*>(findkey.data());
		k.mv_size = findkey.size();
		MDB_txn *txn;
		MDB_cursor *cursor;
		int rc = mdb_txn_begin(m_env, NULL, MDB_RDONLY, &txn);
		if(rc != 0)
		{
			//ERROR_LOG("Failed to create cursor:%s", mdb_strerror(rc));
			return NULL;
		}
		rc = mdb_cursor_open(txn, m_dbi, &cursor);
		rc = mdb_cursor_get(cursor, &k, &data, MDB_SET_RANGE);
		return new LMDBIterator(txn, cursor, rc == 0);
	}

	void LMDBIterator::SeekToFirst()
	{
		int rc = mdb_cursor_get(m_cursor, &m_key, &m_value, MDB_FIRST);
		m_valid = rc == 0;
	}
	void LMDBIterator::SeekToLast()
	{
		int rc = mdb_cursor_get(m_cursor, &m_key, &m_value, MDB_LAST);
		m_valid = rc == 0;
	}

	void LMDBIterator::Next()
	{
		int rc = mdb_cursor_get(m_cursor, &m_key, &m_value, MDB_NEXT);
		m_valid = rc == 0;
	}
	void LMDBIterator::Prev()
	{
		int rc = mdb_cursor_get(m_cursor, &m_key, &m_value, MDB_PREV);
		m_valid = rc == 0;
	}
	Slice LMDBIterator::Key() const
	{
		return Slice((const char*) m_key.mv_data, m_key.mv_size);
	}
	Slice LMDBIterator::Value() const
	{
		return Slice((const char*) m_value.mv_data, m_value.mv_size);
	}
	bool LMDBIterator::Valid()
	{
		return m_valid;
	}
}

