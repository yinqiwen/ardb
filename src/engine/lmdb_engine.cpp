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
		mdb_env_set_mapsize(m_env, 100000000 * 100 * 32L / 10);
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
			int env_opt = MDB_NOSYNC | MDB_NOMETASYNC | MDB_NOMETASYNC;
			int rc = mdb_env_open(m_env, m_cfg.path.c_str(), env_opt, 0664);
			if (rc != 0)
			{
				ERROR_LOG("Failed to open mdb:%s\n", mdb_strerror(rc));
				return NULL;
			}
			m_env_opened = true;
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
			m_env(NULL), m_dbi(0)
	{

	}

	LMDBEngine::~LMDBEngine()
	{
		Close();
	}

	void LMDBEngine::Clear()
	{
		if (0 != m_dbi)
		{
			BatchHolder& holder = m_batch_local.GetValue();
			MDB_txn* txn = holder.txn;
			if (NULL == holder.txn)
			{
				mdb_txn_begin(m_env, NULL, 0, &txn);
			}
			mdb_drop(txn, m_dbi, 1);
			if (NULL == holder.txn)
			{
				mdb_txn_commit(txn);
			}
		}
	}
	void LMDBEngine::Close()
	{
		if (0 != m_dbi)
		{
			mdb_dbi_close(m_env, m_dbi);
			m_dbi = 0;
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

	int LMDBEngine::FlushWriteBatch(BatchHolder& holder)
	{
		if (NULL != holder.txn)
		{
			mdb_txn_commit(holder.txn);
			holder.txn = NULL;
		}
		return 0;
	}

	int LMDBEngine::BeginBatchWrite()
	{
		BatchHolder& holder = m_batch_local.GetValue();
		holder.AddRef();
		if (NULL == holder.txn)
		{
			mdb_txn_begin(m_env, NULL, 0, &holder.txn);
		}
		return 0;
	}
	int LMDBEngine::CommitBatchWrite()
	{
		BatchHolder& holder = m_batch_local.GetValue();
		holder.ReleaseRef();
		if (holder.EmptyRef())
		{
			return FlushWriteBatch(holder);
		}
		return 0;
	}
	int LMDBEngine::DiscardBatchWrite()
	{
		BatchHolder& holder = m_batch_local.GetValue();
		holder.ReleaseRef();
		holder.Clear();
		return 0;
	}

	int LMDBEngine::Put(const Slice& key, const Slice& value)
	{
		MDB_val k, v;
		k.mv_data = const_cast<char*>(key.data());
		k.mv_size = key.size();
		v.mv_data = const_cast<char*>(value.data());
		v.mv_size = value.size();
		BatchHolder& holder = m_batch_local.GetValue();
		if (!holder.EmptyRef())
		{
			mdb_put(holder.txn, m_dbi, &k, &v, 0);
		} else
		{
			MDB_txn *txn = NULL;
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
		BatchHolder& holder = m_batch_local.GetValue();
		if (!holder.EmptyRef())
		{
			rc = mdb_get(holder.txn, m_dbi, &k, &v);
		} else
		{
			MDB_txn *txn = NULL;
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
		BatchHolder& holder = m_batch_local.GetValue();
		if (!holder.EmptyRef())
		{
			mdb_del(holder.txn, m_dbi, &k, NULL);
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
		MDB_cursor *cursor = NULL;
		int rc = 0;
		BatchHolder& holder = m_batch_local.GetValue();
		MDB_txn *txn;
		mdb_txn_begin(m_env, holder.txn, 0, &txn);
		rc = mdb_cursor_open(txn, m_dbi, &cursor);
		if (0 != rc)
		{
			ERROR_LOG(
					"Failed to create cursor for reason:%s\n", mdb_strerror(rc));
			return NULL;
		}
		rc = mdb_cursor_get(cursor, &k, &data, MDB_SET_RANGE);
		if (0 != rc)
		{
			rc = mdb_cursor_get(cursor, &k, &data, MDB_LAST);
		}
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

	LMDBIterator::~LMDBIterator()
	{
		mdb_cursor_close(m_cursor);
		mdb_txn_commit(m_txn);

	}
}

