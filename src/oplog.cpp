/*
 * oplog.cpp
 *
 *  Created on: 2013-5-1
 *      Author: wqy
 */
#include "replication.hpp"

namespace ardb
{
	OpKey::OpKey(const DBID& id, const std::string& k) :
			db(id), key(k)
	{

	}
	bool OpKey::operator<(const OpKey& other) const
	{
		if (db > other.db)
		{
			return false;
		}
		if (key == other.key)
		{
			return key < other.key;
		}
		return true;
	}

	OpLogs::OpLogs(ArdbServerConfig& cfg) :
			m_cfg(cfg), m_min_seq(0), m_max_seq(0), m_op_log_file(NULL)
	{

	}

	void OpLogs::Load()
	{

	}

	void OpLogs::Run()
	{
		Load();
		while(true)
		{
			Runnable* task = NULL;
			{
				LockGuard<ThreadMutexLock> guard(m_lock);
				while (m_tasks.empty())
				{
					m_lock.Wait(1000);
				}
				if(!m_tasks.empty())
				{
					task = m_tasks.front();
					m_tasks.pop_front();
				}
			}
			if(NULL != task)
			{
				task->Run();
			}
		}

	}

	void OpLogs::PostTask(Runnable* r)
	{
		LockGuard<ThreadMutexLock> guard(m_lock);
		m_tasks.push_back(r);
		m_lock.Notify();
	}

	void OpLogs::CheckCurrentDB(const DBID& db)
	{
		if (m_current_db != db)
		{
			//generate 'select' cmd
			m_current_db = db;
			ArgumentArray strs;
			strs.push_back("select");
			strs.push_back(m_current_db);
			SaveCmdOp(new RedisCommandFrame(strs));
		}
	}

	void OpLogs::SaveCmdOp(RedisCommandFrame* cmd)
	{

	}

	int OpLogs::LoadOpLog(uint64 seq)
	{
		if(seq < m_min_seq)
		{
			return -1;
		}
	}

	void OpLogs::SaveSetOp(const DBID& db, const Slice& key, const Slice& value)
	{
		CheckCurrentDB(db);
	}
	void OpLogs::SaveDeleteOp(const DBID& db, const Slice& key)
	{
		CheckCurrentDB(db);
	}
	void OpLogs::SaveFlushOp(const DBID& db)
	{
		CheckCurrentDB(db);
		ArgumentArray strs;
		strs.push_back("flushdb");
		SaveCmdOp(new RedisCommandFrame(strs));
	}
}

