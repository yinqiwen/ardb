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

	void OpLogs::Run()
	{
		Load();
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
	}
}

