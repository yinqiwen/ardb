/*
 * oplog.cpp
 *
 *  Created on: 2013-5-1
 *      Author: wqy
 */
#include "replication.hpp"
#include "ardb_server.hpp"
#include <sstream>

#define OPLOGS_VER 1

namespace ardb
{
	static const uint8 kSetOpType = 1;
	static const uint8 kDelOpType = 2;
	static const uint8 kOtherOpType = 3;

	static const uint32 k_max_rolling_index = 3;

	CachedWriteOp::CachedWriteOp(uint8 t, OpKey& k) :
			CachedOp(t), key(k), v(NULL)
	{
	}
	CachedWriteOp::~CachedWriteOp()
	{
		DELETE(v);
	}
	CachedCmdOp::CachedCmdOp(RedisCommandFrame* c) :
			CachedOp(kOtherOpType), cmd(c)
	{
	}
	CachedCmdOp::~CachedCmdOp()
	{
		DELETE(cmd);
	}

	OpKey::OpKey(const std::string& k) :
			key(k)
	{

	}
	bool OpKey::operator<(const OpKey& other) const
	{
		return key < other.key;
	}

	OpLogs::OpLogs(ArdbServer* server) :
			m_server(server), m_min_seq(0), m_max_seq(0), m_op_log_file(NULL), m_current_oplog_record_size(
					0), m_last_flush_time(0)
	{

	}

	OpLogs::~OpLogs()
	{
		CachedOpTable::iterator it = m_mem_op_logs.begin();
		while (it != m_mem_op_logs.end())
		{
			CachedOp* op = it->second;
			DELETE(op);
			it++;
		}
	}

	void OpLogs::FillCacheValue()
	{
		//load value in db for '__set__' cmd
		CachedOpTable::iterator it = m_mem_op_logs.begin();
		while (it != m_mem_op_logs.end())
		{
			CachedOp* op = it->second;
			if (op->type == kSetOpType)
			{
				CachedWriteOp* wop = (CachedWriteOp*) op;
				std::string* v = new std::string;
				if (0 == m_server->m_db->RawGet(wop->key.key, v))
				{
					wop->v = v;
				} else
				{
					DELETE(v);
				}
			}
			it++;
		}
	}

	void OpLogs::LoadCachedOpLog(Buffer & buf)
	{
		uint32 mark = buf.GetReadIndex();
		while (true)
		{
			mark = buf.GetReadIndex();
			uint64 seq = 0;
			if (!BufferHelper::ReadVarUInt64(buf, seq))
			{
				break;
			}
			uint8 type;
			if (!BufferHelper::ReadFixUInt8(buf, type))
			{
				break;
			}
			if (type == kSetOpType || type == kDelOpType)
			{
				std::string key;
				if (!BufferHelper::ReadVarString(buf, key))
				{
					break;
				}
				OpKey ok(key);
				SaveWriteOp(ok, type, false, NULL, true, seq);
			} else
			{
				uint32 size;
				if (!BufferHelper::ReadVarUInt32(buf, size))
				{
					break;
				}
				ArgumentArray strs;
				for (uint32 i = 0; i < size; i++)
				{
					std::string str;
					if (!BufferHelper::ReadVarString(buf, str))
					{
						goto ret;
					}
					strs.push_back(str);
				}
				RedisCommandFrame* cmd = new RedisCommandFrame(strs);
				SaveCmdOp(cmd, false, true, seq);
			}
			if (m_current_oplog_record_size == 0)
			{
				m_min_seq = seq;
			}
			m_current_oplog_record_size++;
		}
		ret: buf.SetReadIndex(mark);
	}

	void OpLogs::LoadCachedOpLog(const std::string& file)
	{
		FILE* cacheOpFile = fopen(file.c_str(), "rb");
		if (NULL == cacheOpFile)
		{
			ERROR_LOG("Failed to open oplog file:%s", file.c_str());
			return;
		}
		Buffer buffer;
		while (true)
		{
			buffer.EnsureWritableBytes(20 * 1024 * 1024);
			char* tmpbuf = const_cast<char*>(buffer.GetRawWriteBuffer());
			uint32 tmpbufsize = buffer.WriteableBytes();
			size_t ret = fread(tmpbuf, 1, tmpbufsize, cacheOpFile);
			buffer.AdvanceWriteIndex(ret);
			LoadCachedOpLog(buffer);
			buffer.DiscardReadedBytes();
			if (ret < tmpbufsize)
			{
				break;
			}
		}
		if (buffer.ReadableBytes() > 0)
		{
			ERROR_LOG("Corrupted oplog file.");
		}
		if (NULL != cacheOpFile)
		{
			fclose(cacheOpFile);
		}
	}

	void OpLogs::Load()
	{
		INFO_LOG("Start loading oplogs.");
		Buffer keybuf;
		std::string serverkey_path = m_server->GetServerConfig().repl_data_dir
				+ "/repl.key";
		if (file_read_full(serverkey_path, keybuf) == 0)
		{
			m_server_key = keybuf.AsString();
		} else
		{
			m_server_key = random_string(16);
			file_write_content(serverkey_path, m_server_key);
		}
		INFO_LOG("Server replication key is %s", m_server_key.c_str());
		uint64 start = get_current_epoch_millis();
		std::string filename = m_server->GetServerConfig().repl_data_dir
				+ "/repl.oplog.1";
		if (is_file_exist(filename))
		{
			LoadCachedOpLog(filename);
		}
		m_current_oplog_record_size = 0;
		filename = m_server->GetServerConfig().repl_data_dir + "/repl.oplog";
		if (is_file_exist(filename))
		{
			LoadCachedOpLog(filename);
		}
		//FillCacheValue();
		uint64 end = get_current_epoch_millis();
		ReOpenOpLog();
		INFO_LOG(
				"Cost %llums to load all oplogs, min seq = %llu, max seq = %llu", (end- start), m_min_seq, m_max_seq);
	}

	void OpLogs::Routine()
	{
		time_t now = time(NULL);
		if (now - m_last_flush_time >= 5)
		{
			FlushOpLog();
		}
	}

	void OpLogs::ReOpenOpLog()
	{
		if (NULL == m_op_log_file)
		{
			std::string oplog_file_path =
					m_server->GetServerConfig().repl_data_dir + "/repl.oplog";
			m_op_log_file = fopen(oplog_file_path.c_str(), "a+");
			if (NULL == m_op_log_file)
			{
				ERROR_LOG(
						"Failed to open oplog file:%s", oplog_file_path.c_str());
			}
		}
	}

	void OpLogs::RollbackOpLogs()
	{
		if (NULL != m_op_log_file)
		{
			fclose(m_op_log_file);
			m_op_log_file = NULL;
		}
		std::string oplog_file_path = m_server->GetServerConfig().repl_data_dir
				+ "/repl.oplog";
		std::stringstream oldest_file(
				std::stringstream::in | std::stringstream::out);
		oldest_file << oplog_file_path << "." << k_max_rolling_index;
		remove(oldest_file.str().c_str());

		for (int i = k_max_rolling_index - 1; i >= 1; --i)
		{
			std::stringstream source_oss(
					std::stringstream::in | std::stringstream::out);
			std::stringstream target_oss(
					std::stringstream::in | std::stringstream::out);

			source_oss << oplog_file_path << "." << i;
			target_oss << oplog_file_path << "." << (i + 1);
			if (is_file_exist(source_oss.str()))
			{
				remove(target_oss.str().c_str());
				rename(source_oss.str().c_str(), target_oss.str().c_str());
			}
			//loglog_renaming_result(*loglog, source, target, ret);
		}
		std::stringstream ss(std::stringstream::in | std::stringstream::out);
		ss << oplog_file_path << ".1";
		std::string path = ss.str();
		rename(oplog_file_path.c_str(), path.c_str());
		ReOpenOpLog();
	}

	void OpLogs::FlushOpLog()
	{
		if (m_op_log_buffer.Readable() && NULL != m_op_log_file)
		{
			fwrite(m_op_log_buffer.GetRawReadBuffer(), 1,
					m_op_log_buffer.ReadableBytes(), m_op_log_file);
			fflush(m_op_log_file);
			m_op_log_buffer.Clear();
			if (m_current_oplog_record_size
					>= m_server->GetServerConfig().repl_backlog_size)
			{
				//rollback op logs
				RollbackOpLogs();
				m_current_oplog_record_size = 0;
			}
		}
	}

	void OpLogs::WriteCachedOp(uint64 seq, CachedOp* op)
	{
		Buffer tmp;
		BufferHelper::WriteVarUInt64(tmp, seq);
		uint8 optype = op->type;
		tmp.WriteByte(optype);
		if (optype == kSetOpType || optype == kDelOpType)
		{
			CachedWriteOp* writeOp = (CachedWriteOp*) op;
			/*
			 * Only key would be persisted
			 */
			BufferHelper::WriteVarString(tmp, writeOp->key.key);
		} else
		{
			CachedCmdOp* cmdOp = (CachedCmdOp*) op;
			BufferHelper::WriteVarUInt32(tmp,
					cmdOp->cmd->GetArguments().size() + 1);
			BufferHelper::WriteVarString(tmp, cmdOp->cmd->GetCommand());
			for (uint32 i = 0; i < cmdOp->cmd->GetArguments().size(); i++)
			{
				BufferHelper::WriteVarString(tmp,
						*(cmdOp->cmd->GetArgument(i)));
			}
		}
		m_op_log_buffer.Write(&tmp, tmp.ReadableBytes());
		m_current_oplog_record_size++;
		if (m_op_log_buffer.ReadableBytes() >= 5 * 1024 * 1024)
		{
			FlushOpLog();
		}
	}

	void OpLogs::RemoveOldestOp()
	{
		while (m_min_seq < m_max_seq)
		{
			CachedOpTable::iterator found = m_mem_op_logs.find(m_min_seq++);
			if (found != m_mem_op_logs.end())
			{
				if (found->second->type != kOtherOpType)
				{
					CachedWriteOp* op = (CachedWriteOp*) found->second;
					m_mem_op_idx.erase(op->key);
				}
				DELETE(found->second);
				m_mem_op_logs.erase(found);
				break;
			}
		}
	}

	CachedOp* OpLogs::SaveCmdOp(RedisCommandFrame* cmd, bool writeOpLog,
			bool with_seq, uint64 seq)
	{
		if (!with_seq)
		{
			seq = (++m_max_seq);
		} else
		{
			if (seq > m_max_seq)
			{
				m_max_seq = seq;
			}
		}
		RemoveExistOp(seq);
		CachedCmdOp* op = new CachedCmdOp(cmd);
		m_mem_op_logs[seq] = op;
		while (m_mem_op_logs.size()
				>= m_server->GetServerConfig().repl_backlog_size)
		{
			RemoveOldestOp();
		}
		if (writeOpLog)
		{
			WriteCachedOp(seq, op);
		}
		return op;
	}

	CachedOp* OpLogs::SaveWriteOp(OpKey& opkey, uint8 type, bool writeOpLog,
			std::string* v, bool with_seq, uint64 seq)
	{
		RemoveExistOp(opkey);
		if (!with_seq)
		{
			seq = (++m_max_seq);
		} else
		{
			if (seq > m_max_seq)
			{
				m_max_seq = seq;
			}
		}

		CachedWriteOp* op = new CachedWriteOp(type, opkey);
		if (type == kSetOpType && NULL != v)
		{
			op->v = v;
		}

		RemoveExistOp(seq);
		m_mem_op_logs[seq] = op;
		m_mem_op_idx[opkey] = seq;
		while (m_mem_op_logs.size()
				>= m_server->GetServerConfig().repl_backlog_size)
		{
			RemoveOldestOp();
		}
		if (writeOpLog)
		{
			WriteCachedOp(seq, op);
		}
		return op;
	}

	int OpLogs::LoadOpLog(DBIDSet& dbs, uint64& seq, Buffer& buf,
			bool is_master_slave)
	{
		if (seq < m_min_seq || seq > m_max_seq)
		{
			DEBUG_LOG("Expect seq:%llu, current seq:%llu", seq, m_max_seq);
			return -1;
		}
		while (seq < m_max_seq)
		{
			uint64 current_seq = seq;
			char seqbuf[256];
			sprintf(seqbuf, "%llu", current_seq);
			seq++;
			CachedOpTable::iterator fit = m_mem_op_logs.find(current_seq);
			if (fit != m_mem_op_logs.end())
			{
				CachedOp* op = fit->second;
				if (is_master_slave && op->from_master)
				{
					continue;
				}
				if (op->type != kOtherOpType)
				{
					CachedWriteOp* writeOp = (CachedWriteOp*) op;
					if (!dbs.empty())
					{
						DBID dbid;
						KeyType keytype;
						peek_dbkey_header(writeOp->key.key, dbid, keytype);
						if (dbs.count(dbid) == 0)
						{
							continue;
						}
					}
					ArgumentArray strs;
					strs.push_back(
							op->type == kSetOpType ? "__set__" : "__del__");
					strs.push_back(writeOp->key.key);
					if (op->type == kSetOpType)
					{
						/*
						 * Load value if not exist
						 */
						if (NULL == writeOp->v)
						{
							std::string* v = new std::string;
							if (0
									== m_server->m_db->RawGet(writeOp->key.key,
											v))
							{
								writeOp->v = v;
							} else
							{
								DELETE(v);
							}
						}
						if (NULL != writeOp->v)
						{
							strs.push_back(*(writeOp->v));
						} else
						{
							continue;
						}
					}
					//push seq at last
					strs.push_back(seqbuf);
					RedisCommandFrame cmd(strs);
					RedisCommandEncoder::Encode(buf, cmd);
				} else
				{
					CachedCmdOp* cmdop = (CachedCmdOp*) op;
					//push seq at last
					cmdop->cmd->GetArguments().push_back(seqbuf);
					RedisCommandEncoder::Encode(buf, *(cmdop->cmd));
				}
				return 1;
			}
		}
		return 0;
	}

	void OpLogs::RemoveExistOp(uint64 seq)
	{
		CachedOpTable::iterator fit = m_mem_op_logs.find(seq);
		if (fit != m_mem_op_logs.end())
		{
			CachedOp* op = fit->second;
			DELETE(op);
			m_mem_op_logs.erase(fit);
		}
	}

	void OpLogs::RemoveExistOp(OpKey& key)
	{
		OpKeyIndexTable::iterator found = m_mem_op_idx.find(key);
		if (found != m_mem_op_idx.end())
		{
			uint64 seq = found->second;
			CachedOpTable::iterator fit = m_mem_op_logs.find(seq);
			if (fit != m_mem_op_logs.end())
			{
				CachedOp* op = fit->second;
				DELETE(op);
				m_mem_op_logs.erase(fit);
				m_mem_op_idx.erase(found);
			}
		}
	}

	CachedOp* OpLogs::SaveSetOp(const std::string& key, std::string* value)
	{
		OpKey ok(key);
		return SaveWriteOp(ok, kSetOpType, true, value, false, 0);
	}
	CachedOp* OpLogs::SaveDeleteOp(const std::string& key)
	{
		OpKey ok(key);
		return SaveWriteOp(ok, kDelOpType, true, NULL, false, 0);

	}
	CachedOp* OpLogs::SaveFlushOp(const DBID& db)
	{
		ArgumentArray strs;
		strs.push_back("flushdb");
		return SaveCmdOp(new RedisCommandFrame(strs), true, false, 0);
	}

	bool OpLogs::VerifyClient(const std::string& serverKey, uint64 seq)
	{
		if (m_server_key == serverKey && seq >= m_min_seq && seq <= m_max_seq)
		{
			while (seq <= m_max_seq)
			{
				CachedOpTable::iterator it = m_mem_op_logs.find(seq);
				if (it != m_mem_op_logs.end())
				{
					return true;
				} else
				{
					seq++;
				}
			}
		}
		return false;
	}
}

