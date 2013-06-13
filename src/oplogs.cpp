/*
 * oplog.cpp
 *
 *  Created on: 2013-5-1
 *      Author: wqy
 */
#include "replication.hpp"
#include "ardb_server.hpp"
#include "util/file_helper.hpp"
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <sstream>

#define OPLOGS_VER 1

namespace ardb
{
	static const uint8 kSetOpType = 1;
	static const uint8 kDelOpType = 2;
	static const uint8 kOtherOpType = 3;

	CachedWriteOp::CachedWriteOp(uint8 t, OpKey& k) :
			CachedOp(t), key(k), v(NULL)
	{
	}
	bool CachedWriteOp::IsInDBSet(DBIDSet& dbs)
	{
		DBID dbid;
		KeyType keytype;
		peek_dbkey_header(key.key, dbid, keytype);
		if (dbs.count(dbid) == 0)
		{
			return false;
		}
		return true;
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

	static const uint32 kOpLogFlushTrigger = 1 * 1024 * 1024;
	OpLogFile::OpLogFile(const std::string& path) :
			m_op_log_file_fd(-1), m_file_path(path), m_last_flush_ts(0)
	{
	}

	int OpLogFile::OpenWrite()
	{
		int mod = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
		m_op_log_file_fd = ::open(m_file_path.c_str(),
				O_RDWR | O_CREAT | O_APPEND, mod);
		if (m_op_log_file_fd > 0)
		{
			m_write_buffer.EnsureWritableBytes(kOpLogFlushTrigger);
		}
		return m_op_log_file_fd;
	}

	int OpLogFile::OpenRead()
	{
		m_op_log_file_fd = ::open(m_file_path.c_str(), O_RDONLY);
		return m_op_log_file_fd;
	}

	int OpLogFile::Load(CachedOpVisitor* visitor, uint32 maxReadBytes)
	{
		if (m_op_log_file_fd == -1)
		{
			return -1;
		}
		if (m_read_buffer.ReadableBytes() < maxReadBytes)
		{
			m_read_buffer.EnsureWritableBytes(
					maxReadBytes - m_read_buffer.ReadableBytes());
			char* tmpbuf = const_cast<char*>(m_read_buffer.GetRawWriteBuffer());
			uint32 tmpbufsize = m_read_buffer.WriteableBytes();
			size_t ret = ::read(m_op_log_file_fd, tmpbuf, tmpbufsize);
			if (ret <= 0)
			{
				return -1;
			}
			m_read_buffer.AdvanceWriteIndex(ret);
		}

		uint32 mark = m_read_buffer.GetReadIndex();
		while (true)
		{
			mark = m_read_buffer.GetReadIndex();
			uint64 seq = 0;
			if (!BufferHelper::ReadVarUInt64(m_read_buffer, seq))
			{
				break;
			}
			uint8 type;
			if (!BufferHelper::ReadFixUInt8(m_read_buffer, type))
			{
				break;
			}
			if (type == kSetOpType || type == kDelOpType)
			{
				std::string key;
				if (!BufferHelper::ReadVarString(m_read_buffer, key))
				{
					break;
				}
				OpKey ok(key);
				CachedWriteOp* op = new CachedWriteOp(type, ok);
				visitor->OnCachedOp(op, seq);
			} else if (type == kOtherOpType)
			{
				uint32 size;
				if (!BufferHelper::ReadVarUInt32(m_read_buffer, size))
				{
					break;
				}
				ArgumentArray strs;
				for (uint32 i = 0; i < size; i++)
				{
					std::string str;
					if (!BufferHelper::ReadVarString(m_read_buffer, str))
					{
						goto ret;
					}
					strs.push_back(str);
				}
				RedisCommandFrame* cmd = new RedisCommandFrame(strs);
				CachedCmdOp* op = new CachedCmdOp(cmd);
				visitor->OnCachedOp(op, seq);
			} else
			{
				ERROR_LOG(
						"Faild to read file:%u %llu at %u for %s", type, seq, m_read_buffer.GetReadIndex(), m_file_path.c_str());
				return -1;
			}
		}
		ret: m_read_buffer.SetReadIndex(mark);
		m_read_buffer.DiscardReadedBytes();
		return 0;
	}

	bool OpLogFile::Write(Buffer& content)
	{
		if (m_op_log_file_fd < 0)
		{
			return false;
		}
		m_write_buffer.Write(&content, content.ReadableBytes());
		if (m_write_buffer.ReadableBytes() >= kOpLogFlushTrigger)
		{
			Flush();
		}
		return true;
	}

	void OpLogFile::Flush()
	{
		while (m_write_buffer.Readable() && m_op_log_file_fd > 0)
		{
			int err = 0;
			//DEBUG_LOG("Flush %u bytes",m_write_buffer.ReadableBytes() );
			m_write_buffer.WriteFD(m_op_log_file_fd, err);
			if (err != 0)
			{
				ERROR_LOG("Write oplog failed:%s", strerror(err));
			}
			if (m_write_buffer.Readable())
			{
				m_write_buffer.DiscardReadedBytes();
			} else
			{
				m_write_buffer.Clear();
			}
			m_last_flush_ts = time(NULL);
		}
	}

	OpLogFile::~OpLogFile()
	{
		if (m_op_log_file_fd > 0)
		{
			::close(m_op_log_file_fd);
		}
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
		DELETE(m_op_log_file);
	}

	bool OpLogs::PeekDBID(CachedOp* op, DBID& db)
	{
		if (op->type == kSetOpType || op->type == kDelOpType)
		{
			CachedWriteOp* wop = (CachedWriteOp*) op;
			KeyType keytype;
			peek_dbkey_header(wop->key.key, db, keytype);
			return true;
		} else
		{
			return false;
		}
	}

	bool OpLogs::IsInDiskOpLogs(uint64 seq)
	{
		uint64 start_seq = 0;
		for (uint32 i = m_server->m_cfg.repl_max_backup_logs; i > 0; i--)
		{
			if (PeekOpLogStartSeq(i, start_seq))
			{
				if (start_seq > seq)
				{
					return false;
				}
				return true;
			}
		}
		return false;
	}

	bool OpLogs::PeekOpLogStartSeq(uint32 log_index, uint64& seq)
	{
		if (!is_file_exist(GetOpLogPath(log_index)))
		{
			return false;
		}
		FILE* cacheOpFile = fopen(GetOpLogPath(log_index).c_str(), "rb");
		Buffer buffer;
		buffer.EnsureWritableBytes(512);
		char* tmpbuf = const_cast<char*>(buffer.GetRawWriteBuffer());
		uint32 tmpbufsize = buffer.WriteableBytes();
		size_t ret = fread(tmpbuf, 1, tmpbufsize, cacheOpFile);
		buffer.AdvanceWriteIndex(ret);
		fclose(cacheOpFile);
		return BufferHelper::ReadVarUInt64(buffer, seq);
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
		struct OpLogVisitor: public CachedOpVisitor
		{
				OpLogs* oplogs;
				bool first;
				OpLogVisitor(OpLogs* ops) :
						oplogs(ops), first(true)
				{
				}
				int OnCachedOp(CachedOp* op, uint64 seq)
				{
					if (first)
					{
						oplogs->m_min_seq = seq;
						first = false;
					}
					oplogs->SaveOp(op, false, true, seq);
					oplogs->m_current_oplog_record_size++;
					return 0;
				}
		} visitor(this);

		uint64 start = get_current_epoch_millis();
		if (is_file_exist(GetOpLogPath(1)))
		{
			OpLogFile oplog(GetOpLogPath(1));
			if (oplog.OpenRead() > 0)
			{
				while (oplog.Load(&visitor, 2 * 1024 * 1024) == 0)
					;
			}
		}
		m_current_oplog_record_size = 0;
		if (is_file_exist(GetOpLogPath(0)))
		{
			OpLogFile oplog(GetOpLogPath(0));
			if (oplog.OpenRead() > 0)
			{
				while (oplog.Load(&visitor, 2 * 1024 * 1024) == 0)
					;
			}
		}
		uint64 end = get_current_epoch_millis();
		ReOpenOpLog();
		INFO_LOG(
				"Cost %llums to load all oplogs, min seq = %llu, max seq = %llu", (end- start), m_min_seq, m_max_seq);
	}

	void OpLogs::Routine()
	{
		time_t now = time(NULL);
		if (now - m_op_log_file->LastFlush() >= 5)
		{
			//DEBUG_LOG("Flush oplog.");
			m_op_log_file->Flush();
		}
	}

	void OpLogs::ReOpenOpLog()
	{
		if (NULL == m_op_log_file)
		{
			std::string oplog_file_path =
					m_server->GetServerConfig().repl_data_dir + "/repl.oplog";
			m_op_log_file = new OpLogFile(oplog_file_path);
			if (m_op_log_file->OpenWrite() < 0)
			{
				ERROR_LOG(
						"Failed to open oplog file:%s", oplog_file_path.c_str());
				DELETE(m_op_log_file);
			}
		}
	}

	void OpLogs::RollbackOpLogs()
	{
		m_op_log_file->Flush();
		DELETE(m_op_log_file);
		for (int i = m_server->m_cfg.repl_max_backup_logs - 1; i >= 0; --i)
		{
			if (is_file_exist(GetOpLogPath(i).c_str()))
			{
				remove(GetOpLogPath(i + 1).c_str());
				rename(GetOpLogPath(i).c_str(), GetOpLogPath(i + 1).c_str());
			}
		}
		ReOpenOpLog();
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
		m_op_log_file->Write(tmp);
		m_current_oplog_record_size++;
		if (m_current_oplog_record_size
				>= m_server->GetServerConfig().repl_backlog_size)
		{
			//rollback op logs
			RollbackOpLogs();
			m_current_oplog_record_size = 0;
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
					m_mem_op_idx.erase(OpKeyPtrWrapper(op->key));
				}
				DELETE(found->second);
				m_mem_op_logs.erase(found);
				break;
			}
		}
	}

	CachedOp* OpLogs::SaveOp(CachedOp* op, bool persist, bool with_seq,
			uint64 seq)
	{
		if (op->type == kSetOpType || op->type == kDelOpType)
		{
			CachedWriteOp* wop = (CachedWriteOp*) op;
			RemoveExistOp(wop->key);
		}
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
		if (m_min_seq == 0)
		{
			m_min_seq = seq;
		}
		RemoveExistOp(seq);
		m_mem_op_logs[seq] = op;
		if (op->type == kSetOpType || op->type == kDelOpType)
		{
			CachedWriteOp* wop = (CachedWriteOp*) op;
			m_mem_op_idx[OpKeyPtrWrapper(wop->key)] = seq;
		}

		while (m_mem_op_logs.size()
				> m_server->GetServerConfig().repl_backlog_size)
		{
			RemoveOldestOp();
		}
		if (persist)
		{
			DEBUG_LOG("Save record:%llu", seq);
			WriteCachedOp(seq, op);
		}
		return op;
	}

	bool OpLogs::CachedOp2RedisCommand(CachedOp* op, RedisCommandFrame& cmd,
			uint64 seq)
	{
		char seqbuf[1024];
		if (seq > 0)
		{
			sprintf(seqbuf, "%"PRIu64, seq);
		}
		if (op->type != kOtherOpType)
		{
			CachedWriteOp* writeOp = (CachedWriteOp*) op;
			ArgumentArray strs;
			strs.push_back(op->type == kSetOpType ? "__set__" : "__del__");
			strs.push_back(writeOp->key.key);
			if (op->type == kSetOpType)
			{
				/*
				 * Load value if not exist
				 */
				if (NULL == writeOp->v)
				{
					std::string* v = new std::string;
					if (0 == m_server->m_db->RawGet(writeOp->key.key, v))
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
					return false;
				}
			}
			if (seq > 0)
			{
				//push seq at last
				strs.push_back(seqbuf);
			}
			RedisCommandFrame ncmd(strs);
			cmd = ncmd;
		} else
		{
			CachedCmdOp* cmdop = (CachedCmdOp*) op;
			if (seq > 0)
			{
				//push seq at last
				cmdop->cmd->GetArguments().push_back(seqbuf);
			}
			cmd = *(cmdop->cmd);
		}
		return true;
	}

	int OpLogs::LoadOpLog(DBIDSet& dbs, uint64& seq, Buffer& buf,
			bool is_master_slave)
	{
		if ((seq + 1) < m_min_seq || (seq + 1) > m_max_seq)
		{
			DEBUG_LOG(
					"Expect seq:%"PRIu64", current seq:%"PRIu64, (seq + 1), m_max_seq);
			return -1;
		}
		while (seq < m_max_seq)
		{
			uint64 current_seq = seq + 1;
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
						if (!writeOp->IsInDBSet(dbs))
						{
							continue;
						}
					}
				}
				RedisCommandFrame cmd;
				if (CachedOp2RedisCommand(op, cmd, current_seq))
				{
					DEBUG_LOG("Feed slave:%"PRIu64, current_seq);
					RedisCommandEncoder::Encode(buf, cmd);
				} else
				{
					continue;
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
		OpKeyIndexTable::iterator found = m_mem_op_idx.find(
				OpKeyPtrWrapper(key));
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
		CachedWriteOp* op = new CachedWriteOp(kSetOpType, ok);
		op->v = value;
		return SaveOp(op, true, false, 0);
	}
	CachedOp* OpLogs::SaveDeleteOp(const std::string& key)
	{
		OpKey ok(key);
		CachedWriteOp* op = new CachedWriteOp(kDelOpType, ok);
		return SaveOp(op, true, false, 0);
	}

	std::string OpLogs::GetOpLogPath(uint32 index)
	{
		std::string oplog_file_path = m_server->GetServerConfig().repl_data_dir
				+ "/repl.oplog";
		if (index > 0)
		{
			char temp[oplog_file_path.size() + 100];
			sprintf(temp, "%s.%u", oplog_file_path.c_str(), index);
			return temp;
		}
		return oplog_file_path;
	}

	bool OpLogs::VerifyClient(const std::string& serverKey, uint64 seq)
	{
		INFO_LOG(
				"Verify Slave request %s:%"PRIu64", while current server is %s[%"PRIu64"-%"PRIu64"]", serverKey.c_str(), seq, m_server_key.c_str(), m_min_seq, m_max_seq);
		if (m_server_key == serverKey && (seq + 1) >= m_min_seq
				&& seq <= (m_max_seq + 1))
		{
			if (seq == m_max_seq + 1)
			{
				//already synced
				return true;
			}
			while (seq <= m_max_seq)
			{
				CachedOpTable::iterator it = m_mem_op_logs.find(seq++);
				if (it != m_mem_op_logs.end())
				{
					return true;
				} else
				{
					seq++;
				}
			}
		}
		INFO_LOG(
				"Slave request %s:%"PRIu64", while current server is %s[%"PRIu64"-%"PRIu64"]", serverKey.c_str(), seq, m_server_key.c_str(), m_min_seq, m_max_seq);
		return false;
	}
}

