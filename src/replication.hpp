 /*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 * 
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 * 
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef REPLICATION_HPP_
#define REPLICATION_HPP_
#include "channel/all_includes.hpp"
#include "ardb.hpp"
#include "util/thread/thread.hpp"
#include "util/concurrent_queue.hpp"
#include <stdio.h>
#include <btree_map.h>
#include <btree_set.h>

using namespace ardb::codec;

namespace ardb
{
	struct SlaveConn
	{
			Channel* conn;
			std::string server_key;
			uint64 synced_cmd_seq;
			uint32 state;
			uint8 type;
			DBIDSet syncdbs;
			SlaveConn(Channel* c = NULL);
			SlaveConn(Channel* c, const std::string& key, uint64 seq,
					DBIDSet& dbs);
			bool WriteRedisCommand(RedisCommandFrame& cmd);
	};

	struct OpKey
	{
			std::string key;
			OpKey()
			{
			}
			OpKey(const std::string& k);
			bool operator<(const OpKey& other) const;
	};

	struct CachedOp
	{
			uint8 type;
			bool from_master;
			CachedOp(uint8 t) :
					type(t), from_master(false)
			{
			}
			virtual ~CachedOp()
			{
			}
	};
	struct CachedWriteOp: public CachedOp
	{
			OpKey key;
			std::string* v;
			CachedWriteOp(uint8 t, OpKey& k);
			bool IsInDBSet(DBIDSet& dbs);
			~CachedWriteOp();
	};
	struct CachedCmdOp: public CachedOp
	{
			RedisCommandFrame* cmd;
			CachedCmdOp(RedisCommandFrame* c);
			~CachedCmdOp();
	};

	struct ReplInstruction
	{
			uint8 type;
			void* ptr;
			ReplInstruction(uint8 t = 0, void* p = NULL) :
					type(t), ptr(p)
			{
			}
	};

	class Ardb;
	class ArdbServer;
	class ArdbServerConfig;

	struct CachedOpVisitor
	{
			virtual int OnCachedOp(CachedOp* op, uint64 seq) = 0;
			virtual ~CachedOpVisitor()
			{
			}
	};

	class OpLogFile
	{
		private:
			int m_op_log_file_fd;
			std::string m_file_path;
			Buffer m_read_buffer;
			Buffer m_write_buffer;
			time_t m_last_flush_ts;
		public:
			OpLogFile(const std::string& path);
			int OpenWrite();
			int OpenRead();
			int Load(CachedOpVisitor* visitor, uint32 maxReadBytes = 4096);
			bool Write(Buffer& content);
			void Flush();
			time_t LastFlush()
			{
				return m_last_flush_ts;
			}
			~OpLogFile();
	};

	struct OpKeyPtrWrapper
	{
			OpKey* ptr;
			OpKeyPtrWrapper(OpKey& key):ptr(&key)
			{
			}
			OpKeyPtrWrapper():ptr(NULL){}
			bool operator<(const OpKeyPtrWrapper& other) const
			{
				return *ptr < *(other.ptr);
			}
	};

	class OpLogs
	{
		private:
			ArdbServer* m_server;
			uint64 m_min_seq;
			uint64 m_max_seq;
			OpLogFile* m_op_log_file;
			uint32 m_current_oplog_record_size;
			time_t m_last_flush_time;

			std::string m_server_key;

			typedef btree::btree_map<uint64, CachedOp*> CachedOpTable;
			typedef btree::btree_map<OpKeyPtrWrapper, uint64> OpKeyIndexTable;
			CachedOpTable m_mem_op_logs;
			OpKeyIndexTable m_mem_op_idx;

			void RemoveExistOp(OpKey& key);
			void RemoveExistOp(uint64 seq);
			void RemoveOldestOp();
			void ReOpenOpLog();
			void RollbackOpLogs();
			void WriteCachedOp(uint64 seq, CachedOp* op);
			CachedOp* SaveOp(CachedOp* op, bool persist, bool with_seq,
					uint64 seq);
		public:
			OpLogs(ArdbServer* server);
			~OpLogs();
			void Routine();
			void Load();
			int MemCacheSize()
			{
				return m_mem_op_logs.size();
			}
			int LoadOpLog(DBIDSet& dbs, uint64& seq, Buffer& cmd,
					bool is_master_slave);
			bool PeekOpLogStartSeq(uint32 log_index, uint64& seq);
			bool IsInDiskOpLogs(uint64 seq);
			bool PeekDBID(CachedOp* op, DBID& db);
			uint64 GetMaxSeq()
			{
				return m_max_seq;
			}
			uint64 GetMinSeq()
			{
				return m_min_seq;
			}
			std::string GetOpLogPath(uint32 index);
			CachedOp* SaveSetOp(const std::string& key, std::string* value);
			CachedOp* SaveDeleteOp(const std::string& key);
			bool CachedOp2RedisCommand(CachedOp* op, RedisCommandFrame& cmd,
					uint64 seq = 0);
			bool VerifyClient(const std::string& serverKey, uint64 seq);
			const std::string& GetServerKey()
			{
				return m_server_key;
			}
	};

	class ArdbConnContext;
	class SlaveClient: public ChannelUpstreamHandler<RedisCommandFrame>,
			public ChannelUpstreamHandler<Buffer>,
			public Runnable
	{
		private:
			ArdbServer* m_serv;
			Channel* m_client;
			SocketHostAddress m_master_addr;
			uint32 m_chunk_len;
			uint32 m_slave_state;
			bool m_cron_inited;
			bool m_ping_recved;
			RedisCommandDecoder m_decoder;
			NullRedisReplyEncoder m_encoder;

			uint8 m_server_type;
			std::string m_server_key;
			uint64 m_sync_seq;
			/*
			 * empty means all db
			 */
			DBIDSet m_sync_dbs;

			ArdbConnContext *m_actx;

			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<RedisCommandFrame>& e);
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<Buffer>& e);
			void ChannelClosed(ChannelHandlerContext& ctx,
					ChannelStateEvent& e);
			void ChannelConnected(ChannelHandlerContext& ctx,
					ChannelStateEvent& e);
			void Timeout();
			void Run();
			void PersistSyncState();
			void LoadSyncState();
		public:
			SlaveClient(ArdbServer* serv) :
					m_serv(serv), m_client(NULL), m_chunk_len(0), m_slave_state(
							0), m_cron_inited(false), m_ping_recved(false), m_server_type(
							0), m_server_key("-"), m_sync_seq(0), m_actx(NULL)
			{
			}
			const SocketHostAddress& GetMasterAddress()
			{
				return m_master_addr;
			}
			void SetSyncDBs(DBIDSet& dbs)
			{
				m_sync_dbs = dbs;
			}

			int ConnectMaster(const std::string& host, uint32 port);
			void Close();
			void Stop();
	};

	class ReplicationService;
	class LoadSyncTask: public Runnable
	{
		private:
			ReplicationService* m_repl;
			uint32 m_conn_id;
			Iterator* m_iter;
			OpLogFile* m_op_log;
			uint64 m_start_time;
			uint64 m_seq_after_sync_iter;
			uint8 m_state;
			DBIDSet m_syncing_dbs;
			void Run();
			void SyncDBIter();
			void SyncOpLogs();
		public:
			LoadSyncTask(ReplicationService* repl, uint32 id, uint8 state);
			~LoadSyncTask();
	};

	class ReplicationService: public Thread,
			public SoftSignalHandler,
			public ChannelUpstreamHandler<Buffer>,
			public RawKeyListener
	{
		private:
			ChannelService m_serv;
			ArdbServer* m_server;
			volatile bool m_is_saving;
			uint32 m_last_save;

			void Run();
			typedef std::deque<SlaveConn> SyncClientQueue;
			typedef std::map<uint32, SlaveConn> SlaveConnTable;
			SyncClientQueue m_waiting_slaves;
			SlaveConnTable m_slaves;

			OpLogs m_oplogs;

			SoftSignalChannel* m_inst_signal;
			MPSCQueue<ReplInstruction> m_inst_queue;

			//connection id for the connection that is master-slave
			uint32 m_master_slave_id;
			void Routine();
			void PingSlaves();
			void ChannelClosed(ChannelHandlerContext& ctx,
					ChannelStateEvent& e);
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<Buffer>& e)
			{

			}
			void OnSoftSignal(uint32 soft_signo, uint32 appendinfo);
			void ProcessInstructions();
			void CheckSlaveQueue();
			void FeedSlaves();
			void LoadSync(SlaveConn& client);

			int OnKeyUpdated(const Slice& key, const Slice& value);
			int OnKeyDeleted(const Slice& key);
			void OfferInstruction(ReplInstruction& inst);

			void OnLoadSynced(LoadSyncTask* task, bool success);
			SlaveConn* GetSlaveConn(uint32 id);
			friend class LoadSyncTask;
		public:
			ReplicationService(ArdbServer* serv);
			int Init();
			void ServSlaveClient(Channel* client);
			void ServARSlaveClient(Channel* client,
					const std::string& serverKey, uint64 seq, DBIDSet& dbs);
			void RecordFlushDB(const DBID& db);
			OpLogs& GetOpLogs()
			{
				return m_oplogs;
			}
			Ardb& GetDB();
			ArdbServerConfig& GetConfig();
			ChannelService& GetChannelService()
			{
				return m_serv;
			}
			void MarkMasterSlave(Channel* conn)
			{
				m_master_slave_id = conn->GetID();
			}
			int Save();
			int BGSave();
			bool IsSavingData()
			{
				return m_is_saving;
			}
			uint32 LastSave()
			{
				return m_last_save;
			}
			void Stop();
			~ReplicationService();
	};
}

#endif /* REPLICATION_HPP_ */
