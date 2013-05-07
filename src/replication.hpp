/*
 * replication.hpp
 *
 *  Created on: 2013-4-22
 *      Author: wqy
 */

#ifndef REPLICATION_HPP_
#define REPLICATION_HPP_
#include "channel/all_includes.hpp"
#include "ardb.hpp"
#include "util/thread/thread.hpp"
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
			SlaveConn(Channel* c = NULL);
			SlaveConn(Channel* c, const std::string& key, uint64 seq);
			void WriteRedisCommand(RedisCommandFrame& cmd);
	};

	struct OpKey
	{
			DBID db;
			std::string key;
			OpKey(){}
			OpKey(const DBID& id, const std::string& k);
			bool operator<(const OpKey& other) const;
			bool Empty()
			{
				return db.empty();
			}
	};

	struct CachedOp
	{
			uint8 type;
			CachedOp(uint8 t) :
					type(t)
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
			size_t Size()
			{
				return sizeof(type) + sizeof(ptr);
			}
	};

	struct ReplInstructionDecoder: public ardb::codec::StackFrameDecoder<
			ReplInstruction>
	{
			bool Decode(ChannelHandlerContext& ctx, Channel* channel,
					Buffer& buffer, ReplInstruction& msg)
			{
				if (buffer.ReadableBytes() < msg.Size())
				{
					return false;
				}
				BufferHelper::ReadFixUInt8(buffer, msg.type);
				buffer.Read(&msg.ptr, sizeof(msg.ptr));
				return true;
			}
	};

	struct ReplInstructionEncoder: public ChannelDownstreamHandler<
			ReplInstruction>
	{
			bool WriteRequested(ChannelHandlerContext& ctx,
					MessageEvent<ReplInstruction>& e)
			{
				static Buffer buffer(e.GetMessage()->Size());
				buffer.Clear();
				BufferHelper::WriteFixUInt8(buffer, e.GetMessage()->type);
				buffer.Write(&(e.GetMessage()->ptr), sizeof(e.GetMessage()->ptr));
				return ctx.GetChannel()->Write(buffer);
			}
	};

	class Ardb;
	class ArdbServer;
	class ArdbServerConfig;

	class OpLogs
	{
		private:
			ArdbServer* m_server;
			uint64 m_min_seq;
			uint64 m_max_seq;
			FILE* m_op_log_file;
			Buffer m_op_log_buffer;
			uint32 m_current_oplog_record_size;
			time_t m_last_flush_time;

			std::string m_server_key;

			DBID m_current_db;
			typedef btree::btree_map<uint64, CachedOp*> CachedOpTable;
			typedef btree::btree_map<OpKey, uint64> OpKeyIndexTable;
			CachedOpTable m_mem_op_logs;
			OpKeyIndexTable m_mem_op_idx;

			void Routine();
			void LoadCachedOpLog(Buffer & buf);
			void LoadCachedOpLog(const std::string& file);

			void RemoveExistOp(OpKey& key);
			void RemoveOldestOp();
			void CheckCurrentDB(const DBID& db);
			void ReOpenOpLog();
			void RollbackOpLogs();
			void FlushOpLog();
			void WriteCachedOp(uint64 seq, CachedOp* op);
			uint64 SaveCmdOp(RedisCommandFrame* cmd, bool writeOpLog = true);
			uint64 SaveWriteOp(OpKey& opkey, uint8 type, bool writeOpLog = true,
					std::string* v = NULL);
		public:
			OpLogs(ArdbServer* server);
			void Load();
			int MemCacheSize()
			{
				return m_mem_op_logs.size();
			}
			int LoadOpLog(uint64& seq, Buffer& cmd);
			uint64 GetMaxSeq()
			{
				return m_max_seq;
			}
			uint64 GetMinSeq()
			{
				return m_min_seq;
			}
			void SaveSetOp(const DBID& db, const std::string& key,
					std::string* value);
			void SaveDeleteOp(const DBID& db, const std::string& key);
			void SaveFlushOp(const DBID& db);
			bool VerifyClient(const std::string& serverKey, uint64 seq);
			const std::string& GetServerKey()
			{
				return m_server_key;
			}
	};

	class SlaveClient: public ChannelUpstreamHandler<RedisCommandFrame>,
			public ChannelUpstreamHandler<Buffer>,
			public Runnable
	{
		private:
			ArdbServer* m_serv;
			Channel* m_client;
			SocketHostAddress m_master_addr;
			uint32 m_chunk_len;
			int m_slave_state;
			bool m_cron_inited;
			bool m_ping_recved;
			RedisCommandDecoder m_decoder;
			NullRedisReplyEncoder m_encoder;

			uint8 m_server_type;
			std::string m_server_key;
			uint64 m_sync_seq;

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
							0), m_server_key("-"), m_sync_seq(0)
			{
			}
			int ConnectMaster(const std::string& host, uint32 port);
			void Close();
			void Stop();
	};

	class ReplicationService: public Thread,
			public ChannelUpstreamHandler<Buffer>,
			public ChannelUpstreamHandler<ReplInstruction>,
			public RawKeyListener
	{
		private:
			ChannelService m_serv;
			ArdbServer* m_server;
			bool m_is_saving;
			uint32 m_last_save;

			void Run();
			typedef std::deque<SlaveConn> SyncClientQueue;
			typedef std::map<uint32, SlaveConn> SlaveConnTable;
			SyncClientQueue m_waiting_slaves;
			SlaveConnTable m_slaves;

			OpLogs m_oplogs;

			//Use pipe to transfer instructions and data
			PipeChannel* m_input_channel;
			PipeChannel* m_notify_channel;
			ReplInstructionEncoder m_inst_encoder;
			ReplInstructionDecoder m_inst_decoder;
			void Routine();
			void PingSlaves();
			void ChannelClosed(ChannelHandlerContext& ctx,
					ChannelStateEvent& e);
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<Buffer>& e)
			{

			}
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<ReplInstruction>& e);
			void CheckSlaveQueue();
			void FeedSlaves();
			void FullSync(SlaveConn& client);

			int OnKeyUpdated(const DBID& db, const Slice& key,
					const Slice& value);
			int OnKeyDeleted(const DBID& db, const Slice& key);
		public:
			ReplicationService(ArdbServer* serv);
			int Init();
			void ServSlaveClient(Channel* client);
			void ServARSlaveClient(Channel* client,
					const std::string& serverKey, uint64 seq);
			void RecordFlushDB(const DBID& db);
			void FireSyncEvent();
			OpLogs& GetOpLogs()
			{
				return m_oplogs;
			}
			int Save();
			int BGSave();
			uint32 LastSave()
			{
				return m_last_save;
			}
	};
}

#endif /* REPLICATION_HPP_ */
