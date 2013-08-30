/*
 * backlog.cpp
 *
 *  Created on: 2013年8月22日
 *      Author: wqy
 */
#include "repl_backlog.hpp"
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>

#define ARDB_SHM_PATH "/ardb/repl"
#define ARDB_MAX_MMAP_UNIT_SIZE  100*1024*1024
#define ARDB_MASTER_SYNC_STATE_MMAP_FILE_SIZE 1024
#define SERVER_KEY_SIZE 40

namespace ardb
{

	ReplBacklog::ReplBacklog() :
			m_backlog_size(0), m_backlog_idx(0), m_master_repl_offset(0), m_repl_backlog_histlen(
					0), m_repl_backlog_offset(0), m_sync_state_change(false)
	{

	}

	void ReplBacklog::Run()
	{

	}

	bool ReplBacklog::LoadSyncState(const std::string& path)
	{
		std::string file = path + "/master_sync.state";
		if (0 != m_sync_state_buf.Init(file,
		ARDB_MASTER_SYNC_STATE_MMAP_FILE_SIZE))
		{
			return false;
		}
		if (*(m_sync_state_buf.m_buf))
		{
			char serverkeybuf[SERVER_KEY_SIZE + 1];
			sscanf(m_sync_state_buf.m_buf, "%s %llu %llu %llu %llu",
					serverkeybuf, &m_master_repl_offset, &m_repl_backlog_offset,
					&m_backlog_idx, &m_repl_backlog_histlen);
			m_server_key = serverkeybuf;
		} else
		{
			m_server_key = random_hex_string(SERVER_KEY_SIZE);
			m_master_repl_offset++;

			/* We don't have any data inside our buffer, but virtually the first
			 * byte we have is the next byte that will be generated for the
			 * replication stream. */
			m_repl_backlog_offset = m_master_repl_offset + 1;
			PersistSyncState();
		}
		INFO_LOG(
				"[Master]Server key:%s, master_repl_offset:%llu, repl_backlog_offset:%llu, backlog_idx:%llu, repl_backlog_histlen=%llu",
				m_server_key.c_str(), m_master_repl_offset,
				m_repl_backlog_offset, m_backlog_idx, m_repl_backlog_histlen);
		return true;
	}

	void ReplBacklog::PersistSyncState()
	{
		int len = snprintf(m_sync_state_buf.m_buf, m_sync_state_buf.m_size,
				"%s %llu %llu %llu %llu", m_server_key.c_str(),
				m_master_repl_offset, m_repl_backlog_offset, m_backlog_idx,
				m_repl_backlog_histlen);
		msync(m_sync_state_buf.m_buf, len, MS_ASYNC);
		m_sync_state_change = false;
	}

	int ReplBacklog::Init(const std::string& path, uint64 backlog_size)
	{
		if (backlog_size < 1024 * 1024)
		{
			ERROR_LOG("Replication backlog buffer size must greater than 1mb.");
			return -1;
		}
		if (!LoadSyncState(path))
		{
			return -1;
		}
		std::string file = path + "/master_repl_mmap.log";
		if (0 != m_backlog.Init(file, backlog_size))
		{
			return -1;
		}
		m_backlog_size = backlog_size;
		m_backlog_idx = 0;

		return 0;
	}

	void ReplBacklog::Feed(RedisCommandFrame& cmd)
	{
		Buffer buffer(256);
		RedisCommandEncoder::Encode(buffer, cmd);
		const char* buf = buffer.GetRawReadBuffer();
		size_t len = buffer.ReadableBytes();
		m_master_repl_offset += len;
		/* This is a circular buffer, so write as much data we can at every
		 * iteration and rewind the "idx" index if we reach the limit. */
		while (len)
		{
			size_t thislen = m_backlog_size - m_backlog_idx;
			if (thislen > len)
				thislen = len;
			memcpy(m_backlog.m_buf + m_backlog_idx, buf, thislen);
			m_backlog_idx += thislen;
			if (m_backlog_idx == m_backlog_size)
				m_backlog_idx = 0;
			len -= thislen;
			buf += thislen;
			m_repl_backlog_histlen += thislen;
		}
		if (m_repl_backlog_histlen > m_backlog_size)
			m_repl_backlog_histlen = m_backlog_size;
		/* Set the offset of the first byte we have in the backlog. */
		m_repl_backlog_offset = m_master_repl_offset - m_repl_backlog_histlen
				+ 1;
		m_sync_state_change = true;
	}

	const std::string& ReplBacklog::GetServerKey()
	{
		return m_server_key;
	}
	uint64 ReplBacklog::GetReplEndOffset()
	{
		return m_master_repl_offset;
	}

	uint64 ReplBacklog::GetReplStartOffset()
	{
		return m_repl_backlog_offset;
	}

	size_t ReplBacklog::WriteChannel(Channel* channle, int64 offset, size_t len)
	{
		if (!IsValidOffset(offset))
		{
			return 0;
		}
		int64 skip = offset - m_repl_backlog_offset;
		uint64 j = (m_backlog_idx + (m_backlog_size - m_repl_backlog_histlen))
				% m_backlog_size;
		j = (j + skip) % m_backlog_size;
		uint64 total = len;
		if (total > m_repl_backlog_histlen - skip)
		{
			total = m_repl_backlog_histlen - skip;
		}
		while (total)
		{
			uint64 thislen =
					((m_backlog_size - j) < len) ?
							(m_backlog_size - j) : len;
			Buffer buf(m_backlog.m_buf + j, 0, thislen);
			channle->Write(buf);
			total -= thislen;
			j = 0;
		}
		return (size_t)total;
	}

	bool ReplBacklog::IsValidOffset(int64 offset)
	{
		if (offset < 0 || (uint64)offset > m_master_repl_offset || (uint64)offset < m_repl_backlog_offset)
		{
			INFO_LOG(
					"Unable to partial resync with the slave for lack of backlog (Slave request was: %lld).",
					offset);
			return false;
		}
		return true;
	}

	bool ReplBacklog::IsValidOffset(const std::string& server_key, int64 offset)
	{
		if (m_server_key != server_key)
		{
			INFO_LOG("Partial resynchronization not accepted: "
					"Serverkey mismatch (Client asked for '%s', I'm '%s')",
					server_key.c_str(), m_server_key.c_str());
			return false;
		}
		return IsValidOffset(offset);
	}
}

