/*
 * backlog.cpp
 *
 *  Created on: 2013-08-29      Author: wqy
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
			m_backlog_size(0), m_backlog_idx(0), m_end_offset(0), m_histlen(0), m_begin_offset(
			        0), m_last_sync_offset(0), m_sync_state_change(false)
	{

	}

	void ReplBacklog::Run()
	{

	}

	bool ReplBacklog::LoadSyncState(const std::string& path)
	{
		std::string file = path + "/master_sync.state";
		if (0
		        != m_sync_state_buf.Init(file,
		                ARDB_MASTER_SYNC_STATE_MMAP_FILE_SIZE))
		{
			return false;
		}
		if (*(m_sync_state_buf.m_buf))
		{
			char serverkeybuf[SERVER_KEY_SIZE + 1];
			sscanf(m_sync_state_buf.m_buf, "%s %llu %llu %llu %llu",
			        serverkeybuf, &m_end_offset, &m_begin_offset,
			        &m_backlog_idx, &m_histlen);
			m_server_key = serverkeybuf;
		}
		else
		{
			m_server_key = random_hex_string(SERVER_KEY_SIZE);
			/* When a new backlog buffer is created, we increment the replication
			 * offset by one to make sure we'll not be able to PSYNC with any
			 * previous slave. This is needed because we avoid incrementing the
			 * master_repl_offset if no backlog exists nor slaves are attached. */
			m_end_offset++;

			/* We don't have any data inside our buffer, but virtually the first
			 * byte we have is the next byte that will be generated for the
			 * replication stream. */
			m_begin_offset = m_end_offset + 1;
			PersistSyncState();
		}
		m_last_sync_offset = m_end_offset;
		INFO_LOG(
		        "[Master]Server key:%s, begin_offset:%llu, end_offset:%llu, idx:%llu, histlen=%llu", m_server_key.c_str(), m_begin_offset, m_end_offset, m_backlog_idx, m_histlen);
		return true;
	}

	void ReplBacklog::PersistSyncState()
	{
		if(!m_sync_state_change)
		{
			return;
		}
		int len = snprintf(m_sync_state_buf.m_buf, m_sync_state_buf.m_size,
		        "%s %llu %llu %llu %llu", m_server_key.c_str(), m_end_offset,
		        m_begin_offset, m_backlog_idx, m_histlen);
		msync(m_sync_state_buf.m_buf, len, MS_ASYNC);
		if (m_end_offset > m_last_sync_offset)
		{
			uint64 len = m_end_offset - m_last_sync_offset;
			if (m_backlog_idx > len)
			{
				msync(m_sync_state_buf.m_buf + m_backlog_idx - len, len,
				        MS_ASYNC);
			}
			else
			{
				msync(
				        m_sync_state_buf.m_buf + m_backlog_size - len
				                + m_backlog_idx, len - m_backlog_idx, MS_ASYNC);
				msync(m_sync_state_buf.m_buf, m_backlog_idx, MS_ASYNC);
			}
		}
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

		return 0;
	}

	void ReplBacklog::Feed(Buffer& buffer)
	{
		const char* buf = buffer.GetRawReadBuffer();
		size_t len = buffer.ReadableBytes();
		m_end_offset += len;
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
			m_histlen += thislen;
		}
		if (m_histlen > m_backlog_size)
			m_histlen = m_backlog_size;
		/* Set the offset of the first byte we have in the backlog. */
		m_begin_offset = m_end_offset - m_histlen + 1;
		m_sync_state_change = true;
	}

	const std::string& ReplBacklog::GetServerKey()
	{
		return m_server_key;
	}
	uint64 ReplBacklog::GetReplEndOffset()
	{
		return m_end_offset;
	}

	uint64 ReplBacklog::GetReplStartOffset()
	{
		return m_begin_offset;
	}

	size_t ReplBacklog::WriteChannel(Channel* channle, int64 offset, size_t len)
	{
		if (!IsValidOffset(offset))
		{
			return 0;
		}
		int64 skip = offset - m_begin_offset;
		uint64 j = (m_backlog_idx + (m_backlog_size - m_histlen))
		        % m_backlog_size;
		j = (j + skip) % m_backlog_size;
		uint64 total = len;
		if (total > m_histlen - skip)
		{
			total = m_histlen - skip;
		}
		len = total;

		while (total)
		{
			uint64 thislen =
			        ((m_backlog_size - j) < len) ? (m_backlog_size - j) : len;
			Buffer buf(m_backlog.m_buf + j, 0, thislen);
			channle->Write(buf);
			total -= thislen;
			j = 0;
		}
		return len;
	}

	bool ReplBacklog::IsValidOffset(int64 offset)
	{
		if ((uint64) offset == m_begin_offset)
		{
			return true;
		}
		if (offset < 0 || (uint64) offset > m_end_offset + 1
		        || (uint64) offset < m_begin_offset)
		{
			INFO_LOG(
			        "Unable to partial resync with the slave for lack of backlog (Slave request was: %lld). %lld-%lld", offset, m_begin_offset, m_end_offset);
			return false;
		}
		return true;
	}

	bool ReplBacklog::IsValidOffset(const std::string& server_key, int64 offset)
	{
		if (m_server_key != server_key)
		{
			INFO_LOG(
			        "Partial resynchronization not accepted: "
			        "Serverkey mismatch (Client asked for '%s', I'm '%s')", server_key.c_str(), m_server_key.c_str());
			return false;
		}
		return IsValidOffset(offset);
	}
}

