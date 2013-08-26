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

namespace ardb
{

	ReplBacklog::ReplBacklog() :
			m_backlog_size(0)
	{

	}

	void ReplBacklog::Run()
	{

	}

	void ReplBacklog::ClearMapBuf(BacklogBufArray& bufs)
	{
		BacklogBufArray::iterator it = bufs.begin();
		while (it != bufs.end())
		{
			MMapBuf& mbuf = *it;
			munmap(mbuf.buf, mbuf.size);
			it++;
		}
		bufs.clear();
	}
	int ReplBacklog::ReInit(const std::string& path, uint64 backlog_size)
	{
		if (backlog_size < 1024 * 1024)
		{
			ERROR_LOG("Replication backlog buffer size must greater than 1mb.");
			return -1;
		}
		int fd = open(path.c_str(), O_CREAT | O_RDWR,
		S_IRUSR | S_IWUSR);
		if (fd < 0)
		{
			const char* err = strerror(errno);
			ERROR_LOG("Failed to open replication log:%s for reason:%s",
					path.c_str(), err);
			return -1;
		}

		struct stat st;
		fstat(fd, &st);
		if (backlog_size < st.st_size)
		{
			ERROR_LOG("Can not shrink replication log size from %llu to %llu",
					st.st_size, backlog_size);
			return -1;
		}

		if (-1 == ftruncate(fd, backlog_size))
		{
			const char* err = strerror(errno);
			ERROR_LOG("Failed to truncate replication log:%s for reason:%s",
					path.c_str(), err);
			close(fd);
			return -1;
		}

		/*
		 *
		 */
		uint32 idx = 0;
		uint32 total_mmap_size = backlog_size;
		BacklogBufArray mbufs;
		while (total_mmap_size > 0)
		{
			uint32 mmap_size =
					backlog_size > ARDB_MAX_MMAP_UNIT_SIZE ?
					ARDB_MAX_MMAP_UNIT_SIZE :
																backlog_size;
			char* mbuf = (char*) mmap(NULL, mmap_size,
			PROT_READ | PROT_WRITE,
			MAP_SHARED, fd, idx * ARDB_MAX_MMAP_UNIT_SIZE);
			if (mbuf == MAP_FAILED)
			{
				const char* err = strerror(errno);
				ERROR_LOG(
						"Failed to mmap replication log:%s from %llu for reason:%s",
						path.c_str(), idx * ARDB_MAX_MMAP_UNIT_SIZE, err);
				close(fd);
				ClearMapBuf(mbufs);
				return -1;
			} else
			{
				MMapBuf m;
				m.buf = mbuf;
				m.size = mmap_size;
				mbufs.push_back(m);
			}
			idx++;
			total_mmap_size -= mmap_size;
		}
		m_backlog_bufs = mbufs;
		m_backlog_size = backlog_size;
		return 0;
	}

	void ReplBacklog::Put(const char* buf, size_t len)
	{
		uint32 buf_idx = len / ARDB_MAX_MMAP_UNIT_SIZE;
		uint32 buf_offset = len % ARDB_MAX_MMAP_UNIT_SIZE;
		while(len)
		{
			uint32 copylen = m_backlog_bufs[buf_idx].size - buf_offset + 1;
			if(copylen > len)
			{
				copylen = len;
			}
			memcpy(m_backlog_bufs[buf_idx].buf + buf_offset, buf, copylen);
			len -= copylen;
			buf_offset = 0;
			buf_idx++;
		}
	}

	void ReplBacklog::Feed(const char* buf, size_t len)
	{
		m_master_repl_offset += len;

		/* This is a circular buffer, so write as much data we can at every
		 * iteration and rewind the "idx" index if we reach the limit. */
		while (len)
		{
			size_t thislen = m_backlog_size - m_backlog_idx;
			if (thislen > len)
				thislen = len;
			Put(buf, thislen);
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
	}
}

