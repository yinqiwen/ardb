/*
 * backlog.cpp
 *
 *  Created on: 2013-08-29      Author: wqy
 */
#include "repl_backlog.hpp"
#include "ardb.hpp"
#include "redis/crc64.h"
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>

#define ARDB_SHM_PATH "/ardb/repl"
#define MAX_MMAP_UNIT_SIZE  100*1024*1024
#define REPL_STATE_MAX_SIZE 256

namespace ardb
{

    ReplBacklog::ReplBacklog() :
            m_state(NULL), m_repl_backlog(NULL), m_last_sync_offset(0), m_sync_state_change(false), m_inited(false)
    {
    }

    void ReplBacklog::Run()
    {
        Persist();
    }

    bool ReplBacklog::Load(const std::string& path, uint32 backlog_size)
    {
        if (0 != m_backlog.Init(path,
        REPL_STATE_MAX_SIZE + backlog_size))
        {
            return false;
        }
        m_state = (ReplPersistState*) (m_backlog.m_buf);
        m_state->repl_backlog_size = backlog_size;
        m_repl_backlog = (m_backlog.m_buf + REPL_STATE_MAX_SIZE);
        if (m_state->serverkey[0])
        {
            /*
             * Loaded previous state, do nothing
             */
        }
        else
        {
            std::string randomkey = random_hex_string(SERVER_KEY_SIZE);
            memcpy(m_state->serverkey, randomkey.data(), SERVER_KEY_SIZE);
            m_state->serverkey[SERVER_KEY_SIZE] = 0;
            m_state->repl_backlog_histlen = 0;
            m_state->repl_backlog_idx = 0;
            /* When a new backlog buffer is created, we increment the replication
             * offset by one to make sure we'll not be able to PSYNC with any
             * previous slave. This is needed because we avoid incrementing the
             * master_repl_offset if no backlog exists nor slaves are attached. */
            m_state->master_repl_offset++;

            /* We don't have any data inside our buffer, but virtually the first
             * byte we have is the next byte that will be generated for the
             * replication stream. */
            m_state->repl_backlog_off = m_state->master_repl_offset + 1;

            m_state->cksm = 0;
            m_state->current_db = ARDB_GLOBAL_DB;
            m_sync_state_change = true;
            Persist();
        }
        m_last_sync_offset = m_state->repl_backlog_off;
        return true;
    }

    void ReplBacklog::Persist()
    {
        if (!m_sync_state_change)
        {
            return;
        }
        msync(m_state, sizeof(ReplPersistState), MS_ASYNC);
        if (m_state->repl_backlog_off > m_last_sync_offset)
        {
            uint64 len = m_state->repl_backlog_off - m_last_sync_offset;
            if ((uint64) (m_state->repl_backlog_idx) > len)
            {
                msync(m_repl_backlog + m_state->repl_backlog_idx - len, len,
                MS_ASYNC);
            }
            else
            {
                msync(m_repl_backlog + m_state->repl_backlog_size - len + m_state->repl_backlog_idx,
                        len - m_state->repl_backlog_idx, MS_ASYNC);
                msync(m_repl_backlog, m_state->repl_backlog_idx, MS_ASYNC);
            }
            m_last_sync_offset = m_state->repl_backlog_off;
        }
        m_sync_state_change = false;
    }

    int ReplBacklog::Init(Ardb* server)
    {
        m_inited = false;
        ArdbConfig& cfg = server->GetConfig();
        uint64 backlog_size = cfg.repl_backlog_size;
        if (cfg.repl_backlog_size == 0)
        {
            WARN_LOG("Replication backlog buffer is disabled, and current server can NOT accept slave connections.");
            return -1;
        }
        if (cfg.repl_backlog_size < 0 || (cfg.repl_backlog_size > 0 && backlog_size < 1024 * 1024))
        {
            WARN_LOG("Replication backlog buffer size should greater than 1mb.");
            return -1;
        }
        std::string state_file = cfg.repl_data_dir + "/repl.log";
        if (!Load(state_file, backlog_size))
        {
            ERROR_LOG("Failed to load replication state file.");
            return -1;
        }

        /*
         * If user disable replication log, then do NOT start persist task
         */
        if (cfg.repl_backlog_size > 0)
        {
            server->m_service->GetTimer().Schedule(this, cfg.repl_state_persist_period, cfg.repl_state_persist_period,
                    SECONDS);
        }
        m_inited = true;
        return 0;
    }

    void ReplBacklog::Feed(Buffer& buffer)
    {
        const char* p = buffer.GetRawReadBuffer();
        size_t len = buffer.ReadableBytes();

        m_state->master_repl_offset += len;

        /* This is a circular buffer, so write as much data we can at every
         * iteration and rewind the "idx" index if we reach the limit. */
        while (len)
        {
            size_t thislen = m_state->repl_backlog_size - m_state->repl_backlog_idx;
            if (thislen > len)
                thislen = len;
            memcpy(m_repl_backlog + m_state->repl_backlog_idx, p, thislen);
            m_state->repl_backlog_idx += thislen;
            if (m_state->repl_backlog_idx == m_state->repl_backlog_size)
                m_state->repl_backlog_idx = 0;
            len -= thislen;
            p += thislen;
            m_state->repl_backlog_histlen += thislen;
        }
        if (m_state->repl_backlog_histlen > m_state->repl_backlog_size)
            m_state->repl_backlog_histlen = m_state->repl_backlog_size;
        /* Set the offset of the first byte we have in the backlog. */
        m_state->repl_backlog_off = m_state->master_repl_offset - m_state->repl_backlog_histlen + 1;

        m_state->cksm = crc64(m_state->cksm, (const unsigned char *) (buffer.GetRawReadBuffer()),
                buffer.ReadableBytes());
        m_sync_state_change = true;
    }

    const char* ReplBacklog::GetServerKey()
    {
        return m_state->serverkey;
    }
    uint64 ReplBacklog::GetReplEndOffset()
    {
        return m_state->master_repl_offset;
    }

    uint64 ReplBacklog::GetReplStartOffset()
    {
        return m_state->repl_backlog_off;
    }

    size_t ReplBacklog::WriteChannel(Channel* channle, int64 offset, size_t len)
    {
        if (!IsValidOffset(offset))
        {
            return 0;
        }

        int64 total = m_state->master_repl_offset - offset;
        size_t write_len = 0;
        if (m_state->repl_backlog_idx >= total)
        {
            write_len = total > len ? len : total;
            Buffer buf(m_repl_backlog + m_state->repl_backlog_idx - total, 0, write_len);
            channle->Write(buf);
        }
        else
        {
            int64 start_pos = m_state->repl_backlog_size - total + m_state->repl_backlog_idx;
            total = total - m_state->repl_backlog_idx;
            if (total <= len)
            {
                write_len = total;
            }else
            {
                write_len = len;
            }
            Buffer buf1(m_repl_backlog + start_pos, 0, write_len);
            channle->Write(buf1);
        }
        return write_len;
    }

    bool ReplBacklog::IsValidOffset(int64 offset)
    {
        if (offset < 0 || (uint64) offset < m_state->repl_backlog_off
                || (uint64) offset > (m_state->repl_backlog_off + m_state->repl_backlog_histlen))
        {
            INFO_LOG(
                    "Unable to partial resync with the slave for lack of backlog (Slave request was: %" PRId64"), and server offset is %" PRId64,
                    offset, m_state->master_repl_offset);
            return false;
        }
        return true;
    }

    bool ReplBacklog::IsValidOffset(const std::string& server_key, int64 offset)
    {
        if (strcmp(server_key.c_str(), m_state->serverkey))
        {
            INFO_LOG("Partial resynchronization not accepted: "
                    "Serverkey mismatch (Client asked for '%s', I'm '%s')", server_key.c_str(), m_state->serverkey);
            return false;
        }
        return IsValidOffset(offset);
    }

    bool ReplBacklog::IsValidCksm(int64 offset, uint64 cksm)
    {
        uint32 total = m_state->master_repl_offset - offset;
        uint64 newcksm = cksm;
        if (m_state->repl_backlog_idx >= total)
        {
            newcksm = crc64(newcksm, (const unsigned char *) (m_repl_backlog + m_state->repl_backlog_idx - total),
                    total);
        }
        else
        {
            newcksm = crc64(newcksm,
                    (const unsigned char *) (m_repl_backlog + m_state->repl_backlog_size - total
                            + m_state->repl_backlog_idx), total - m_state->repl_backlog_idx);
            newcksm = crc64(newcksm, (const unsigned char *) m_repl_backlog, m_state->repl_backlog_idx);
        }
        if (newcksm != m_state->cksm)
        {
            INFO_LOG(
                    "Unable to partial resync with the slave for confilct checksum (Slave cksm is: %llu, and server cksm is %llu",
                    newcksm, m_state->cksm);
            return false;
        }
        return true;
    }

    void ReplBacklog::SetServerkey(const std::string& serverkey)
    {
        memcpy(m_state->serverkey, serverkey.data(), SERVER_KEY_SIZE);
        m_state->serverkey[SERVER_KEY_SIZE] = 0;
        m_sync_state_change = true;
    }
    void ReplBacklog::SetReplOffset(int64 offset)
    {
        m_state->repl_backlog_idx = 0;
        m_state->repl_backlog_histlen = 0;
        m_state->master_repl_offset = offset;
        m_state->repl_backlog_off = offset + 1;
        m_sync_state_change = true;
    }
    void ReplBacklog::SetChecksum(uint64 cksm)
    {
        m_state->cksm = cksm;
        m_sync_state_change = true;
    }

    void ReplBacklog::ClearState()
    {
        SetReplOffset(0);
        SetServerkey(random_hex_string(SERVER_KEY_SIZE));
    }
}

