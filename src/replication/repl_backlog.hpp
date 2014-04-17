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

/*
 * backlog.hpp
 *
 *  Created on: 2013-08-29
 *  Author: yinqiwen
 */

#ifndef BACKLOG_HPP_
#define BACKLOG_HPP_

#include <string>
#include <vector>
#include "common.hpp"
#include "channel/all_includes.hpp"
#include "util/mmap.hpp"
#include "repl.hpp"
#include "data_format.hpp"

#define SERVER_KEY_SIZE 40

using namespace ardb::codec;

namespace ardb
{
    struct ReplPersistState
    {
            char serverkey[SERVER_KEY_SIZE + 1];
            uint64 cksm;
            uint32 repl_backlog_size;
            uint64 repl_backlog_off;
            uint32 repl_backlog_idx;
            uint64 repl_backlog_histlen;
            uint64 master_repl_offset;
            DBID current_db;
    };

    class ArdbServer;
    class ReplBacklog: public Runnable
    {
        private:
            ReplPersistState* m_state;
            char* m_repl_backlog;
            uint64 m_last_sync_offset;
            bool m_sync_state_change;

            MMapBuf m_backlog;
            bool m_inited;

            void Run();

            bool Load(const std::string& path, uint32 backlog_size);
            void Persist();
        public:
            ReplBacklog();
            int Init(ArdbServer* server);
            bool IsInited()
            {
                return m_inited;
            }
            void Feed(Buffer& cmd);
            bool IsValidOffset(int64 offset);
            bool IsValidOffset(const std::string& server_key, int64 offset);
            const char* GetServerKey();
            uint64 GetChecksum()
            {
                return m_state->cksm;
            }
            uint64 GetReplEndOffset();
            uint64 GetReplStartOffset();
            int64 GetBacklogSize()
            {
                return (int64) m_state->repl_backlog_size;
            }
            int64 GetHistLen()
            {
                return (int64) m_state->repl_backlog_histlen;
            }
            DBID GetCurrentDBID()
            {
                return m_state->current_db;
            }
            void SetCurrentDBID(DBID id)
            {
                m_state->current_db = id;
            }
            void ClearDBID()
            {
                m_state->current_db = ARDB_GLOBAL_DB;
            }
            size_t WriteChannel(Channel* channle, int64 offset, size_t len);
            void SetServerkey(const std::string& serverkey);
            void SetReplOffset(int64 offset);
            void SetChecksum(uint64 cksm);

    };
}

#endif /* BACKLOG_HPP_ */
