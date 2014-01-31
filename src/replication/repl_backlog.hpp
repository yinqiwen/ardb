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
 *  Author: wqy
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

using namespace ardb::codec;

namespace ardb
{
    class ArdbServer;
    class ReplBacklog: public Runnable
    {
        private:
            uint64 m_backlog_size;
            uint64 m_backlog_idx;
            uint64 m_end_offset;
            uint64 m_histlen;
            uint64 m_begin_offset;
            uint64 m_last_sync_offset;

            std::string m_server_key;
            bool m_sync_state_change;

            MMapBuf m_sync_state_buf;
            MMapBuf m_backlog;

            DBID m_current_dbid;
            bool m_inited;

            void Run();

            bool LoadSyncState(const std::string& path);
        public:
            ReplBacklog();
            int Init(ArdbServer* server);
            bool IsInited()
            {
                return m_inited;
            }
            void PersistSyncState();

//            void Feed(RedisCommandFrame& cmd,const DBID& dbid);
            void Feed(Buffer& cmd);
            bool IsValidOffset(int64 offset);
            bool IsValidOffset(const std::string& server_key, int64 offset);
            const std::string& GetServerKey();
            uint64 GetReplEndOffset();
            uint64 GetReplStartOffset();
            int64 GetBacklogSize()
            {
                return (int64) m_backlog_size;
            }
            int64 GetHistLen()
            {
                return (int64) m_histlen;
            }
            DBID GetCurrentDBID()
            {
                return m_current_dbid;
            }
            void SetCurrentDBID(DBID id)
            {
                m_current_dbid = id;
            }
            void ClearDBID()
            {
                m_current_dbid = ARDB_GLOBAL_DB;
            }
            size_t WriteChannel(Channel* channle, int64 offset, size_t len);
            void UpdateState(const std::string& serverkey, int64 offset);

    };
}

#endif /* BACKLOG_HPP_ */
