/*
 * repl.hpp
 *
 *  Created on: 2013-08-29     Author: wqy
 */

#ifndef REPL_HPP_
#define REPL_HPP_

#include "common/common.hpp"
#include "channel/all_includes.hpp"
#include "thread/thread.hpp"
#include "thread/thread_mutex.hpp"
#include "thread/lock_guard.hpp"
#include "util/concurrent_queue.hpp"
#include "swal.h"
#include <map>

using namespace ardb::codec;

OP_NAMESPACE_BEGIN
    class ReplicationBacklog
    {
        private:
            swal_t* m_wal;
            void ReCreateWAL();
            static void WriteWALCallback(Channel*, void* data);
            int WriteWAL(DBID db, const Buffer& cmd);
            int WriteWAL(const Buffer& cmd);
            void Routine();
            void FlushSyncWAL();
        public:
            ReplicationBacklog();
            int Init();
            void Routine();
            swal_t* GetWAL();
            const char* GetServerKey();
            void SetServerKey(const std::string& str);
            int WriteWAL(DBID db, RedisCommandFrame& cmd);
            bool IsValidOffsetCksm(int64_t offset, uint64_t cksm);
            uint64_t WALStartOffset();
            uint64_t WALEndOffset();
            uint64_t WALCksm();
            void ResetWALOffsetCksm(uint64_t offset, uint64_t cksm);

            void ResetDataOffsetCksm(uint64_t offset, uint64_t cksm);
            uint64_t DataOffset();
            uint64_t DataCksm();
            void UpdateDataOffsetCksm(const Buffer& data);
            ~ReplicationBacklog();
    };
    extern ReplicationBacklog* g_repl;
OP_NAMESPACE_END

#endif /* REPL_HPP_ */
