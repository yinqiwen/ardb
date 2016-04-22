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

#ifndef RDB_HPP_
#define RDB_HPP_
#include <string>
#include <deque>
#include "common.hpp"
#include "buffer/buffer_helper.hpp"
#include "context.hpp"
#include "db/codec.hpp"

namespace ardb
{

    enum SnapshotType
    {
        REDIS_DUMP = 0, ARDB_DUMP
    };

    enum SnapshotState
    {
        SNAPSHOT_INVALID = 0, DUMP_START = 1, DUMPING, DUMP_SUCCESS, DUMP_FAIL, LOAD_START = 10, LODING, LOAD_SUCCESS, LOAD_FAIL
    };
    class Snapshot;
    typedef int SnapshotRoutine(SnapshotState state, Snapshot* snapshot, void* cb);

    class Snapshot
    {
        protected:
            FILE* m_read_fp;
            FILE* m_write_fp;
            std::string m_file_path;
            uint64 m_cksm;
            SnapshotRoutine* m_routine_cb;
            void *m_routine_cbdata;
            uint64 m_processed_bytes;
            uint64 m_file_size;
            SnapshotState m_state;
            uint64 m_routinetime;
            char* m_read_buf;

            int64 m_expected_data_size;
            int64 m_writed_data_size;

            Buffer m_write_buffer;
            uint64 m_cached_repl_offset;
            uint64 m_cached_repl_cksm;
            time_t m_save_time;
            SnapshotType m_type;
            bool Read(void* buf, size_t buflen, bool cksm = true);

            int WriteType(uint8 type);
            int WriteKeyType(KeyType type);
            int WriteLen(uint32 len);
            int WriteMillisecondTime(uint64 ts);
            int WriteDouble(double v);
            int WriteLongLongAsStringObject(long long value);
            int WriteRawString(const char *s, size_t len);
            int WriteLzfStringObject(const char *s, size_t len);
            int WriteTime(time_t t);
            int WriteStringObject(const Data& o);

            int ReadType();
            time_t ReadTime();
            int64 ReadMillisecondTime();
            uint32_t ReadLen(int *isencoded);
            bool ReadInteger(int enctype, int64& v);
            bool ReadLzfStringObject(std::string& str);
            bool ReadString(std::string& str);
            int ReadDoubleValue(double&val);

            bool RedisLoadObject(Context& ctx, int type, const std::string& key, int64 expiretime);
            void RedisLoadListZipList(Context& ctx, unsigned char* data, const std::string& key, ValueObject& meta_value);
            void RedisLoadHashZipList(Context& ctx, unsigned char* data, const std::string& key, ValueObject& meta_value);
            void RedisLoadZSetZipList(Context& ctx, unsigned char* data, const std::string& key, ValueObject& meta_value);
            void RedisLoadSetIntSet(Context& ctx, unsigned char* data, const std::string& key, ValueObject& meta_value);

            void RedisWriteMagicHeader();

            int RedisLoad();
            int RedisSave();

            int ArdbWriteMagicHeader();
            int ArdbSaveRawKeyValue(const Slice& key, const Slice& value);
            int ArdbFlushWriteBuffer();
            int ArdbLoadBuffer(Context& ctx, Buffer& buffer);
            int ArdbSave();
            int ArdbLoad();

            int DoSave();
            int PrepareSave(SnapshotType type, const std::string& file, SnapshotRoutine* cb, void *data);
        public:
            Snapshot();
            SnapshotType GetType()
            {
                return m_type;
            }
            uint64 CachedReplOffset()
            {
                return m_cached_repl_offset;
            }
            uint64 CachedReplCksm()
            {
                return m_cached_repl_cksm;
            }
            const std::string& GetPath()
            {
                return m_file_path;
            }
            time_t SaveTime()
            {
                return m_save_time;
            }
            bool IsSaving() const;
            bool IsReady() const;
            void SetExpectedDataSize(int64 size);
            int64 DumpLeftDataSize();
            int64 ProcessLeftDataSize();
            int Write(const void* buf, size_t buflen);
            int OpenWriteFile(const std::string& file);
            int OpenReadFile(const std::string& file);
            int Load(const std::string& file, SnapshotRoutine* cb, void *data);
            int Reload(SnapshotRoutine* cb, void *data);
            int Save(SnapshotType type, const std::string& file, SnapshotRoutine* cb, void *data);
            int BGSave(SnapshotType type, const std::string& file, SnapshotRoutine* cb = NULL, void *data = NULL);

            void Flush();
            void Remove();
            int Rename(const std::string& default_file = "dump.rdb");
            void Close();
            ~Snapshot();

            static int IsRedisDumpFile(const std::string& file);
    };

    class SnapshotManager
    {
        private:
            typedef std::deque<Snapshot*> SnapshotArray;
            SnapshotArray m_snapshots;
        public:
            SnapshotManager();
            void RemoveExpiredSnapshots();
            Snapshot* GetSyncSnapshot(SnapshotType type, SnapshotRoutine* cb, void *data);
    };

    extern SnapshotManager* g_snapshot_manager;

}

#endif /* RDB_HPP_ */
