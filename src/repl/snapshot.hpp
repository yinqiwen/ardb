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

#ifndef SNAPSHOT_HPP_
#define SNAPSHOT_HPP_
#include <string>
#include <deque>
#include "common.hpp"
#include "buffer/buffer_helper.hpp"
#include "context.hpp"
#include "db/codec.hpp"
#include "db/db_utils.hpp"
#include "thread/thread_mutex_lock.hpp"

namespace ardb
{
    enum SnapshotType
    {
        REDIS_DUMP = 1, ARDB_DUMP, BACKUP_DUMP
    };

    enum SnapshotState
    {
        SNAPSHOT_INVALID = 0, DUMP_START = 1, DUMPING, DUMP_SUCCESS, DUMP_FAIL, LOAD_START = 10, LODING, LOAD_SUCCESS, LOAD_FAIL
    };
    class Snapshot;
    typedef int SnapshotRoutine(SnapshotState state, Snapshot* snapshot, void* cb);

    class ObjectIO
    {
        protected:
            DBWriter* m_dbwriter;
            virtual bool Read(void* buf, size_t buflen, bool cksm = true) = 0;
            virtual int Write(const void* buf, size_t buflen) = 0;
            int WriteType(uint8 type);
            int WriteKeyType(KeyType type);
            int WriteLen(uint64 len);
            int WriteMillisecondTime(uint64 ts);
            int WriteDouble(double v);
            int WriteLongLongAsStringObject(long long value);
            int WriteRawString(const std::string& str);
            int WriteRawString(const char *s, size_t len);
            int WriteLzfStringObject(const char *s, size_t len);
            int WriteTime(time_t t);
            int WriteStringObject(const Data& o);

            int ReadType();
            time_t ReadTime();
            int64 ReadMillisecondTime();
            uint64_t ReadLen(int *isencoded);
            bool ReadInteger(int enctype, int64& v);
            bool ReadLzfStringObject(std::string& str);
            bool ReadString(std::string& str);
            int ReadDoubleValue(double& val, bool binary = false);
            int ReadBinaryDoubleValue(double& val);
            int ReadBinaryFloatValue(float& val);

            bool RedisLoadCheckModuleValue(char* name);
            bool RedisLoadObject(Context& ctx, int type, const std::string& key, int64 expiretime);
            void RedisLoadListZipList(Context& ctx, unsigned char* data, const std::string& key, ValueObject& meta_value);
            void RedisLoadHashZipList(Context& ctx, unsigned char* data, const std::string& key, ValueObject& meta_value);
            void RedisLoadZSetZipList(Context& ctx, unsigned char* data, const std::string& key, ValueObject& meta_value);
            void RedisLoadSetIntSet(Context& ctx, unsigned char* data, const std::string& key, ValueObject& meta_value);
            void RedisWriteMagicHeader();

            int ArdbWriteMagicHeader();
            int ArdbLoadChunk(Context& ctx, int type);
            int ArdbLoadBuffer(Context& ctx, Buffer& buffer);

            DBWriter& GetDBWriter();
        public:
            ObjectIO() :
                    m_dbwriter(NULL)
            {
            }
            void SetDBWriter(DBWriter* writer)
            {
                m_dbwriter = writer;
            }
            int ArdbSaveRawKeyValue(const Slice& key, const Slice& value, Buffer& buffer, int64 ttl);
            int ArdbFlushWriteBuffer(Buffer& buffer);
            virtual ~ObjectIO()
            {
            }
    };

    class ObjectBuffer: public ObjectIO
    {
        private:
            Buffer m_buffer;
            bool Read(void* buf, size_t buflen, bool cksm);
            int Write(const void* buf, size_t buflen);
        public:
            ObjectBuffer();
            ObjectBuffer(const std::string& content);
            bool RedisSave(Context& ctx, const std::string& key, std::string& content, uint64* ttl = NULL);
            bool RedisLoad(Context& ctx, const std::string& key, int64 ttl);
            bool CheckReadPayload();

            Buffer& GetInternalBuffer()
            {
                return m_buffer;
            }
            bool ArdbLoad(Context& ctx);
            void Reset()
            {
                m_buffer.Clear();
            }

    };

    class SnapshotManager;
    class Snapshot: public ObjectIO
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

            const void* m_engine_snapshot;
            bool Read(void* buf, size_t buflen, bool cksm);

            int RedisLoad();
            int RedisSave();

            int ArdbSave();
            int ArdbLoad();

            int BackupSave();
            int BackupLoad();

            int DoSave();
            int BeforeSave(SnapshotType type, const std::string& file, SnapshotRoutine* cb, void *data);
            int AfterSave(const std::string& fname, int err);
            int PrepareSave(SnapshotType type, const std::string& file, SnapshotRoutine* cb, void *data);
            void VerifyState();

            friend class SnapshotManager;
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
            bool IsSaving();
            bool IsReady();
            void MarkDumpComplete();
            void SetExpectedDataSize(int64 size);
            int64 DumpLeftDataSize();
            int64 ProcessLeftDataSize();
            int Write(const void* buf, size_t buflen);
            int OpenWriteFile(const std::string& file);
            int OpenReadFile(const std::string& file);
            int SetFilePath(const std::string& path);
            int Load(const std::string& file, SnapshotRoutine* cb, void *data);
            int Reload(SnapshotRoutine* cb, void *data);
            int Save(SnapshotType type, const std::string& file, SnapshotRoutine* cb, void *data);
            int BGSave(SnapshotType type, const std::string& file, SnapshotRoutine* cb = NULL, void *data = NULL);

            void Flush();
            void Remove();
            int Rename(const std::string& default_file = "dump.rdb");
            void Close();
            void SetRoutineCallback(SnapshotRoutine* cb, void *data);
            void* GetIteratorByNamespace(Context& ctx, const Data& ns);
            ~Snapshot();

            static SnapshotType GetSnapshotType(const std::string& file);
            static SnapshotType GetSnapshotTypeByName(const std::string& name);
            static std::string GetSyncSnapshotPath(SnapshotType type, uint64 offset, uint64 cksm);
    };

    class SnapshotManager
    {
        private:
            typedef std::deque<Snapshot*> SnapshotArray;
            ThreadMutexLock m_snapshots_lock;
            SnapshotArray m_snapshots;
        public:
            SnapshotManager();
            void Init();
            void Routine();
            Snapshot* GetSyncSnapshot(SnapshotType type, SnapshotRoutine* cb, void *data);
            Snapshot* NewSnapshot(SnapshotType type, bool bgsave, SnapshotRoutine* cb, void *data);
            void AddSnapshot(const std::string& path);
            time_t LastSave();
            int CurrentSaverNum();
            time_t LastSaveCost();
            int LastSaveErr();
            time_t LastSaveStartUnixTime();
            void PrintSnapshotInfo(std::string& str);
    };

    extern SnapshotManager* g_snapshot_manager;

}

#endif /* RDB_HPP_ */
