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

#ifndef KYOTOCABINET_ENGINE_HPP_
#define KYOTOCABINET_ENGINE_HPP_

/*
 * leveldb_engine.hpp
 *
 *  Created on: 2013-3-31
 *      Author: yinqiwen
 */

#include <kcpolydb.h>
#include <stack>
#include "db.hpp"
#include "util/config_helper.hpp"
#include "util/thread/thread_local.hpp"

namespace ardb
{
    class KCDBIterator: public Iterator
    {
        private:
            kyotocabinet::DB::Cursor* m_cursor;
            bool m_valid;
            Buffer m_key_buffer;
            Buffer m_value_buffer;
            void Next();
            void Prev();
            Slice Key() const;
            Slice Value() const;
            bool Valid();
            void SeekToFirst();
            void SeekToLast();
            void Seek(const Slice& target);
        public:
            KCDBIterator(kyotocabinet::DB::Cursor* cursor, bool valid = true) :
                    m_cursor(cursor), m_valid(valid)
            {
            }
            ~KCDBIterator()
            {
                DELETE(m_cursor);
            }
    };

    class KCDBComparator: public kyotocabinet::Comparator
    {
            int32_t compare(const char* akbuf, size_t aksiz, const char* bkbuf, size_t bksiz);
    };

    struct KCDBConfig
    {
            std::string path;
    };

    class KCDBEngine: public KeyValueEngine
    {
        private:
            kyotocabinet::TreeDB* m_db;
            KCDBComparator m_comparator;

            struct BatchHolder
            {
                    StringSet batch_del;
                    StringStringMap batch_put;
                    uint32 ref;
                    void ReleaseRef()
                    {
                        if (ref > 0)
                            ref--;
                    }
                    void AddRef()
                    {
                        ref++;
                    }
                    bool EmptyRef()
                    {
                        return ref == 0;
                    }
                    void Put(const Slice& key, const Slice& value)
                    {
                        std::string keystr(key.data(), key.size());
                        std::string valstr(value.data(), value.size());
                        batch_del.erase(keystr);
                        batch_put.insert(StringStringMap::value_type(keystr, valstr));
                    }
                    void Del(const Slice& key)
                    {
                        std::string keystr(key.data(), key.size());
                        batch_del.insert(keystr);
                        batch_put.erase(keystr);
                    }
                    void Clear()
                    {
                        batch_del.clear();
                        batch_put.clear();
                        ref = 0;
                    }
                    BatchHolder() :
                            ref(0)
                    {
                    }
            };
            ThreadLocal<BatchHolder> m_batch_local;
            int FlushWriteBatch(BatchHolder& holder);
        public:
            KCDBEngine();
            ~KCDBEngine();
            void Clear();
            void Close();
            int Init(const KCDBConfig& cfg);
            int Put(const Slice& key, const Slice& value);
            int Get(const Slice& key, std::string* value, bool fill_cache);
            int Del(const Slice& key);
            int BeginBatchWrite();
            int CommitBatchWrite();
            int DiscardBatchWrite();
            Iterator* Find(const Slice& findkey, bool cache);
    };

    class KCDBEngineFactory: public KeyValueEngineFactory
    {
        private:
            KCDBConfig m_cfg;
            static void ParseConfig(const Properties& props, KCDBConfig& cfg);
        public:
            KCDBEngineFactory(const Properties& cfg);
            KeyValueEngine* CreateDB(const std::string& db);
            void DestroyDB(KeyValueEngine* engine);
            void CloseDB(KeyValueEngine* engine);
            const std::string GetName()
            {
                return "KyotoCabinet";
            }
    };
}

#endif /* KYOTOCABINET_ENGINE_HPP_ */
