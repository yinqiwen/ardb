/*
 *Copyright (c) 2013-2016, yinqiwen <yinqiwen@gmail.com>
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

#ifndef SRC_DB_ENGINE_HPP_
#define SRC_DB_ENGINE_HPP_
#include "common/common.hpp"
#include "codec.hpp"
#include "context.hpp"
#include "util/config_helper.hpp"

OP_NAMESPACE_BEGIN

    struct Iterator
    {
            virtual bool Valid() = 0;
            virtual void Next() = 0;
            virtual void Prev() = 0;
            virtual void Jump(const KeyObject& next) = 0;
            virtual void JumpToFirst() = 0;
            virtual void JumpToLast() = 0;
            /*
             * 'clone_str' indicate that if the string part of key should clone or not
             * In some situation, if the string part of key cached for later use, the clone flag should be setting to true.
             */
            virtual KeyObject& Key(bool clone_str = false) = 0;
            virtual Slice RawKey() = 0;
            virtual Slice RawValue() = 0;
            virtual ValueObject& Value(bool clone_str = false) = 0;

            /*
             * Delete key/value pair at current iterator position.
             * It's more efficient, and the only right way to delete data in iterator for some engine(forestdb).
             */
            virtual void Del() = 0;
            virtual ~Iterator()
            {
            }
    };

    typedef const void* EngineSnapshot;

    struct FeatureSet
    {
            unsigned support_namespace :1;
            unsigned support_compactfilter :1;
            unsigned support_merge :1;
            unsigned support_backup :1;
            unsigned support_delete_range :1;
            FeatureSet() :
                    support_namespace(0), support_compactfilter(0), support_merge(0), support_backup(0), support_delete_range(
                            0)
            {
            }
    };

    class Engine
    {
        public:
            virtual int Init(const std::string& dir, const std::string& options) = 0;
            virtual int Repair(const std::string& dir) = 0;

            virtual int PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value) = 0;
            virtual int Put(Context& ctx, const KeyObject& key, const ValueObject& value) = 0;
            virtual int Get(Context& ctx, const KeyObject& key, ValueObject& value) = 0;
            virtual int Del(Context& ctx, const KeyObject& key) = 0;
            virtual int DelRange(Context& ctx, const KeyObject& start, const KeyObject& end)
            {
                return ERR_NOTSUPPORTED;
            }
            virtual int MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values,
                    ErrCodeArray& errs) = 0;
            virtual int Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& values) = 0;
            int Merge(Context& ctx, const KeyObject& key, uint16_t op, const Data& value)
            {
                return Merge(ctx, key, op, DataArray(1, value));
            }
            virtual bool Exists(Context& ctx, const KeyObject& key) = 0;

            virtual Iterator* Find(Context& ctx, const KeyObject& key) = 0;

            virtual int Compact(Context& ctx, const KeyObject& start, const KeyObject& end) = 0;
            virtual int CompactAll(Context& ctx);

            virtual int BeginWriteBatch(Context& ctx) = 0;
            virtual int CommitWriteBatch(Context& ctx) = 0;
            virtual int DiscardWriteBatch(Context& ctx) = 0;

            virtual int ListNameSpaces(Context& ctx, DataArray& nss) = 0;
            virtual int DropNameSpace(Context& ctx, const Data& ns) = 0;

            virtual int Flush(Context& ctx, const Data& ns)
            {
                return ERR_NOTSUPPORTED;
            }
            virtual int FlushAll(Context& ctx);

            virtual int BeginBulkLoad(Context& ctx)
            {
                return ERR_NOTSUPPORTED;
            }

            virtual int EndBulkLoad(Context& ctx)
            {
                return ERR_NOTSUPPORTED;
            }

            virtual int Backup(Context& ctx, const std::string& dir)
            {
                return ERR_NOTSUPPORTED;
            }
            virtual int Restore(Context& ctx, const std::string& dir)
            {
                return ERR_NOTSUPPORTED;
            }

            virtual int64_t EstimateKeysNum(Context& ctx, const Data& ns) = 0;
            virtual void Stats(Context& ctx, std::string& str) = 0;

            virtual const std::string GetErrorReason(int err) = 0;

            virtual const FeatureSet GetFeatureSet() = 0;

            virtual int Routine()
            {
                return ERR_NOTSUPPORTED;
            }

            virtual EngineSnapshot CreateSnapshot()
            {
                return NULL;
            }

            virtual void ReleaseSnapshot(EngineSnapshot s)
            {
            }

            virtual int MaxOpenFiles() = 0;

            virtual ~Engine()
            {
            }
    };

    struct WriteBatchGuard
    {
            Context& ctx;
            Engine* engine;
            int err;
            WriteBatchGuard(Context& c, Engine* e) :
                    ctx(c), engine(NULL), err(0)
            {
                int err = e->BeginWriteBatch(ctx);
                if (0 == err)
                {
                    engine = e;
                }
            }
            void MarkFailed(int errcode)
            {
                err = errcode;
            }
            ~WriteBatchGuard()
            {
                if (NULL != engine)
                {
                    if (0 == err)
                    {
                        err = engine->CommitWriteBatch(ctx);
                    }
                    else
                    {
                        engine->DiscardWriteBatch(ctx);
                    }
                    ctx.transc_err = err;
                }
            }
    };

    int compare_keys(const char* k1, size_t k1_len, const char* k2, size_t k2_len, bool has_ns);
    int compare_keyslices(const Slice& k1, const Slice& k2, bool has_ns);

    extern Engine* g_engine;
OP_NAMESPACE_END

#endif /* SRC_DB_ENGINE_HPP_ */
