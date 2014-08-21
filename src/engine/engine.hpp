/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
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

#ifndef ENGINE_HPP_
#define ENGINE_HPP_

#include "common/common.hpp"
#include "slice.hpp"

OP_NAMESPACE_BEGIN
    struct Iterator
    {
            virtual void Next() = 0;
            virtual void Prev() = 0;
            virtual Slice Key() const = 0;
            virtual Slice Value() const = 0;
            virtual bool Valid() = 0;
            virtual void SeekToFirst() = 0;
            virtual void SeekToLast() = 0;
            virtual void Seek(const Slice& target) = 0;
            virtual ~Iterator()
            {
            }
    };

    struct Options
    {
            bool read_fill_cache;
            bool seek_fill_cache;
            Options() :
                    read_fill_cache(true),seek_fill_cache(false)
            {
            }
    };

    struct KeyValueEngine
    {
            virtual int Get(const Slice& key, std::string* value, const Options& options) = 0;
            virtual int Put(const Slice& key, const Slice& value, const Options& options) = 0;
            virtual int Del(const Slice& key, const Options& options) = 0;
            virtual int BeginBatchWrite() = 0;
            virtual int CommitBatchWrite() = 0;
            virtual int DiscardBatchWrite() = 0;
            virtual Iterator* Find(const Slice& findkey, const Options& options) = 0;
            virtual int MaxOpenFiles() = 0;

            virtual const std::string Stats()
            {
                return "";
            }
            virtual void CompactRange(const Slice& begin, const Slice& end)
            {
            }
            virtual ~KeyValueEngine()
            {
            }
    };

    struct KeyValueEngineFactory
    {
            virtual const std::string GetName() = 0;
            virtual KeyValueEngine* CreateDB(const std::string& name) = 0;
            virtual void CloseDB(KeyValueEngine* engine) = 0;
            virtual void DestroyDB(KeyValueEngine* engine) = 0;
            virtual ~KeyValueEngineFactory()
            {
            }
    };

    struct CommonComparator
    {
            static int Compare(const char* akbuf, size_t aksiz, const char* bkbuf, size_t bksiz);
    };

OP_NAMESPACE_END

#endif /* ENGINE_HPP_ */
