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

#ifndef ITERATOR_HPP_
#define ITERATOR_HPP_

#include "common/common.hpp"
#include "codec.hpp"
#include "engine/engine.hpp"
#include "channel/all_includes.hpp"
#include "cache/cache.hpp"

using namespace ardb::codec;
OP_NAMESPACE_BEGIN

    class Ardb;

    class KVIterator
    {
        protected:
            ValueObject* m_meta;
            Iterator* m_iter;
            KeyObject m_current_key;
            ValueObject m_current_value;
            Slice m_current_raw_key;
            Slice m_current_raw_value;
            friend class Ardb;
        public:
            KVIterator();
            void SetMeta(ValueObject* meta)
            {
                m_meta = meta;
            }
            void SetIter(Iterator* iter)
            {
                m_iter = iter;
            }
            Slice& CurrentRawKey()
            {
                return m_current_raw_key;
            }
            Slice& CurrentRawValue()
            {
                return m_current_raw_value;
            }
            virtual ~KVIterator();
    };

    struct BitsetIterator: public KVIterator
    {
            int64 Index();
            ValueObject& Value();
            std::string Bits();
            int64 Count();
            bool Next();
            bool Valid();
    };

    class ZSetIterator: public KVIterator
    {
        private:
            DataMap::iterator m_zip_iter;
            DataMap m_zipmap_clone;
            ZSetElementArray m_ziparray;
            bool m_iter_by_value;
            bool m_readonly;
            uint32 m_zip_cursor;
            ZSetDataCache* m_cache;
            ZSetDataSet::iterator m_cache_iter;
            friend class Ardb;
        public:
            ZSetIterator() :
                    m_iter_by_value(false), m_readonly(true),m_zip_cursor(0), m_cache(NULL)
            {
            }
            DataMap& GetZipMap()
            {
                if(m_readonly)
                {
                    return m_meta->meta.zipmap;
                }
                return m_zipmap_clone;
            }
            const Data* Element();
            const Data* Score();
            void GeoLocation(Location& loc);
            bool Next();
            bool Prev();
            ~ZSetIterator();
            void SeekScore(const Data& score);
            void Skip(uint32 step);
            bool Valid();
    };

    class SetIterator: public KVIterator
    {
        private:
            DataSet::iterator m_cache_iter;
            DataSet::iterator m_zip_iter;
            friend class Ardb;
        public:
            Data* Element();
            bool Next();
            bool Valid();
    };
    class HashIterator: public KVIterator
    {
        private:
            DataMap::iterator m_cache_iter;
            DataMap::iterator m_zip_iter;
            friend class Ardb;
        public:
            const Data* Field();
            Data* Value();
            bool Next();
            bool Valid();
    };

    class ListIterator: public KVIterator
    {
        private:
            uint32 m_zip_cursor;
            friend class Ardb;
        public:
            ListIterator();
            Data* Element();
            Data* Score();
            bool Next();
            bool Prev();
            bool Valid();
            ~ListIterator();
    };

OP_NAMESPACE_END

#endif /* ITERATOR_HPP_ */
