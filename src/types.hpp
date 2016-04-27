/*
 *Copyright (c) 2013-2015, yinqiwen <yinqiwen@gmail.com>
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

#ifndef ARDB_TYPES_HPP_
#define ARDB_TYPES_HPP_

#include "common/common.hpp"
#include "buffer/buffer.hpp"
#include <vector>
#include <string>
#include <map>
#include <assert.h>
#include <stddef.h>
#include <string.h>
#include "constants.hpp"
OP_NAMESPACE_BEGIN

    typedef std::vector<std::string> StringArray;
    typedef TreeMap<std::string, std::string>::Type StringStringMap;
    typedef TreeSet<std::string>::Type StringTreeSet;
    typedef TreeMap<std::string, double>::Type StringDoubleMap;

    class Slice
    {
        public:
            // Create an empty slice.
            Slice() :
                    data_(""), size_(0)
            {
            }
            Slice(const Slice& s) :
                    data_(s.data()), size_(s.size())
            {
            }

            // Create a slice that refers to d[0,n-1].
            Slice(const char* d, size_t n) :
                    data_(d), size_(n)
            {
            }

            // Create a slice that refers to the contents of "s"
            Slice(const std::string& s) :
                    data_(s.data()), size_(s.size())
            {
            }

            // Create a slice that refers to s[0,strlen(s)-1]
            Slice(const char* s) :
                    data_(s), size_(strlen(s))
            {
            }

            // Return a pointer to the beginning of the referenced data
            const char* data() const
            {
                return data_;
            }

            // Return the length (in bytes) of the referenced data
            size_t size() const
            {
                return size_;
            }

            // Return true iff the length of the referenced data is zero
            bool empty() const
            {
                return size_ == 0;
            }

            // Return the ith byte in the referenced data.
            // REQUIRES: n < size()
            char operator[](size_t n) const
            {
                assert(n < size());
                return data_[n];
            }

            // Change this slice to refer to an empty array
            void clear()
            {
                data_ = "";
                size_ = 0;
            }

            // Drop the first "n" bytes from this slice.
            void remove_prefix(size_t n)
            {
                assert(n <= size());
                data_ += n;
                size_ -= n;
            }

            // Return a string that contains the copy of the referenced data.
            std::string ToString() const
            {
                return std::string(data_, size_);
            }

            // Three-way comparison.  Returns value:
            //   <  0 iff "*this" <  "b",
            //   == 0 iff "*this" == "b",
            //   >  0 iff "*this" >  "b"
            int compare(const Slice& b) const;

            // Return true iff "x" is a prefix of "*this"
            bool starts_with(const Slice& x) const
            {
                return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
            }

        private:
            const char* data_;
            size_t size_;

            // Intentionally copyable
    };

    inline bool operator==(const Slice& x, const Slice& y)
    {
        return ((x.size() == y.size()) && (memcmp(x.data(), y.data(), x.size()) == 0));
    }

    inline bool operator!=(const Slice& x, const Slice& y)
    {
        return !(x == y);
    }

    inline bool operator<(const Slice& x, const Slice& y)
    {
        return x.compare(y) < 0;
    }
    inline int Slice::compare(const Slice& b) const
    {
        const int min_len = (size_ < b.size_) ? size_ : b.size_;
        int r = memcmp(data_, b.data_, min_len);
        if (r == 0)
        {
            if (size_ < b.size_)
                r = -1;
            else if (size_ > b.size_)
                r = +1;
        }
        return r;
    }

    struct Data
    {
            int64_t data;
            uint32 len;
            uint8_t encoding;
            Data();
            Data(const std::string& v, bool try_int_encoding);
            Data(const Data& data);
            Data(int64_t v);
            Data(double v);
            static Data WrapCStr(const std::string& str);
            Data& operator=(const Data& data);
            ~Data();

            void* ReserveStringSpace(size_t size);
            void Encode(Buffer& buf) const;
            bool Decode(Buffer& buf, bool clone_str);

            void SetString(const std::string& str, bool try_int_encoding);
            void SetString(const char* str, size_t len, bool clone);
            void SetString(const std::string& str, bool try_int_encoding, bool clone);
            void SetInt64(int64 v);
            void SetFloat64(double v);
            int64 GetInt64() const;
            double GetFloat64() const;

            void Clone(const Data& data);
            int Compare(const Data& other, bool alpha_cmp = false) const;
            bool operator <(const Data& other) const
            {
                return Compare(other) < 0;
            }
            bool operator <=(const Data& other) const
            {
                return Compare(other) <= 0;
            }
            bool operator ==(const Data& other) const
            {
                return Compare(other) == 0;
            }
            bool operator >=(const Data& other) const
            {
                return Compare(other) >= 0;
            }
            bool operator >(const Data& other) const
            {
                return Compare(other) > 0;
            }
            bool operator !=(const Data& other) const
            {
                return Compare(other) != 0;
            }
            bool IsInteger() const;
            bool IsFloat() const;
            bool IsNumber() const
            {
                return IsInteger() || IsFloat();
            }
            bool IsNil() const;
            bool IsString() const;
            bool IsCStr() const;
            uint32 StringLength() const;
            void Clear();
            const char* CStr() const;
            const std::string& ToString(std::string& str) const;
            std::string AsString() const
            {
                std::string str;
                return ToString(str);
            }
            char* ToMutableStr();

    };

    struct DataHash
    {
            size_t operator()(const Data& t) const;
    };
    struct DataEqual
    {
            bool operator()(const Data& s1, const Data& s2) const;
    };

    typedef std::vector<Data> DataArray;
    typedef TreeSet<Data>::Type DataSet;
    typedef TreeMap<Data, double>::Type DataScoreMap;

    template<typename T>
    struct PointerArray: public std::vector<T>
    {
        public:
            ~PointerArray()
            {
                for(size_t i = 0 ; i < std::vector<T>::size(); i++)
                {
                    delete(std::vector<T>::at(i));
                }
            }
    };

OP_NAMESPACE_END

#endif /* SRC_TYPES_HPP_ */
