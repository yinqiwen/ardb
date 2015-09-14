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

#include "codec.hpp"
#include "buffer/buffer_helper.hpp"
#include "buffer/struct_codec_macros.hpp"
#include <cmath>

OP_NAMESPACE_BEGIN

    enum DataEncoding
    {
        E_INT64 = 1, E_FLOAT64 = 2, E_CSTR = 3, E_SDS = 4
    };

    Data::Data() :
            encoding(0),len(0)
    {
        //value.iv = 0;
    }
    Data::Data(const std::string& v, bool try_int_encoding) :
            encoding(0)
    {
        SetString(v, try_int_encoding);
    }
    Data::~Data()
    {
        Clear();
    }

    Data::Data(const Data& other) :
            encoding(other.encoding), len(other.len)
    {
        Clone(other);
    }

    Data& Data::operator=(const Data& data)
    {
        return *this;
    }

    bool Data::Encode(Buffer& buf) const;
    bool Data::Decode(Buffer& buf);

    void Data::SetString(const std::string& str, bool try_int_encoding)
    {

    }
    double Data::NumberValue() const;
    void Data::SetInt64(int64 v)
    {
        Clear();
    }
    void Data::SetDouble(double v);
    bool Data::GetInt64(int64& v) const;
    bool GetDouble(double& v) const
    {
        //if()
    }

    void Data::Clone(const Data& other)
    {
        Clear();
        encoding = other.encoding;
        len = other.len;
        memcpy(data, other.data, sizeof(data));
        if(encoding == E_SDS)
        {
            void* s = malloc(other.len);
            memcpy(s, (char*)other.data, other.len);
            *(void**) data = s;
        }
    }
    int Data::Compare(const Data& other) const
    {

    }

    bool Data::IsInteger() const
    {
        return encoding == E_INT64;
    }
    uint32 Data::StringLength() const
    {
        return len;
    }
    void Data::Clear()
    {
        if(encoding == E_SDS)
        {
            free((char*)data);
        }
        encoding = 0;
    }
    const std::string& Data::ToString(std::string& str)const
    {

    }


OP_NAMESPACE_END

