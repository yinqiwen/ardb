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
#include "thread/spin_mutex_lock.hpp"
#include "thread/lock_guard.hpp"
#include "db_utils.hpp"
#include "util/file_helper.hpp"
#include "db.hpp"

#define DEFAULT_LOCAL_ENCODE_BUFFER_SIZE 8192
OP_NAMESPACE_BEGIN

    Slice DBLocalContext::GetSlice(const KeyObject& key)
    {
        Buffer& key_encode_buffer = GetEncodeBufferCache();
        return key.Encode(key_encode_buffer, false, g_engine->GetFeatureSet().support_namespace?false:true);
    }
    void DBLocalContext::GetSlices(const KeyObject& key, const ValueObject& val, Slice ss[2])
    {
        Buffer& encode_buffer = GetEncodeBufferCache();
        key.Encode(encode_buffer, false, g_engine->GetFeatureSet().support_namespace?false:true);
        size_t key_len = encode_buffer.ReadableBytes();
        val.Encode(encode_buffer);
        size_t value_len = encode_buffer.ReadableBytes() - key_len;
        ss[0] = Slice(encode_buffer.GetRawBuffer(), key_len);
        ss[1] = Slice(encode_buffer.GetRawBuffer() + key_len, value_len);
    }

    Buffer& DBLocalContext::GetEncodeBufferCache()
    {
        encode_buffer_cache.Clear();
        encode_buffer_cache.Compact(DEFAULT_LOCAL_ENCODE_BUFFER_SIZE);
        return encode_buffer_cache;
    }

OP_NAMESPACE_END

