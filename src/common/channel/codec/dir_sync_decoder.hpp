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

#ifndef COMMON_CHANNEL_CODEC_DIR_SYNC_DECODER_HPP_
#define COMMON_CHANNEL_CODEC_DIR_SYNC_DECODER_HPP_

#include "channel/codec/stack_frame_decoder.hpp"
#include <stdio.h>

using namespace ardb;
using ardb::Buffer;
namespace ardb
{
    namespace codec
    {
        struct DirSyncStatus
        {
                std::string path;
                std::string reason;
                int status;
                int err;
                DirSyncStatus() :
                        status(0), err(0)
                {
                }
                void Clear()
                {
                    path.clear();
                    reason.clear();
                    status = 0;
                    err = 0;
                }
                bool IsComplete() const;
                bool IsError() const;
                bool IsSuccess() const;
                bool IsItemSuccess() const;
        };
        /*
         * A dir sync decoder used to sync dir from remote server
         */
        class RedisMessageDecoder;
        class DirSyncDecoder: public StackFrameDecoder<DirSyncStatus>
        {
            private:
                std::string _dir;
                std::string _current_fname;
                FILE* _current_file;
                int64_t _current_file_rest_bytes;
                int64_t _rest_file_num;
                uint8 _state;
                bool Decode(ChannelHandlerContext& ctx, Channel* channel, Buffer& buffer, DirSyncStatus& msg);
                void CloseCurrentFile();
                friend class RedisMessageDecoder;
            public:
                DirSyncDecoder(const std::string& dir = "./") :
                        _dir(dir),_current_file(NULL),_current_file_rest_bytes(0),_rest_file_num(0), _state(0)
                {
                }
                void SetSyncBaseDir(const std::string& dir)
                {
                    _current_fname.clear();
                    _current_file_rest_bytes = 0;
                    _rest_file_num = 0;
                    _state = 0;
                    _dir = dir;
                }
                ~DirSyncDecoder();
        };
    }
}

#endif /* SRC_COMMON_CHANNEL_CODEC_DIR_SYNC_DECODER_HPP_ */
