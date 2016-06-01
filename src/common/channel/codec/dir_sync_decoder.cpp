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
#include "dir_sync_decoder.hpp"


#define STATE_DIR_SYNC_WAITING_DIR_HEADER 0
#define STATE_DIR_SYNC_WAITING_FILE_HEADER 1
#define STATE_DIR_SYNC_WAITING_FILE_CONTENT 2

namespace ardb
{
    namespace codec
    {
        bool DirSyncDecoder::Decode(ChannelHandlerContext& ctx, Channel* channel, Buffer& buffer, DirSyncStatus& s)
        {
            while (buffer.Readable())
            {
                switch (_state)
                {
                    case STATE_DIR_SYNC_WAITING_DIR_HEADER:
                    {
                        if (!BufferHelper::ReadFixInt64(buffer, _rest_file_num))
                        {
                            return false;
                        }
                        _state = STATE_DIR_SYNC_WAITING_FILE_HEADER;
                        s.status = _state;
                        continue;
                    }
                    case STATE_DIR_SYNC_WAITING_FILE_HEADER:
                    {
                        std::string file;
                        if (!BufferHelper::ReadVarString(buffer, file) || !BufferHelper::ReadFixInt64(buffer, _current_file_rest_bytes))
                        {
                            return false;
                        }
                        if (-1 != _current_fd)
                        {
                            close(_current_fd);
                            _current_fd = -1;
                        }

                        _state = STATE_DIR_SYNC_WAITING_FILE_CONTENT;
                        s.status = _state;
                        continue;
                    }
                    case STATE_DIR_SYNC_WAITING_FILE_CONTENT:
                    {
                        size_t write_len = _current_file_rest_bytes;
                        if (buffer.ReadableBytes() < write_len)
                        {
                            write_len = buffer.ReadableBytes();
                        }
                        int n = write(_current_fd, (const void*) (buffer.GetRawReadBuffer()), write_len);
                        if (n > 0)
                        {
                            _current_file_rest_bytes -= n;
                            buffer.AdvanceReadIndex(n);
                        }
                        else
                        {
                            return false;
                        }
                        if (0 == _current_file_rest_bytes)
                        {
                            close(_current_fd);
                            _current_fd = -1;
                            _state = STATE_DIR_SYNC_WAITING_FILE_HEADER;
                            _rest_file_num--;
                        }
                        s.status = _state;
                        continue;
                    }
                    default:
                    {
                        return false;
                    }
                }
            }
            return true;
        }
    }
}

