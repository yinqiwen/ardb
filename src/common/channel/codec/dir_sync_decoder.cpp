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
#include "channel/all_includes.hpp"
#include "dir_sync_decoder.hpp"

#define STATE_DIR_SYNC_WAITING_DIR_HEADER    0
#define STATE_DIR_SYNC_WAITING_FILE_HEADER   1
#define STATE_DIR_SYNC_WAITING_FILE_CONTENT  2
#define STATE_DIR_SYNC_ITEM_SUCCESS          3
#define STATE_DIR_SYNC_SUCCESS               4
#define STATE_DIR_SYNC_FAILED                5

namespace ardb
{
    namespace codec
    {
        bool DirSyncStatus::IsItemSuccess() const
        {
            return status == STATE_DIR_SYNC_ITEM_SUCCESS;
        }
        bool DirSyncStatus::IsComplete() const
        {
            return IsSuccess() || IsError();
        }
        bool DirSyncStatus::IsSuccess() const
        {
            return status == STATE_DIR_SYNC_SUCCESS;
        }
        bool DirSyncStatus::IsError() const
        {
            return status == STATE_DIR_SYNC_FAILED;
        }
        void DirSyncDecoder::CloseCurrentFile()
        {
            if (NULL != _current_file)
            {
                fclose(_current_file);
                _current_file = NULL;
            }
        }
        bool DirSyncDecoder::Decode(ChannelHandlerContext& ctx, Channel* channel, Buffer& buffer, DirSyncStatus& s)
        {
            s.path = _current_fname;
            while (buffer.Readable())
            {
                size_t mark = buffer.GetReadIndex();
                switch (_state)
                {
                    case STATE_DIR_SYNC_WAITING_DIR_HEADER:
                    {
                        while (buffer.Readable() &&  buffer.GetRawReadBuffer()[0] == '\n')
                        {
                            buffer.AdvanceReadIndex(1);
                        }
                        if (buffer.Readable())
                        {
                            if(buffer.GetRawReadBuffer()[0] != '#')
                            {
                                s.err = -1;
                                s.reason = "invalid start char, should be '#' started";
                                s.status = STATE_DIR_SYNC_FAILED;
                                ERROR_LOG("Failed to start sync backup:%s", s.reason.c_str());
                                return true;
                            }
                            buffer.AdvanceReadIndex(1);
                        }
                        else
                        {
                            return false;
                        }
                        if (!BufferHelper::ReadFixInt64(buffer, _rest_file_num))
                        {
                            buffer.SetReadIndex(mark);
                            return false;
                        }
                        if (_rest_file_num <= 0)
                        {
                            s.status = STATE_DIR_SYNC_SUCCESS;
                            return true;
                        }
                        else
                        {
                            _state = STATE_DIR_SYNC_WAITING_FILE_HEADER;
                            s.status = _state;
                            continue;
                        }
                    }
                    case STATE_DIR_SYNC_WAITING_FILE_HEADER:
                    {
                        std::string file;
                        if (buffer.ReadableBytes() < 12)
                        {
                            return false;
                        }
                        if (!BufferHelper::ReadVarString(buffer, file) || !BufferHelper::ReadFixInt64(buffer, _current_file_rest_bytes))
                        {
                            buffer.SetReadIndex(mark);
                            return false;
                        }
                        CloseCurrentFile();
                        _current_fname = file;
                        std::string path = _dir + "/" + file;
                        make_file(path);
                        if (_current_file_rest_bytes > 0)
                        {
                            _current_file = fopen(path.c_str(), "w");
                            if (_current_file == NULL)
                            {
                                s.err = errno;
                                s.reason = strerror(s.err);
                                s.status = STATE_DIR_SYNC_FAILED;
                                ERROR_LOG("Failed to open sync backup file:%s to write", path.c_str());
                                return true;
                            }
                        }
                        _state = STATE_DIR_SYNC_WAITING_FILE_CONTENT;
                        s.status = _state;
                        continue;
                    }
                    case STATE_DIR_SYNC_WAITING_FILE_CONTENT:
                    {
                        if (_current_file_rest_bytes > 0)
                        {
                            size_t write_len = _current_file_rest_bytes;
                            if (buffer.ReadableBytes() < write_len)
                            {
                                write_len = buffer.ReadableBytes();
                            }
                            int n = fwrite((const void*) (buffer.GetRawReadBuffer()), write_len, 1, _current_file);
                            if (n > 0)
                            {
                                _current_file_rest_bytes -= write_len;
                                buffer.AdvanceReadIndex(write_len);
                            }
                            else
                            {
                                s.err = errno;
                                s.reason = strerror(s.err);
                                s.status = STATE_DIR_SYNC_FAILED;
                                return true;
                            }
                        }
                        if (0 == _current_file_rest_bytes)
                        {
                            CloseCurrentFile();
                            _rest_file_num--;
                            if (_rest_file_num == 0)
                            {
                                _state = STATE_DIR_SYNC_SUCCESS;
                                s.status = _state;
                            }
                            else
                            {
                                _state = STATE_DIR_SYNC_ITEM_SUCCESS;
                                s.status = _state;
                            }
                            return true;
                        }
                        continue;
                    }
                    case STATE_DIR_SYNC_ITEM_SUCCESS:
                    {
                        _state = STATE_DIR_SYNC_WAITING_FILE_HEADER;
                        s.status = _state;
                        continue;
                    }
                    default:
                    {
                        ERROR_LOG("Invalid state:%d", _state);
                        return false;
                    }
                }
            }
            return true;
        }

        DirSyncDecoder::~DirSyncDecoder()
        {
            CloseCurrentFile();
        }
    }
}

