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

/*
 * redis_message_decoder.hpp
 *
 *  Created on: 2013-9-4
 *      Author: wqy
 */

#ifndef REDIS_MESSAGE_DECODER_HPP_
#define REDIS_MESSAGE_DECODER_HPP_
#include "redis_command_codec.hpp"
#include "redis_reply_codec.hpp"
#include "dir_sync_decoder.hpp"
#include "redis_reply.hpp"
#include "redis_command.hpp"

#define REDIS_COMMAND_DECODER_TYPE 1
#define REDIS_REPLY_DECODER_TYPE 2
#define REDIS_DUMP_DECODER_TYPE 3
#define ARDB_DIR_SYNC_DECODER_TYPE 4

namespace ardb
{
    namespace codec
    {
        struct RedisMessage
        {
                RedisReply reply;
                RedisCommandFrame command;
                RedisDumpFileChunk chunk;
                DirSyncStatus backup;
                uint8 type;
                RedisMessage() :
                        type(REDIS_COMMAND_DECODER_TYPE)
                {
                }
                bool IsReply()
                {
                    return type == REDIS_REPLY_DECODER_TYPE;
                }
                bool IsCommand()
                {
                    return type == REDIS_COMMAND_DECODER_TYPE;
                }
                bool IsDumpFile()
                {
                    return type == REDIS_DUMP_DECODER_TYPE;
                }
                bool IsBackupSync()
                {
                    return type == ARDB_DIR_SYNC_DECODER_TYPE;
                }
        };

        class RedisMessageDecoder: public StackFrameDecoder<RedisMessage>
        {
            protected:
                uint8 m_decoder_type;
                RedisCommandDecoder m_cmd_decoder;
                RedisReplyDecoder m_reply_decoder;
                RedisDumpFileChunkDecoder m_dump_file_decoder;
                DirSyncDecoder m_backup_sync_decoder;
                bool Decode(ChannelHandlerContext& ctx, Channel* channel, Buffer& buffer, RedisMessage& msg)
                {
                    msg.type = m_decoder_type;
                    if (msg.IsReply())
                    {
                        return m_reply_decoder.Decode(ctx, channel, buffer, msg.reply);
                    }
                    else if (msg.IsCommand())
                    {
                        return m_cmd_decoder.Decode(ctx, channel, buffer, msg.command);
                    }
                    else if(msg.IsBackupSync())
                    {
                        msg.backup.Clear();
                        return m_backup_sync_decoder.Decode(ctx, channel, buffer, msg.backup);
                    }
                    else
                    {
                        return m_dump_file_decoder.Decode(ctx, channel, buffer, msg.chunk);
                    }
                }
            public:
                RedisMessageDecoder() :
                        m_decoder_type(REDIS_COMMAND_DECODER_TYPE)
                {
                }

                void SwitchToCommandDecoder()
                {
                    m_decoder_type = REDIS_COMMAND_DECODER_TYPE;
                }
                void SwitchToReplyDecoder()
                {
                    m_decoder_type = REDIS_REPLY_DECODER_TYPE;
                }
                void SwitchToDumpFileDecoder()
                {
                    m_decoder_type = REDIS_DUMP_DECODER_TYPE;
                }
                void SwitchToBackupSyncDecoder(const std::string& basedir)
                {
                    m_decoder_type = ARDB_DIR_SYNC_DECODER_TYPE;
                    m_backup_sync_decoder.SetSyncBaseDir(basedir);
                }
        };
    }
}

#endif /* REDIS_MESSAGE_DECODER_HPP_ */
