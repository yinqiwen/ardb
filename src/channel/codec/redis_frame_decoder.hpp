/*
 * redis_frame_decoder.hpp
 *
 *  Created on: 2011-7-14
 *      Author: qiyingwang
 */

#ifndef REDIS_FRAME_DECODER_HPP_
#define REDIS_FRAME_DECODER_HPP_

#include "channel/all_includes.hpp"
#include "channel/codec/frame_decoder.hpp"
#include <vector>
#include <string>

namespace rddb
{
    namespace codec
    {
        class RedisFrameDecoder;
        class RedisCommandFrame
        {
            public:
                typedef std::vector<Buffer*> ArgumentArray;
            private:
                bool m_is_inline;
                ArgumentArray m_args;
                Buffer* GetNextArgument(size_t len);
                friend class RedisFrameDecoder;
            public:
                RedisCommandFrame() :
                    m_is_inline(false)
                {
                }
                bool IsInLine()
                {
                    return m_is_inline;
                }
                ArgumentArray& GetArguments()
                {
                    return m_args;
                }
                Buffer* GetArgument(uint32 index);
                void Clear();
                int DecodeFromBuffer(Buffer& buf);
                int EncodeToBuffer(Buffer& buf);
                int EncodeRawProtocol(Buffer& buf);
                //int EncodeToRawCommandString(std::string& str);
                ~RedisCommandFrame();
        };

        //typedef std::list<std::string> RedisCommandFrame;
        class RedisFrameDecoder: public FrameDecoder<RedisCommandFrame>
        {
            protected:
                int ProcessInlineBuffer(ChannelHandlerContext& ctx,
                        Buffer& buffer, RedisCommandFrame* frame);
                int ProcessMultibulkBuffer(ChannelHandlerContext& ctx,
                        Buffer& buffer, RedisCommandFrame* frame);
                FrameDecodeResult<RedisCommandFrame> Decode(
                        ChannelHandlerContext& ctx, Channel* channel,
                        Buffer& buffer);
            public:

                RedisFrameDecoder()
                {
                }

        };
    }
}

#endif /* REDIS_FRAME_DECODER_HPP_ */
