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

#ifndef REDIS_COMMAND_HPP_
#define REDIS_COMMAND_HPP_

#include <deque>
#include <string>

namespace ardb
{
	namespace codec
	{
		class RedisCommandDecoder;
		typedef std::deque<std::string> ArgumentArray;
		class RedisCommandFrame
		{
			private:
				bool m_is_inline;
				bool m_cmd_seted;
				std::string m_cmd;
				ArgumentArray m_args;
				inline void FillNextArgument(Buffer& buf, size_t len)
				{
					const char* str = buf.GetRawReadBuffer();
					buf.AdvanceReadIndex(len);
					if (m_cmd_seted)
					{
						m_args.push_back(std::string(str, len));
					}
					else
					{
						m_cmd.append(str, len);
						m_cmd_seted = true;
					}
				}
				friend class RedisCommandDecoder;
			public:
				RedisCommandFrame() :
						m_is_inline(false), m_cmd_seted(false)
				{
				}
				RedisCommandFrame(ArgumentArray& cmd) :
						m_is_inline(false), m_cmd_seted(false)
				{
					m_cmd = cmd.front();
					cmd.pop_front();
					m_args = cmd;
				}
				bool IsInLine()
				{
					return m_is_inline;
				}
				ArgumentArray& GetArguments()
				{
					return m_args;
				}
				std::string& GetCommand()
				{
					return m_cmd;
				}
				std::string* GetArgument(uint32 index)
				{
					if (index >= m_args.size())
					{
						return NULL;
					}
					return &(m_args[index]);
				}
				void Clear()
				{
					m_cmd_seted = false;
					m_cmd.clear();
					m_args.clear();
				}
				~RedisCommandFrame()
				{

				}
		};
	}
}

#endif /* REDIS_COMMAND_HPP_ */
