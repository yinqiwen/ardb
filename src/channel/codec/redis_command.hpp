/*
 * redis_command.hpp
 *
 *  Created on: 2013-4-21
 *      Author: wqy
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
