/*
 * ChannelException.hpp
 *
 *  Created on: 2011-1-10
 *      Author: qiyingwang
 */

#ifndef NOVA_CHANNELEXCEPTION_HPP_
#define NOVA_CHANNELEXCEPTION_HPP_
#include "common.hpp"
#include <string>

using std::string;
namespace ardb
{
	class ChannelException
	{
		private:
			string m_cause;
			int32 m_errno;
		public:
			inline ChannelException():m_errno(-1)
			{
			}
			inline ChannelException(const string& cause, int err = -1) :
					m_cause(cause), m_errno(err)
			{
			}
			inline int32 GetErrorNo()
			{
				return m_errno;
			}
			inline const string& GetCause()
			{
				return m_cause;
			}
	};
}

#endif /* CHANNELEXCEPTION_HPP_ */
