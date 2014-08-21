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

#ifndef NOVA_SOCKETCHANNEL_HPP_
#define NOVA_SOCKETCHANNEL_HPP_
#include "channel/channel.hpp"
#include "util/socket_address.hpp"


namespace ardb
{
	enum SocketChannelState
	{
		SOCK_INVALID, SOCK_INIT, SOCK_CONNECTING, SOCK_CONNECTED, SOCK_CLOSED
	};

	enum SocketProtocolType
	{
		IPV4, IPV6, UNIX
	};

	class ChannelService;
	class SocketChannel: public Channel
	{
		protected:
			Address* m_localAddr;
			Address* m_remoteAddr;
			SocketChannelState m_state;
			const Address* GetLocalAddress();
			const Address* GetRemoteAddress();
			void setRemoteAddress(Address* addr);
			virtual int GetProtocol()
			{
				//default TCP
				return SOCK_STREAM;
			}
			int GetSocketFD(int domain);
			virtual bool DoOpen();
			virtual bool DoBind(Address* local);
			virtual bool DoConnect(Address* remote);
			virtual bool DoClose();
			virtual bool DoConfigure(const ChannelOptions& options);
			virtual int32 HandleExceptionEvent(int32 event);
			void OnWrite();

			void OnAccepted();
			friend class ChannelService;
		public:
			SocketChannel(ChannelService& factory) :
					Channel(NULL, factory), m_localAddr(NULL), m_remoteAddr(
							NULL), m_state(SOCK_INIT)
			{
			}
			std::string GetRemoteStringAddress();
			virtual ~SocketChannel();
	};
}

#endif /* SOCKETCHANNEL_HPP_ */
