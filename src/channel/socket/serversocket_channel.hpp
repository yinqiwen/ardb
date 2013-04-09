/*
 * ServerSocketChannel.hpp
 *
 *  Created on: 2011-1-8
 *      Author: wqy
 */

#ifndef NOVA_SERVERSOCKETCHANNEL_HPP_
#define NOVA_SERVERSOCKETCHANNEL_HPP_

#include "channel/socket/socket_channel.hpp"
#include "util/socket_host_address.hpp"

namespace ardb
{
	class ServerSocketChannel;
	typedef int SocketAcceptedCallBack(ServerSocketChannel* server,
			ClientSocketChannel* client);
	class ServerSocketChannel: public SocketChannel
	{
		protected:
			uint32 m_connected_socks;
			SocketAcceptedCallBack* m_accepted_cb;
			bool DoBind(Address* local);
			bool DoConnect(Address* remote);
			bool DoConfigure(const ChannelOptions& options);
			void OnRead();
			void OnChildClose(Channel* ch);
			friend class ChannelService;
		public:
			ServerSocketChannel(ChannelService& factory);
			void SetSocketAcceptedCallBack(SocketAcceptedCallBack* cb)
			{
				m_accepted_cb = cb;
			}
			uint32 ConnectedSockets();
			~ServerSocketChannel();
	};
}

#endif /* SERVERSOCKETCHANNEL_HPP_ */
