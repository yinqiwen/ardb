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
	class ServerSocketChannel: public SocketChannel
	{
		protected:
			uint32 m_connected_socks;
			bool DoBind(Address* local);
			bool DoConnect(Address* remote);
			bool DoConfigure(const ChannelOptions& options);
			void OnRead();
			void OnChildClose(Channel* ch);
			friend class ChannelService;
		public:
			ServerSocketChannel(ChannelService& factory);
			uint32 ConnectedSockets();
			~ServerSocketChannel();
	};
}

#endif /* SERVERSOCKETCHANNEL_HPP_ */
