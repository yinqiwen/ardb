/*
 * ClientSocketChannel.hpp
 *
 *  Created on: 2011-1-9
 *      Author: wqy
 */

#ifndef NOVA_CLIENTSOCKETCHANNEL_HPP_
#define NOVA_CLIENTSOCKETCHANNEL_HPP_
#include "channel/socket/socket_channel.hpp"

namespace rddb
{
	class ServerSocketChannel;
	class ClientSocketChannel: public SocketChannel
	{
		protected:
			friend class ServerSocketChannel;
		public:
			ClientSocketChannel(ChannelService& factory) :
					SocketChannel(factory)
			{
			}
			~ClientSocketChannel();
	};
}

#endif /* CLIENTSOCKETCHANNEL_HPP_ */
