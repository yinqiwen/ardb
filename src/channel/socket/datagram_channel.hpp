/*
 * DatagramChannel.hpp
 *
 *  Created on: 2011-1-9
 *      Author: wqy
 */

#ifndef NOVA_DATAGRAMCHANNEL_HPP_
#define NOVA_DATAGRAMCHANNEL_HPP_
#include "channel/socket/socket_channel.hpp"
namespace ardb
{
	class DatagramChannel: public SocketChannel
	{
		protected:
			void OnRead();
			int GetProtocol()
			{
				//UDP
				return SOCK_DGRAM;
			}
		public:
			DatagramChannel(ChannelService& factory) :
					SocketChannel(factory)
			{
			}
			int32 Send(const SocketInetAddress& addr, Buffer* buffer);
			int32
			Receive(SocketInetAddress& addr, Buffer* buffer);
			~DatagramChannel();
	};
}

#endif /* DATAGRAMCHANNEL_HPP_ */
