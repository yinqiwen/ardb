/*
 * DatagramPacket.hpp
 *
 *  Created on: 2011-2-17
 *      Author: wqy
 */

#ifndef NOVA_DATAGRAMPACKET_HPP_
#define NOVA_DATAGRAMPACKET_HPP_

#include "socket_inet_address.hpp"
#include "buffer.hpp"
namespace rddb
{
	class DatagramPacket
	{
		private:
			SocketInetAddress m_addr;
			Buffer* m_buffer;
			bool m_is_buffer_self;
		public:
			DatagramPacket(uint32 size);
			DatagramPacket(Buffer* buffer,
					const SocketInetAddress& addr);
			Buffer& GetBuffer()
			{
				return *m_buffer;
			}
			SocketInetAddress& GetInetAddress()
			{
				return m_addr;
			}
			~DatagramPacket();
	};
}

#endif /* DATAGRAMPACKET_HPP_ */
