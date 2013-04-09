/*
 * socket_unix_address.hpp
 *
 *  Created on: 2011-7-22
 *      Author: wqy
 */

#ifndef SOCKET_UNIX_ADDRESS_HPP_
#define SOCKET_UNIX_ADDRESS_HPP_
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <stdint.h>
#include <string.h>
#include "common.hpp"
#include <string>

namespace ardb
{
	class SocketUnixAddress: public Address
	{
		private:
			std::string m_path;
		public:
			SocketUnixAddress(const std::string& path) :
					m_path(path)
			{
			}
			SocketUnixAddress(const SocketUnixAddress& other) :
					m_path(other.GetPath())
			{
			}
			SocketUnixAddress()
			{
			}
			const std::string& GetPath() const
			{
				return m_path;
			}
	};
}

#endif /* SOCKET_UNIX_ADDRESS_HPP_ */
