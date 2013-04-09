/*
 * SocketHostAddress.hpp
 *
 *  Created on: 2011-1-8
 *      Author: wqy
 */

#ifndef NOVA_SOCKETHOSTADDRESS_HPP_
#define NOVA_SOCKETHOSTADDRESS_HPP_

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <stdint.h>
#include <string>
#include "common.hpp"

using std::string;

namespace ardb
{
	class SocketHostAddress: public Address
	{
		private:
			std::string m_host;
			uint16 m_port;
		public:
			SocketHostAddress(const char* host = "", uint16 port = 0) :
					m_host(host), m_port(port)
			{
			}
			SocketHostAddress(const string& host, uint16 port) :
					m_host(host), m_port(port)
			{
			}
			SocketHostAddress(const SocketHostAddress& other) :
					m_host(other.GetHost()), m_port(other.GetPort())
			{
			}
			const std::string& GetHost() const
			{
				return m_host;
			}
			uint16 GetPort() const
			{
				return m_port;
			}
			bool operator<(const SocketHostAddress& other) const
			{
				if (m_port < other.m_port)
				{
					return true;
				} else if (m_port > other.m_port)
				{
					return false;
				}
				return m_host < other.m_host;
			}
			virtual ~SocketHostAddress()
			{
			}
	};
}

#endif /* SOCKETHOSTADDRESS_HPP_ */
