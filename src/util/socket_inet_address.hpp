/*
 * SocketInetAddress.hpp
 *
 *  Created on: 2011-1-8
 *      Author: wqy
 */

#ifndef NOVA_SOCKETINETADDRESS_HPP_
#define NOVA_SOCKETINETADDRESS_HPP_

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/un.h>
#include <net/if.h>
#include <stdint.h>
#include <string.h>
#include "common.hpp"

namespace ardb
{
	class SocketInetAddress: public Address
	{
		private:
			char m_addrbuf[sizeof(sockaddr_un)];
			//				struct sockaddr_in* m_addr4;
			//				struct sockaddr_in6* m_addr6;
			//				struct sockaddr_un* m_addru;
		public:
			SocketInetAddress(const sockaddr* addr, uint8 size)
			{
				memcpy(m_addrbuf, &addr, size);
			}
			SocketInetAddress(const struct sockaddr_in& addr)
			{
				memcpy(m_addrbuf, &addr, sizeof(addr));
			}
			SocketInetAddress(const struct sockaddr_in6& addr)
			{
				memcpy(m_addrbuf, &addr, sizeof(addr));
			}
			SocketInetAddress(const struct sockaddr_un& addr)
			{
				memcpy(m_addrbuf, &addr, sizeof(addr));
			}
			SocketInetAddress()
			{
				memset(&m_addrbuf, 0, sizeof(struct sockaddr_in));
			}

			const sockaddr& GetRawSockAddr() const
			{
				struct sockaddr* p = (struct sockaddr*) m_addrbuf;
				return *p;
			}

			int GetRawSockAddrSize() const
			{
				if (IsIPV6())
				{
					return sizeof(sockaddr_in6);
				} else if (IsUnix())
				{
					return sizeof(sockaddr_un);
				}
				return sizeof(sockaddr_in);
			}
			bool IsIPV6() const
			{
				struct sockaddr* p = (struct sockaddr*) m_addrbuf;
				return AF_INET6 == p->sa_family;
			}
			bool IsUnix() const
			{
				struct sockaddr* p = (struct sockaddr*) m_addrbuf;
				return AF_UNIX == p->sa_family;
			}
			int GetDomain() const
			{
				if (IsIPV6())
				{
					return AF_INET6;
				} else if (IsUnix())
				{
					return AF_UNIX;
				}
				return AF_INET;
			}
	};
}

#endif /* SOCKETINETADDRESS_HPP_ */
