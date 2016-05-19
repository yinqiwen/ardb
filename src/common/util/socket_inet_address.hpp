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
            union MyAddrUnion
            {
                    char m_addrbuf[sizeof(sockaddr_un)];
                    struct sockaddr m_addr;
            } m_addr_union;

            //				struct sockaddr_in* m_addr4;
            //				struct sockaddr_in6* m_addr6;
            //				struct sockaddr_un* m_addru;
        public:
            SocketInetAddress(const sockaddr* addr, uint8 size)
            {
                memcpy(m_addr_union.m_addrbuf, &addr, size);
            }
            SocketInetAddress(const struct sockaddr_in& addr)
            {
                memcpy(m_addr_union.m_addrbuf, &addr, sizeof(addr));
            }
            SocketInetAddress(const struct sockaddr_in6& addr)
            {
                memcpy(m_addr_union.m_addrbuf, &addr, sizeof(addr));
            }
            SocketInetAddress(const struct sockaddr_un& addr)
            {
                memcpy(m_addr_union.m_addrbuf, &addr, sizeof(addr));
            }
            SocketInetAddress()
            {
                memset(&m_addr_union.m_addrbuf, 0, sizeof(struct sockaddr_in));
            }

            const sockaddr& GetRawSockAddr() const
            {
                return m_addr_union.m_addr;
            }

            int GetRawSockAddrSize() const
            {
                if (IsIPV6())
                {
                    return sizeof(sockaddr_in6);
                }
                else if (IsUnix())
                {
                    return sizeof(sockaddr_un);
                }
                return sizeof(sockaddr_in);
            }
            bool IsIPV6() const
            {
                return AF_INET6 == GetRawSockAddr().sa_family;
            }
            bool IsUnix() const
            {
                return AF_UNIX == GetRawSockAddr().sa_family;
            }
            int GetDomain() const
            {
                if (IsIPV6())
                {
                    return AF_INET6;
                }
                else if (IsUnix())
                {
                    return AF_UNIX;
                }
                return AF_INET;
            }
            const std::string& ToString(std::string& str) const
            {
                str.assign("raw_inet_addr");
                return str;
            }
    };
}

#endif /* SOCKETINETADDRESS_HPP_ */
