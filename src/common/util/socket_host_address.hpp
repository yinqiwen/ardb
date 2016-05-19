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
            bool operator==(const SocketHostAddress& other) const
            {
                return m_port == other.m_port && m_host == other.m_host;
            }
            bool operator<(const SocketHostAddress& other) const
            {
                if (m_port < other.m_port)
                {
                    return true;
                }
                else if (m_port > other.m_port)
                {
                    return false;
                }
                return m_host < other.m_host;
            }
            const std::string& ToString(std::string& str) const
            {
                char tmp[100];
                snprintf(tmp, sizeof(tmp), "%u", m_port);
                str.append(m_host).append(":").append(tmp);
                return str;
            }
            virtual ~SocketHostAddress()
            {
            }
    };
}

#endif /* SOCKETHOSTADDRESS_HPP_ */
