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

#ifndef NOVA_NETWORKHELPER_HPP_
#define NOVA_NETWORKHELPER_HPP_
#include "socket_host_address.hpp"
#include "socket_inet_address.hpp"
#include "socket_unix_address.hpp"
#include <list>
#include <vector>

using std::list;

namespace ardb
{
    SocketInetAddress get_inet_address(const string& host, uint16 port);
    SocketInetAddress get_inet_address(const SocketHostAddress& addr);
    SocketInetAddress get_inet_address(const SocketUnixAddress& addr);
    SocketHostAddress get_host_address(const sockaddr& addr);
    SocketHostAddress get_host_address(const SocketInetAddress& addr);
    SocketUnixAddress get_unix_address(const sockaddr& addr);
    SocketUnixAddress get_unix_address(const SocketInetAddress& addr);

    SocketInetAddress
    get_socket_inet_address(int32 fd);
    SocketHostAddress
    get_host_address(int32 fd);
    SocketInetAddress
    get_remote_inet_address(int32 fd);
    SocketHostAddress
    get_remote_host_address(int32 fd);

    uint64 ntoh_u64(uint64 v);
    uint64 hton_u64(uint64 v);

    int get_ip_by_nic_name(const std::string& ifName, std::string& ip);
    int get_local_host_ip_list(std::vector<std::string>& iplist);
    int get_local_host_ipv4(std::string& ip);
    bool is_local_ip(const std::string& ip);
}

#endif /* NETWORKHELPER_HPP_ */
