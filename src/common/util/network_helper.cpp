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

#include "network_helper.hpp"
#include "util/exception/api_exception.hpp"
#include <netdb.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <sys/un.h>
#include <net/if.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <ifaddrs.h>

namespace ardb
{

    static const uint16 kMaxNICCount = 16;
    SocketInetAddress get_inet_address(const SocketHostAddress& addr)
    {
        return get_inet_address(addr.GetHost().c_str(), addr.GetPort());
    }

    SocketInetAddress get_inet_address(const SocketUnixAddress& unix_addr)
    {
        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));

        addr.sun_family = AF_UNIX;
        strcpy(addr.sun_path, unix_addr.GetPath().c_str());
        //SocketInetAddress ret(addr);
        return SocketInetAddress(addr);
    }

    SocketInetAddress get_inet_address(const string& host, uint16 port)
    {
        struct addrinfo hints;
        struct addrinfo* res = NULL;
        //struct sockaddr addr;
        //memset(&addr, 0, sizeof(addr));
        memset(&hints, 0, sizeof(hints));
        hints.ai_flags = 0;
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = 0;
        hints.ai_protocol = 0;

        char szPort[10];
        sprintf(szPort, "%d", port);
        int retvalue = getaddrinfo(host.c_str(), szPort, &hints, &res);
        SocketInetAddress ret;
        if (0 != retvalue)
        {
            freeaddrinfo(res);
            //throw APIException(errno);
            return ret;
        }

        if (NULL != res)
        {
            struct sockaddr* addr = res->ai_addr;
            if (addr->sa_family == AF_INET)
            {
                sockaddr_in* in = (sockaddr_in*) (addr);
                ret = SocketInetAddress(*in);
            }
            else if (addr->sa_family == AF_INET6)
            {
                sockaddr_in6* in6 = (sockaddr_in6*) (addr);
                ret = SocketInetAddress(*in6);
            }
            else if (addr->sa_family == AF_UNIX)
            {
                sockaddr_un* un = (sockaddr_un*) (addr);
                ret = SocketInetAddress(*un);
            }
        }
        freeaddrinfo(res);
        return ret;
    }

    SocketHostAddress get_host_address(const sockaddr& addr)
    {
        SocketHostAddress invalid("", 0);
        int size;
        if (addr.sa_family == AF_INET || addr.sa_family != AF_INET6)
        {
            //ipv4
            char host[INET_ADDRSTRLEN + 1];
            size = sizeof(sockaddr_in);
            sockaddr_in* paddr = (sockaddr_in*) (&addr);
            if (NULL != inet_ntop(AF_INET, &(paddr->sin_addr), host, size))
            {
                return SocketHostAddress(host, ntohs(paddr->sin_port));
            }
            return invalid;
        }
        else
        {
            //ipv6
            char host[INET6_ADDRSTRLEN + 1];
            size = sizeof(sockaddr_in6);
            sockaddr_in6* paddr = (sockaddr_in6*) (&addr);
            if (NULL != inet_ntop(AF_INET6, &(paddr->sin6_addr), host, size))
            {
                sockaddr_in6* paddr = (sockaddr_in6*) (&addr);
                return SocketHostAddress(host, ntohs(paddr->sin6_port));
            }
            return invalid;
        }
    }

    SocketUnixAddress get_unix_address(const sockaddr& addr)
    {
        struct sockaddr_un* pun = (struct sockaddr_un*) &addr;
        const char* path = pun->sun_path;
        return SocketUnixAddress(path);
    }
    SocketUnixAddress get_unix_address(const SocketInetAddress& addr)
    {
        return get_unix_address(addr.GetRawSockAddr());
    }

    SocketHostAddress get_host_address(const SocketInetAddress& addr)
    {
        return get_host_address(addr.GetRawSockAddr());
    }

    SocketInetAddress get_socket_inet_address(int32 fd)
    {
        char temp[256];
        memset(&temp, 0, sizeof(temp));
        sockaddr* addr = (sockaddr*) temp;
        socklen_t socklen = sizeof(temp);
        if (0 == getsockname(fd, addr, &socklen))
        {
            if (addr->sa_family == AF_INET)
            {
                sockaddr_in* in = (sockaddr_in*) addr;
                return SocketInetAddress(*in);
            }
            else if (addr->sa_family == AF_INET6)
            {
                sockaddr_in6* in6 = (sockaddr_in6*) addr;
                return SocketInetAddress(*in6);
            }
            else if (addr->sa_family == AF_UNIX)
            {
                sockaddr_un* un = (sockaddr_un*) addr;
                return SocketInetAddress(*un);
            }
        }
        return SocketInetAddress();
    }

    SocketHostAddress get_host_address(int32 fd)
    {
        SocketInetAddress inetaddr = get_socket_inet_address(fd);
        return get_host_address(inetaddr);
    }

    SocketInetAddress get_remote_inet_address(int32 fd)
    {

        char temp[256];
        memset(&temp, 0, sizeof(temp));
        sockaddr* addr = (sockaddr*) temp;
        socklen_t socklen = sizeof(temp);
        if (0 == getpeername(fd, addr, &socklen))
        {
            if (addr->sa_family == AF_INET)
            {
                sockaddr_in* in = (sockaddr_in*) addr;
                return SocketInetAddress(*in);
            }
            else if (addr->sa_family == AF_INET6)
            {
                sockaddr_in6* in6 = (sockaddr_in6*) addr;
                return SocketInetAddress(*in6);
            }
            else if (addr->sa_family == AF_UNIX)
            {
                sockaddr_un* un = (sockaddr_un*) addr;
                return SocketInetAddress(*un);
            }

        }
        return SocketInetAddress();
    }

    SocketHostAddress get_remote_host_address(int32 fd)
    {
        SocketInetAddress inetaddr = get_remote_inet_address(fd);
        return get_host_address(inetaddr);
    }

    uint64 hton_u64(uint64 v)
    {
        int num = 42;
        //big or little
        if (*(char *) &num == 42)
        {
            uint64_t temp = htonl(v & 0xFFFFFFFF);
            temp <<= 32;
            return temp | htonl(v >> 32);
        }
        else
        {
            return v;
        }

    }

    uint64 ntoh_u64(uint64 v)
    {
        return hton_u64(v);
    }

    int get_ip_by_nic_name(const std::string& ifName, std::string& ip)
    {
        int fd;
        int intrface;
        struct ifconf ifc;
        struct ifreq ifr[kMaxNICCount];

        if (-1 == (fd = socket(AF_INET, SOCK_DGRAM, 0)))
        {
            return -1;
        }

        ifc.ifc_len = sizeof(ifr);
        ifc.ifc_buf = (caddr_t) ifr;

        if (-1 == ioctl(fd, SIOCGIFCONF, (char *) &ifc))
        {
            ::close(fd);
            return -1;
        }

        intrface = ifc.ifc_len / sizeof(struct ifreq);
        while (intrface-- > 0)
        {
            /*Get IP of the net card */
            if (-1 == ioctl(fd, SIOCGIFADDR, (char *) &ifr[intrface]))
                continue;
            if (NULL == ifr[intrface].ifr_name)
                continue;

            if (0 == strcmp(ifName.c_str(), ifr[intrface].ifr_name))
            {
                SocketHostAddress addr = get_host_address(ifr[intrface].ifr_addr);
                ip = addr.GetHost();
                ::close(fd);
                return 0;
            }
        }

        close(fd);
        return -1;
    }

    int get_local_host_ip_list(std::vector<std::string>& iplist)
    {
        struct ifaddrs * ifAddrStruct = NULL;
        struct ifaddrs * ifa = NULL;
        void * tmpAddrPtr = NULL;

        getifaddrs(&ifAddrStruct);

        for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next)
        {
            if (ifa->ifa_addr->sa_family == AF_INET)
            { // check it is IP4
              // is a valid IP4 Address
                tmpAddrPtr = &((struct sockaddr_in *) ifa->ifa_addr)->sin_addr;
                char addressBuffer[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
                iplist.push_back(addressBuffer);
            }
            else if (ifa->ifa_addr->sa_family == AF_INET6)
            { // check it is IP6
              // is a valid IP6 Address
                tmpAddrPtr = &((struct sockaddr_in6 *) ifa->ifa_addr)->sin6_addr;
                char addressBuffer[INET6_ADDRSTRLEN];
                inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
                iplist.push_back(addressBuffer);
            }
        }
        if (ifAddrStruct != NULL)
            freeifaddrs(ifAddrStruct);
        return 0;
    }

    int get_local_host_ipv4(std::string& ip)
    {
        std::vector<std::string> ips;

        if (0 == get_local_host_ip_list(ips))
        {

            for (uint32 i = 0; i < ips.size(); i++)
            {
                if (ips[i].find("127.0.0") != std::string::npos || ips[i].find(":") != std::string::npos)
                {
                    continue;
                }
                ip = ips[i];
                return 0;
            }
        }

        return -1;
    }

    bool is_local_ip(const std::string& ip)
    {
        std::vector<std::string> iplist;
        get_local_host_ip_list(iplist);
        for (uint32 i = 0; i < iplist.size(); i++)
        {
            if (iplist[i] == ip)
            {
                return true;
            }
        }
        return false;
    }
}

