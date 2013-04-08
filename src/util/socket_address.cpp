/*
 * SocketAddress.cpp
 *
 *  Created on: 2011-1-9
 *      Author: wqy
 */
#include "network_helper.hpp"
#include "socket_address.hpp"

using namespace rddb;

SocketAddress::SocketAddress(const string& host, uint32 port):SocketHostAddress(host,port),m_inet_addr(get_inet_address(host,port))
{

}
