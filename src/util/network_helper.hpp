/*
 * NetworkHelper.hpp
 *
 *  Created on: 2011-1-8
 *      Author: wqy
 */

#ifndef NOVA_NETWORKHELPER_HPP_
#define NOVA_NETWORKHELPER_HPP_
#include "socket_host_address.hpp"
#include "socket_inet_address.hpp"
#include "socket_unix_address.hpp"
#include <list>
#include <vector>

using std::list;

namespace rddb
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

	uint64 ntohll(uint64 v);
	uint64 htonll(uint64 v);


	int get_ip_by_nic_name(const std::string& ifName, std::string& ip);
	int get_local_host_ip_list(std::vector<std::string>& iplist);
}

#endif /* NETWORKHELPER_HPP_ */
