/*
 * ChannelHelper.cpp
 *
 *  Created on: 2011-1-8
 *      Author: wqy
 */
#include "channel/all_includes.hpp"

namespace ardb
{
	bool fire_channel_bound(Channel* channel)
	{
		ChannelStateEvent event(channel, BOUND, NULL, true);
		return channel->GetPipeline().SendUpstream(event);
	}

	bool fire_channel_closed(Channel* channel)
	{
		ChannelStateEvent event(channel, CLOSED, NULL, true);
		return channel->GetPipeline().SendUpstream(event);
	}

	bool fire_channel_connected(Channel* channel)
	{
		ChannelStateEvent event(channel, CONNECTED, NULL, true);
		return channel->GetPipeline().SendUpstream(event);
	}

	bool fire_channel_open(Channel* channel)
	{
		ChannelStateEvent event(channel, OPEN, NULL, true);
		return channel->GetPipeline().SendUpstream(event);
	}

	bool fire_exception_caught(Channel* channel, const APIException& cause)
	{
		ExceptionEvent event(channel, cause, true);
		return channel->GetPipeline().SendUpstream(event);
	}

	bool fire_write_complete(Channel* channel)
	{
		ChannelStateEvent event(channel, FLUSH, NULL, true);
		return channel->GetPipeline().SendUpstream(event);
	}

	bool GetSocketRemoteAddress(Channel* channel, SocketHostAddress& address)
	{
		const Address* remote_address = channel->GetRemoteAddress();
		if (NULL == remote_address)
		{
			return false;
		}
		Address* conn_addr = const_cast<Address*>(remote_address);
		if (InstanceOf < SocketHostAddress > (conn_addr).OK)
		{
			SocketHostAddress* host_addr = (SocketHostAddress*) conn_addr;
			address = (*host_addr);
			return true;
		}
		return false;
	}

	bool GetSocketLocalAddress(Channel* channel, SocketHostAddress& address)
	{
		const Address* remote_address = channel->GetLocalAddress();
		if (NULL == remote_address)
		{
			return false;
		}
		Address* conn_addr = const_cast<Address*>(remote_address);
		if (InstanceOf < SocketHostAddress > (conn_addr).OK)
		{
			SocketHostAddress* host_addr = (SocketHostAddress*) conn_addr;
			address = (*host_addr);
			return true;
		}
		return false;
	}
}

