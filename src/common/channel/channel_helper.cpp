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

	bool fire_channel_writable(Channel* channel)
	{
		ChannelStateEvent event(channel, WRITABLE, NULL, true);
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

