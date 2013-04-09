/*
 * DatagramChannel.cpp
 *
 *  Created on: 2011-2-1
 *      Author: wqy
 */
#include "channel/all_includes.hpp"
#include "util/datagram_packet.hpp"

using namespace ardb;

//static boost::object_pool<HeapByteArray> kByteArrayPool;

int32 DatagramChannel::Send(const SocketInetAddress& addr, Buffer* buffer)
{
	char* raw = const_cast<char*>(buffer->GetRawReadBuffer());
	uint32 len = buffer->ReadableBytes();
	int ret = ::sendto(GetSocketFD(addr.GetDomain()), raw, len, 0,
			&(addr.GetRawSockAddr()), addr.GetRawSockAddrSize());
	if (ret > 0)
	{
		buffer->AdvanceReadIndex(len);
	}
	return ret;
}

int32 DatagramChannel::Receive(SocketInetAddress& addr, Buffer* buffer)
{
	sockaddr& tempaddr = const_cast<sockaddr&>(addr.GetRawSockAddr());
	socklen_t socklen = sizeof(sockaddr);
	memset(&tempaddr, 0, sizeof(struct sockaddr_in));
	buffer->EnsureWritableBytes(65536);
	char* raw = const_cast<char*>(buffer->GetRawWriteBuffer());
	int ret = ::recvfrom(GetSocketFD(addr.GetDomain()), raw,
			buffer->WriteableBytes(), 0, &tempaddr, &socklen);
	if (ret > 0)
	{
		buffer->AdvanceWriteIndex(ret);
	}
	return ret;
}

void DatagramChannel::OnRead()
{
	DatagramPacket packet(65536);
	int32 len = Receive(packet.GetInetAddress(), &(packet.GetBuffer()));
	if (len > 0)
	{
		fire_message_received<DatagramPacket>(this, &packet, NULL);
	}
}

DatagramChannel::~DatagramChannel()
{
	//m_service.GetEventChannelService().DestroyEventChannel(m_event_channel);
}
