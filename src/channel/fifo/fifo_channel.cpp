/*
 * ReadFIFOChannel.cpp
 *
 *  Created on: 2011-2-1
 *      Author: wqy
 */
#include "channel/all_includes.hpp"
#include "exception/api_exception.hpp"
#include "util/file_helper.hpp"
#include "exception/api_exception.hpp"

using namespace ardb;

PipeChannel::PipeChannel(ChannelService& factory, int readFd, int writeFd) :
		Channel(NULL, factory), m_read_fd(readFd), m_write_fd(writeFd)
{
	if (-1 != m_read_fd)
	{
		make_fd_nonblocking(m_read_fd);
		aeCreateFileEvent(GetService().GetRawEventLoop(), m_read_fd,
						AE_READABLE, Channel::IOEventCallback, this);

	}
	if (-1 != m_write_fd)
	{
		make_fd_nonblocking(m_write_fd);
	}
}

bool PipeChannel::DoOpen()
{
	return true;
}

bool PipeChannel::DoBind(Address* local)
{
	return false;
}
bool PipeChannel::DoConnect(Address* remote)
{
	return false;
}
bool PipeChannel::DoClose()
{
	close(m_read_fd);
	close(m_write_fd);
	m_read_fd = -1;
	m_write_fd = -1;
	//fireChannelClosed(this);
	return true;
}
int PipeChannel::GetWriteFD()
{
	return m_write_fd;
}

int PipeChannel::GetReadFD()
{
	return m_read_fd;
}

PipeChannel::~PipeChannel()
{
	Close();
}
