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
#include "util/exception/api_exception.hpp"
#include "util/file_helper.hpp"

using namespace ardb;

PipeChannel::PipeChannel(ChannelService& factory, int readFd, int writeFd) :
		Channel(NULL, factory), m_read_fd(readFd), m_write_fd(writeFd)
{
	//DoOpen();
}

bool PipeChannel::DoOpen()
{
	if (-1 == m_read_fd && -1 == m_write_fd)
	{
		int pipefd[2];
		int ret = pipe(pipefd);
		if (ret == -1)
		{
			ERROR_LOG("Failed to create pipe for soft signal channel.");
			return false;
		}
		m_read_fd = pipefd[0];
		m_write_fd = pipefd[1];
	}
	if (-1 != m_read_fd)
	{
		make_fd_nonblocking(m_read_fd);
		int ret = aeCreateFileEvent(GetService().GetRawEventLoop(), m_read_fd,
		        AE_READABLE, Channel::IOEventCallback, this);
		if (ret != 0)
		{
			ERROR_LOG("Failed to create eve:%d.", m_read_fd);
		}

	}
	if (-1 != m_write_fd)
	{
		make_fd_nonblocking(m_write_fd);
	}
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

}
