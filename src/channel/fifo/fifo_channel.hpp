/*
 * FIFOChannel.hpp
 *
 *  Created on: 2011-1-28
 *      Author: wqy
 */

#ifndef NOVA_FIFOCHANNEL_HPP_
#define NOVA_FIFOCHANNEL_HPP_
#include "channel/channel.hpp"
#include "pipe.hpp"
#include <unistd.h>
#include <string>
namespace ardb
{
	class PipeChannel: public Channel
	{
		private:
			int m_read_fd;
			int m_write_fd;
			bool DoOpen();
			bool DoBind(Address* local);
			bool DoConnect(Address* remote);
			bool DoClose();
			int GetWriteFD();
			int GetReadFD();
		public:
			PipeChannel(ChannelService& factory, int readFd, int writeFd);
			virtual ~PipeChannel();
	};
}

#endif /* FIFOCHANNEL_HPP_ */
