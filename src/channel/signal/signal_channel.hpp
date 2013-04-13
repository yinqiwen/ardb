/*
 * signal_channel.hpp
 *
 *  Created on: 2011-4-23
 *      Author: wqy
 */

#ifndef SIGNAL_CHANNEL_HPP_
#define SIGNAL_CHANNEL_HPP_
#include "channel/channel.hpp"
#include <vector>
#include <signal.h>

namespace ardb
{
	struct SignalHandler
	{
			virtual void OnSignal(int signo, siginfo_t& info) = 0;
			virtual ~SignalHandler()
			{
			}
	};

	class SignalChannel: public Channel
	{
		private:
			typedef std::map<uint32, std::vector<SignalHandler*> > SignalHandlerMap;
			SignalHandlerMap m_hander_map;
			static void SignalCB(int signo, siginfo_t* info, void* ctx);
			static SignalChannel* m_singleton_instance;

			int m_self_read_pipe_fd;
			int m_self_write_pipe_fd;
			uint32 m_readed_siginfo_len;
			SignalChannel(ChannelService& factory);
			void FireSignalReceived(int signo, siginfo_t& info);
			bool DoOpen();
			bool DoClose();
			void OnRead();
			int GetWriteFD();
			int GetReadFD();
			~SignalChannel();
			friend class ChannelService;
		public:
			void Register(uint32 signo, SignalHandler* handler);
			void Unregister(SignalHandler* handler);
			void Clear();
	};
}

#endif /* SIGNAL_CHANNEL_HPP_ */
