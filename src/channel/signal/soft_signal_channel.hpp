/*
 * soft_signal_channel.hpp
 *
 *  Created on: 2011-7-12
 *      Author: qiyingwang
 */

#ifndef SOFT_SIGNAL_CHANNEL_HPP_
#define SOFT_SIGNAL_CHANNEL_HPP_
#include "channel/channel.hpp"
#include <vector>
#include <string>

namespace ardb
{
	struct EventFD
	{
		private:
			char _kReadSigInfoBuf[sizeof(uint64)];
			char _kWriteSigInfoBuf[sizeof(uint64)];
			uint32 m_readed_siginfo_len;
		public:
			int write_fd;
			int read_fd;

			EventFD() :
					m_readed_siginfo_len(0), write_fd(-1), read_fd(-1)
			{
			}
			inline bool IsOpen()
			{
				return read_fd > 0 || write_fd > 0;
			}
			int OpenNonBlock();
			int Close();
			int WriteEvent(uint64 ev);
			int ReadEvent(uint64& ev);
	};

	struct SoftSignalHandler
	{
			virtual void OnSoftSignal(uint32 soft_signo, uint32 appendinfo) = 0;
			virtual ~SoftSignalHandler()
			{
			}
	};

	class SoftSignalChannel: public Channel
	{
		private:
			typedef std::map<uint32, std::vector<SoftSignalHandler*> > SignalHandlerMap;
			SignalHandlerMap m_hander_map;

			EventFD m_event_fd;
			SoftSignalChannel(ChannelService& factory);
			void FireSignalReceived(uint32 soft_signo, uint32 append_info);
			bool DoOpen();
			bool DoClose();
			void OnRead();
			int GetWriteFD();
			int GetReadFD();
			~SoftSignalChannel();
			friend class ChannelService;
		public:
			void Register(uint32 signo, SoftSignalHandler* handler);
			void Unregister(SoftSignalHandler* handler);
			void AttachEventFD(EventFD* fd);
			int FireSoftSignal(uint32 signo, uint32 append_info);
			void Clear();
	};
}

#endif /* SOFT_SIGNAL_CHANNEL_HPP_ */
