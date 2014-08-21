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
			typedef TreeMap<uint32, std::vector<SignalHandler*> >::Type SignalHandlerMap;
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
