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

#ifndef SOFT_SIGNAL_CHANNEL_HPP_
#define SOFT_SIGNAL_CHANNEL_HPP_
#include "channel/fifo/fifo_channel.hpp"
#include "channel/codec/int_frame_decoder.hpp"
#include "thread/spin_mutex_lock.hpp"
#include <vector>
#include <string>

namespace ardb
{

	struct SoftSignalHandler
	{
			virtual void OnSoftSignal(uint32 soft_signo, uint32 appendinfo) = 0;
			virtual ~SoftSignalHandler()
			{
			}
	};

	class SoftSignalChannel: public PipeChannel, public ChannelUpstreamHandler<uint64>
	{
		private:
			typedef TreeMap<uint32, std::vector<SoftSignalHandler*> >::Type SignalHandlerMap;
			SignalHandlerMap m_hander_map;
			ardb::codec::UInt64FrameDecoder m_decoder;
			bool m_thread_safe;
			SpinMutexLock m_lock;

			SoftSignalChannel(ChannelService& factory);
			bool DoOpen();
			void MessageReceived(ChannelHandlerContext& ctx,
								MessageEvent<uint64>& e);
			void FireSignalReceived(uint32 soft_signo, uint32 append_info);
			~SoftSignalChannel();
			friend class ChannelService;
		public:
			void Register(uint32 signo, SoftSignalHandler* handler);
			void Unregister(SoftSignalHandler* handler);
			int FireSoftSignal(uint32 signo, uint32 append_info);
			void Clear();
	};
}

#endif /* SOFT_SIGNAL_CHANNEL_HPP_ */
