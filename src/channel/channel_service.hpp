/*
 * ChannelService.hpp
 *
 *  Created on: 2011-1-9
 *      Author: wqy
 */

#ifndef NOVA_CHANNELSERVICE_HPP_
#define NOVA_CHANNELSERVICE_HPP_
#include "util/socket_host_address.hpp"
#include "util/datagram_packet.hpp"
#include "util/zmq/ypipe.hpp"
#include "util/concurrent_queue.hpp"
#include "channel/channel_event.hpp"
#include "channel/timer/timer_channel.hpp"
#include "channel/signal/signal_channel.hpp"
#include "channel/signal/soft_signal_channel.hpp"
#include "channel/socket/clientsocket_channel.hpp"
#include "channel/socket/datagram_channel.hpp"
#include "channel/socket/serversocket_channel.hpp"
#include "channel/fifo/fifo_channel.hpp"
#include "timer/timer.hpp"
#include <map>
#include <list>
#include <set>
#include <utility>
#include <tr1/unordered_map>

using std::map;
using std::list;
using std::pair;
using std::make_pair;

namespace ardb
{
	enum ChannelSoftSignal
	{
		CHANNEL_REMOVE = 1, WAKEUP = 2
	};
	/**
	 * event loop service
	 */
	class ChannelService: public Runnable, public SoftSignalHandler
	{
		private:
			typedef std::list<uint32> RemoveChannelQueue;
			//typedef zmq::ypipe_t<Runnable*, 10> TaskList;
			typedef SPSCQueue<Runnable*> TaskList;
			typedef std::tr1::unordered_map<uint32, Channel*> ChannelTable;
			typedef std::vector<ChannelService*> ChannelServicePool;
			ChannelTable m_channel_table;
			uint32 m_setsize;
			aeEventLoop* m_eventLoop;
			TimerChannel* m_timer;
			SignalChannel* m_signal_channel;
			SoftSignalChannel* m_self_soft_signal_channel;
			RemoveChannelQueue m_remove_queue;

			uint32 m_thread_pool_size;
			ChannelServicePool m_sub_pool;
			pthread_t m_tid;

			TaskList m_pending_tasks;
			bool m_running;
			bool EventSunk(ChannelPipeline* pipeline, ChannelEvent& e)
			{
				ERROR_LOG(
						"Not support this operation!Please register a channel handler to handle this event.");
				return false;
			}
			bool EventSunk(ChannelPipeline* pipeline, ChannelStateEvent& e);
			bool EventSunk(ChannelPipeline* pipeline, MessageEvent<Buffer>& e);
			bool EventSunk(ChannelPipeline* pipeline,
					MessageEvent<DatagramPacket>& e);

			void OnSoftSignal(uint32 soft_signo, uint32 appendinfo);

			void Run();
			friend class Channel;
			friend class ChannelPipeline;
			friend class ChannelHandlerContext;
			friend class SocketChannel;
			friend class ClientSocketChannel;
			friend class ServerSocketChannel;
			friend class DatagramChannel;
			friend class TimerChannel;
			friend class SignalChannel;

			TimerChannel* NewTimerChannel();
			SignalChannel* NewSignalChannel();

			Channel* CloneChannel(Channel* ch);
			void DeleteChannel(Channel* ch);
			void RemoveChannel(Channel* ch);
			void VerifyRemoveQueue();
			void StartSubPool();
			void AttachAcceptedChannel(SocketChannel *ch);
		public:
			ChannelService(uint32 setsize = 10240);
			void SetThreadPoolSize(uint32 size);
			uint32 GetThreadPoolSize();
			ChannelService& GetNextChannelService();

			void Routine();
			void Wakeup();
			bool IsInLoopThread() const;
			Channel* GetChannel(uint32 channelID);
			Timer& GetTimer();
			SignalChannel& GetSignalChannel();
			SoftSignalChannel* NewSoftSignalChannel();

			ClientSocketChannel* NewClientSocketChannel();
			ServerSocketChannel* NewServerSocketChannel();
			PipeChannel* NewPipeChannel(int readFd, int writeFD);
			DatagramChannel* NewDatagramSocketChannel();

			Channel* AttachChannel(Channel* ch, bool transfer_service_only =
					false);
			bool DetachChannel(Channel* ch, bool remove = false);

			//In most time , you should not invoke this method
			inline aeEventLoop* GetRawEventLoop()
			{
				return m_eventLoop;
			}
			/**
			 * Start Event loop
			 */
			void Start(bool self_routine = true);
			void Stop();
			void CloseAllChannels(bool fireCloseEvent = true);
			void CloseAllChannelFD(std::set<Channel*>& exceptions);
			~ChannelService();
	};
}

#endif /* CHANNELSERVICE_HPP_ */
