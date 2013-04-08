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
#include "channel/channel_event.hpp"
#include "channel/timer/timer_channel.hpp"
#include "channel/signal/signal_channel.hpp"
#include "channel/signal/soft_signal_channel.hpp"
#include "channel/socket/clientsocket_channel.hpp"
#include "channel/socket/datagram_channel.hpp"
#include "channel/socket/serversocket_channel.hpp"
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

namespace rddb
{
	enum ChannelSoftSignal
	{
		CHANNEL_REMOVE = 1
	};
	/**
	 * 创建/销毁所有类型的channel，管理基本的Event Loop
	 */
	class ChannelService: public Runnable, public SoftSignalHandler
	{
		private:
			typedef std::list<uint32> RemoveChannelQueue;
			typedef std::tr1::unordered_map<uint32, Channel*> ChannelTable;
			ChannelTable m_channel_table;
			aeEventLoop* m_eventLoop;
			TimerChannel* m_timer;
			SignalChannel* m_signal_channel;
			SoftSignalChannel* m_self_soft_signal_channel;
			RemoveChannelQueue m_remove_queue;
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
			//list<Channel*> m_deleting_queue;
			//void Add2DeleteChannelQueue(Channel* ch);
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

			void DeleteClientSocketChannel(ClientSocketChannel* ch);
			void DeleteServerSocketChannel(ServerSocketChannel* ch);
			void DeleteDatagramChannel(DatagramChannel* ch);
			void DeleteChannel(Channel* ch);
			void RemoveChannel(Channel* ch);
			void VerifyRemoveQueue();
		public:
			ChannelService(uint32 setsize = 10240);
			void Routine();
			Channel* GetChannel(uint32 channelID);
			Timer& GetTimer();
			SignalChannel& GetSignalChannel();
			SoftSignalChannel* NewSoftSignalChannel();

			ClientSocketChannel* NewClientSocketChannel();
			ServerSocketChannel* NewServerSocketChannel();
			DatagramChannel* NewDatagramSocketChannel();

			Channel* AttachChannel(Channel* ch);
			bool DetachChannel(Channel* ch);

			//In most time , you should not invoke this method
			inline aeEventLoop* GetRawEventLoop()
			{
				return m_eventLoop;
			}
			/**
			 * 启动Event loop
			 * @self_routine 是否自行例行check
			 */
			void Start(bool self_routine = true);
			/**
			 * 退出Event loop
			 */
			void Stop();
			void CloseAllChannels();
			void CloseAllChannelFD(std::set<Channel*>& exceptions);
			~ChannelService();
	};
}

#endif /* CHANNELSERVICE_HPP_ */
