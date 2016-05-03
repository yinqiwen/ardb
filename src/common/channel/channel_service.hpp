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

#ifndef NOVA_CHANNELSERVICE_HPP_
#define NOVA_CHANNELSERVICE_HPP_
#include "util/socket_host_address.hpp"
#include "util/datagram_packet.hpp"
//#include "util/zmq/ypipe.hpp"
#include "util/concurrent_queue.hpp"
#include "thread/thread.hpp"
#include "channel/channel_event.hpp"
#include "channel/timer/timer_channel.hpp"
#include "channel/signal/signal_channel.hpp"
#include "channel/signal/soft_signal_channel.hpp"
#include "channel/socket/clientsocket_channel.hpp"
#include "channel/socket/datagram_channel.hpp"
#include "channel/socket/serversocket_channel.hpp"
#include "channel/fifo/fifo_channel.hpp"
#include "timer/timer.hpp"
#include <list>
#include <utility>

using std::map;
using std::list;
using std::pair;
using std::make_pair;

namespace ardb
{
    enum ChannelSoftSignal
    {
        CHANNEL_REMOVE = 1, WAKEUP = 2, USER_DEFINED = 3, CHANNEL_ASNC_IO = 4,
    };



    class ChannelService;
    typedef void UserEventCallback(ChannelService* serv, uint32 ev, void* data);
    typedef void UserRoutineCallback(ChannelService* serv, uint32 idx, void* data);

    struct ChannelServiceLifeCycle
    {
            virtual void OnStart(ChannelService* serv, uint32 idx) = 0;
            virtual void OnStop(ChannelService* serv, uint32 idx) = 0;
            virtual void OnRoutine(ChannelService* serv, uint32 idx) = 0;
            virtual ~ChannelServiceLifeCycle()
            {
            }
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
            typedef TreeMap<uint32, Channel*>::Type ChannelTable;
            typedef std::vector<ChannelService*> ChannelServicePool;
            typedef std::vector<Thread*> ThreadVector;

            typedef MPSCQueue<ChannelAsyncIOContext> AsyncIOQueue;
            ChannelTable m_channel_table;
            uint32 m_setsize;
            aeEventLoop* m_eventLoop;
            TimerChannel* m_timer;
            SignalChannel* m_signal_channel;
            SoftSignalChannel* m_self_soft_signal_channel;
            RemoveChannelQueue m_remove_queue;
            AsyncIOQueue m_async_io_queue;

            bool m_running;

            uint32 m_thread_pool_size;
            ChannelServicePool m_sub_pool;
            ThreadVector m_sub_pool_ts;

            pthread_t m_tid;

            TaskList m_pending_tasks;

//            UserEventCallback* m_user_cb;
//            void* m_user_cb_data;

            ChannelServiceLifeCycle* m_lifecycle_callback;

            /*
             * parent's index is 0
             * children's index is [1-n]
             */
            uint32 m_pool_index;

            ChannelService* m_parent;

            bool EventSunk(ChannelPipeline* pipeline, ChannelEvent& e)
            {
                ERROR_LOG("Not support this operation!Please register a channel handler to handle this event.");
                return false;
            }
            bool EventSunk(ChannelPipeline* pipeline, ChannelStateEvent& e);
            bool EventSunk(ChannelPipeline* pipeline, MessageEvent<Buffer>& e);
            bool EventSunk(ChannelPipeline* pipeline, MessageEvent<DatagramPacket>& e);

            void OnSoftSignal(uint32 soft_signo, uint32 appendinfo);

            static void OnStopCB(Channel*, void*);
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
            void AsyncIO(const ChannelAsyncIOContext& ctx);
            void Routine();
            void SetParent(ChannelService* parent)
            {
                m_parent = parent;
            }
        public:
            ChannelService(uint32 setsize = 10240);
            void SetThreadPoolSize(uint32 size);
            uint32 GetThreadPoolSize();
            ChannelService& GetNextChannelService();
            ChannelService& GetIdlestChannelService(uint32 min, uint32 max);
            uint32 GetPoolIndex()
            {
                return m_pool_index;
            }

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

            Channel* AttachChannel(Channel* ch, bool transfer_service_only = false);
            bool DetachChannel(Channel* ch, bool remove = false);

            //In most time , you should not invoke this method
            inline aeEventLoop* GetRawEventLoop()
            {
                return m_eventLoop;
            }
            /**
             * Start Event loop
             */
            void Start();
            void Stop();
            void Continue();
            void CloseAllChannels(bool fireCloseEvent = true);
            void CloseAllChannelFD(std::set<Channel*>& exceptions);

//            void RegisterUserEventCallback(UserEventCallback* cb, void* data);
            void RegisterLifecycleCallback(ChannelServiceLifeCycle* callback);
            void FireUserEvent(uint32 ev);
            void AsyncIO(uint32 id, ChannelAsyncIOCallback* cb, void* data);

            ChannelService* GetParent()
            {
                return m_parent;
            }
            ~ChannelService();
    };
}

#endif /* CHANNELSERVICE_HPP_ */
