/*
 * Channel.hpp
 *
 *  Created on: 2011-1-7
 *      Author: qiyingwang
 */

#ifndef NOVA_CHANNEL2_HPP_
#define NOVA_CHANNEL2_HPP_
#include "common.hpp"
#include "channel/redis/ae.h"
#include "util/buffer_helper.hpp"
#include "channel/channel_pipeline.hpp"
#include "util/helpers.hpp"
#include <map>

/* delayed ack (quick_ack) */
#ifndef HAVE_TCP_QUICKACK
#ifdef __linux__
#define HAVE_TCP_QUICKACK
#endif /* __OS_ */
#endif /* NO_TCP_QUICKACK */

/* defer accept */
#ifndef  HAVE_TCP_DEFER_ACCEPT
#ifdef __linux__
#define HAVE_TCP_DEFER_ACCEPT
#endif /* __OS_ */
#endif /* NO_TCP_DEFER_ACCEPT */

#define TCP_CLIENT_SOCKET_CHANNEL_ID_BIT_MASK 1
#define TCP_SERVER_SOCKET_CHANNEL_ID_BIT_MASK 2
#define UDP_SOCKET_CHANNEL_ID_BIT_MASK 3
#define FIFO_CHANNEL_ID_BIT_MASK 4
#define SHM_FIFO_CHANNEL_ID_BIT_MASK 5
#define TIMER_CHANNEL_ID_BIT_MASK 6
#define SIGNAL_CHANNEL_ID_BIT_MASK 7
#define SOFT_SIGNAL_CHANNEL_ID_BIT_MASK 8
#define INOTIFY_CHANNEL_ID_BIT_MASK 9
#define MAX_CHANNEL_ID 0xFFFFFFF

#define IS_TCP_CHANNEL(id) ((id&0xF) == TCP_CLIENT_SOCKET_CHANNEL_ID_BIT_MASK || (id&0xF) == TCP_SERVER_SOCKET_CHANNEL_ID_BIT_MASK)
#define IS_UDP_CHANNEL(id) ((id&0xF) == UDP_SOCKET_CHANNEL_ID_BIT_MASK)
#define IS_FIFO_CHANNEL(id) ((id&0xF) == FIFO_CHANNEL_ID_BIT_MASK)
#define IS_SHM_FIFO_CHANNEL(id) ((id&0xF) == SHM_FIFO_CHANNEL_ID_BIT_MASK)

/* True iff e is an error that means a read/write operation can be retried. */
#define IO_ERR_RW_RETRIABLE(e)				\
	((e) == EINTR || (e) == EAGAIN || (e) == EWOULDBLOCK)
/* True iff e is an error that means an connect can be retried. */
#define SOCKET_ERR_CONNECT_RETRIABLE(e)			\
	((e) == EINTR || (e) == EINPROGRESS)
/* True iff e is an error that means a accept can be retried. */
#define SOCKET_ERR_ACCEPT_RETRIABLE(e)			\
	((e) == EINTR || (e) == EAGAIN || (e) == ECONNABORTED)

/* Just for error reporting - use other constants otherwise */
#define CHANNEL_EVENT_READING	0x01	/**< error encountered while reading */
#define CHANNEL_EVENT_WRITING	0x02	/**< error encountered while writing */
#define CHANNEL_EVENT_EOF		0x10	/**< eof file reached */
#define CHANNEL_EVENT_ERROR		0x20	/**< unrecoverable error encountered */
#define CHANNEL_EVENT_TIMEOUT	0x40	/**< user specified timeout reached */
#define CHANNEL_EVENT_CONNECTED	0x80	/**< connect operation finished. */


namespace ardb
{
	struct ChannelOptions
	{
			uint32 receive_buffer_size;
			uint32 send_buffer_size;
			bool tcp_nodelay;
			uint32 keep_alive;
			bool reuse_address;
			uint32 user_write_buffer_water_mark;
			uint32 user_write_buffer_flush_timeout_mills;
			uint32 max_write_buffer_size;

			ChannelOptions() :
					receive_buffer_size(0), send_buffer_size(0), tcp_nodelay(
							true), keep_alive(0), reuse_address(true), user_write_buffer_water_mark(
							0), user_write_buffer_flush_timeout_mills(0), max_write_buffer_size(
							0)
			{
			}
	};
	template<typename T>
	bool write_channel(Channel* channel, T* message,
			typename Type<T>::Destructor* destructor);

	class Channel;
	typedef int ChannelOperationBarrierHook(Channel*, void*);
	typedef void ChannelIOEventCallback(struct aeEventLoop *eventLoop, int fd,
			void *clientData, int mask);

	class ChannelService;
	class Channel: public Runnable
	{
		private:
			bool DoClose(bool inDestructor);
		protected:
			static void IOEventCallback(struct aeEventLoop *eventLoop, int fd,
					void *clientData, int mask);
			ChannelOptions m_options;
			bool m_user_configed;
			bool m_has_removed;
			uint32 m_parent_id;
			ChannelPipeline m_pipeline;
			ChannelService* m_service;
			/**
			 *  Channel ID
			 *  ==============
			 *
			 *  Offset:  0                          27    31
			 *           +--------------------------+------+
			 *  Fields:  | Incremental  ID          | Type |
			 *           +--------------------------+------+
			 *
			 */
			uint32 m_id;
			int m_fd;
			Buffer m_inputBuffer;
			Buffer m_outputBuffer;
			int32 m_flush_timertask_id;
			ChannelPipelineInitializer* m_pipeline_initializor;
			void* m_pipeline_initailizor_user_data;
			ChannelPipelineFinalizer* m_pipeline_finallizer;
			void* m_pipeline_finallizer_user_data;
			bool m_detached;
			bool m_writable;
			bool m_close_after_write;

			Channel(Channel* parent, ChannelService& factory);

			void Run();

			virtual void OnChildClose(Channel* ch)
			{
			}

			virtual void OnRead();
			virtual void OnWrite();

			virtual bool DoConfigure(const ChannelOptions& options);
			virtual bool DoOpen();
			virtual bool DoBind(Address* local);
			virtual bool DoConnect(Address* remote);
			virtual bool DoClose();
			virtual bool DoFlush();
			virtual int32 WriteNow(Buffer* buffer);
			virtual int32 ReadNow(Buffer* buffer);
			virtual int32 HandleExceptionEvent(int32 event);
			int HandleIOError(int err);

			void EnableWriting();
			void DisableWriting();
			void CancelFlushTimerTask();
			void CreateFlushTimerTask();

			friend class ChannelService;
		public:
			virtual int GetWriteFD();
			virtual int GetReadFD();
			inline uint32 GetID()
			{
				return m_id;
			}
			inline uint32 GetParentID()
			{
				return m_parent_id;
			}
			inline void SetParent(Channel* parent)
			{
				m_parent_id = parent->GetID();
			}
			inline ChannelService& GetService()
			{
				return *m_service;
			}

			inline uint32 WritableBytes()
			{
				return m_outputBuffer.ReadableBytes();
			}

			inline uint32 ReadableBytes()
			{
				return m_inputBuffer.ReadableBytes();
			}

			inline void SkipReadBuffer(int32 size)
			{
				m_inputBuffer.SetReadIndex(m_inputBuffer.GetReadIndex() + size);
			}

			inline void HandleReadEvent()
			{
				OnRead();
			}

			inline bool IsDetached()
			{
				return m_detached;
			}
			bool SetIOEventCallback(ChannelIOEventCallback* cb, int mask,
					void* data);
			bool Configure(const ChannelOptions& options);
			bool Open();
			bool Bind(Address* local);
			bool Connect(Address* remote);

			inline bool IsReadReady()
			{
				return GetReadFD() > 0;
			}
			inline bool IsWriteReady()
			{
				return GetWriteFD() > 0;
			}
			inline bool IsClosed()
			{
				return GetReadFD() < 0;
			}
			virtual bool AttachFD(int fd);
			virtual bool AttachFD();
			virtual void DetachFD();

			inline void SetChannelPipelineInitializor(
					ChannelPipelineInitializer* initializor, void* data = NULL)
			{
				ASSERT(NULL == m_pipeline_initializor && NULL != initializor);
				m_pipeline_initializor = initializor;
				m_pipeline_initailizor_user_data = data;
				m_pipeline_initializor(&m_pipeline, data);
			}

			inline void SetChannelPipelineFinalizer(
					ChannelPipelineFinalizer* finallizer, void* data = NULL)
			{
				ASSERT(NULL != finallizer);
				m_pipeline_finallizer = finallizer;
				m_pipeline_finallizer_user_data = data;
			}

			inline void ClearPipeline()
			{
				if(NULL != m_pipeline_finallizer)
				{
					m_pipeline_finallizer(&m_pipeline, m_pipeline_finallizer_user_data);
					m_pipeline_initializor = NULL;
					m_pipeline_finallizer = NULL;
					m_pipeline.Clear();
				}
			}

//                virtual void SetChannelAcceptOperationBarrierHook(
//                        ChannelOperationBarrierHook* hook, void* data)
//                {
//                }

			inline ChannelPipeline& GetPipeline()
			{
				return m_pipeline;
			}

			template<typename T>
			bool Write(T& msg)
			{
				return write_channel<T>(this, &msg, NULL);
			}

			bool Flush();
			virtual const Address* GetLocalAddress()
			{
				return NULL;
			}
			virtual const Address* GetRemoteAddress()
			{
				return NULL;
			}
			bool Close();
			virtual ~Channel();
	};
}

#endif /* CHANNEL_HPP_ */
