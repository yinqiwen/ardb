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

#ifndef NOVA_CHANNEL2_HPP_
#define NOVA_CHANNEL2_HPP_
#include "common.hpp"
#include "channel/redis/ae.h"
#include "buffer/buffer_helper.hpp"
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
            int32 max_write_buffer_size;  //-1: means unlimit 0: disable
            bool auto_disable_writing;
            bool async_write;

            ChannelOptions() :
                    receive_buffer_size(0), send_buffer_size(0), tcp_nodelay(true), keep_alive(0), reuse_address(true), user_write_buffer_water_mark(
                            0), user_write_buffer_flush_timeout_mills(0), max_write_buffer_size(-1), auto_disable_writing(
                            true),async_write(false)
            {
            }
    };
    template<typename T>
    bool write_channel(Channel* channel, T* message, typename Type<T>::Destructor* destructor);

    class Channel;
    typedef int ChannelOperationBarrierHook(Channel*, void*);
    typedef void ChannelIOEventCallback(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);

    typedef void ChannelAsyncIOCallback(Channel*, void*);
    struct ChannelAsyncIOContext
    {
            uint32 channel_id;
            ChannelAsyncIOCallback* cb;
            void* data;
            ChannelAsyncIOContext() :
                    channel_id(0), cb(NULL), data(NULL)
            {
            }
    };

    typedef void IOCallback(void* data);
    struct SendFileSetting
    {
            int fd;
            off_t file_offset;
            off_t file_rest_len;
            void* data;
            IOCallback* on_complete;
            IOCallback* on_failure;
            SendFileSetting() :
                    fd(-1), file_offset(0), file_rest_len(0), data(NULL), on_complete(
                    NULL), on_failure(NULL)
            {
            }
    };

    typedef void AttachDestructor(void* attach);

    class ChannelService;
    class Channel: public Runnable
    {
        private:
            bool DoClose(bool inDestructor);
        protected:
            static void IOEventCallback(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
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
            bool m_close_after_write;
            bool m_block_read;

            SendFileSetting* m_file_sending;
            void* m_attach;
            AttachDestructor* m_attach_destructor;

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

            inline Buffer& GetOutputBuffer()
            {
                return m_outputBuffer;
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
            bool SetIOEventCallback(ChannelIOEventCallback* cb, int mask, void* data);
            bool Configure(const ChannelOptions& options);
            const ChannelOptions& GetOptions() const
            {
                return m_options;
            }
            ChannelOptions& GetWritableOptions()
            {
                return m_options;
            }

            void Attach(void* attach, AttachDestructor* destructor = NULL)
            {
                m_attach = attach;
                m_attach_destructor = destructor;
            }
            void* Attachment()
            {
                return m_attach;
            }

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

            bool IsEnableWriting();
            void EnableWriting();
            void DisableWriting();

            bool BlockRead();
            bool UnblockRead();
            bool IsReadBlocked()
            {
                return m_block_read;
            }

            inline void SetChannelPipelineInitializor(ChannelPipelineInitializer* initializor, void* data = NULL)
            {
                ASSERT(NULL == m_pipeline_initializor && NULL != initializor);
                m_pipeline_initializor = initializor;
                m_pipeline_initailizor_user_data = data;
                m_pipeline_initializor(&m_pipeline, data);
            }

            inline void SetChannelPipelineFinalizer(ChannelPipelineFinalizer* finallizer, void* data = NULL)
            {
                ASSERT(NULL != finallizer);
                m_pipeline_finallizer = finallizer;
                m_pipeline_finallizer_user_data = data;
            }

            inline void ClearPipeline()
            {
                if (NULL != m_pipeline_finallizer)
                {
                    m_pipeline_finallizer(&m_pipeline, m_pipeline_finallizer_user_data);
                    m_pipeline_initializor = NULL;
                    m_pipeline_finallizer = NULL;
                    m_pipeline_finallizer_user_data = NULL;
                    m_pipeline_initailizor_user_data = NULL;
                    m_pipeline.Clear();
                }

            }

            inline ChannelPipeline& GetPipeline()
            {
                return m_pipeline;
            }

            template<typename T>
            bool Write(T& msg)
            {
                return write_channel<T>(this, &msg, NULL);
            }

            int SendFile(const SendFileSetting& setting);

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
