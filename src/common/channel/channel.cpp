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

#include "channel/all_includes.hpp"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/stat.h>
#if defined(linux) || defined(__linux__)
#include <sys/sendfile.h>
#endif
#include "thread/spin_mutex_lock.hpp"
#include "thread/lock_guard.hpp"

using namespace ardb;

static volatile uint32 kChannelIDSeed = 0;
static SpinMutexLock g_channel_id_mutex;

void Channel::IOEventCallback(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask)
{
    Channel* channel = (Channel*) clientData;
    bool fired = false;
    if (mask & AE_READABLE)
    {
        channel->OnRead();
        fired = true;
    }
    if (mask & AE_WRITABLE)
    {
        if (!fired || !channel->IsClosed())
        {
            channel->OnWrite();
        }
    }
}

Channel::Channel(Channel* parent, ChannelService& service) :
        m_user_configed(false), m_has_removed(false), m_parent_id(0), m_service(&service), m_id(0), m_fd(-1), m_flush_timertask_id(-1), m_pipeline_initializor(
        NULL), m_pipeline_initailizor_user_data(NULL), m_pipeline_finallizer(
        NULL), m_pipeline_finallizer_user_data(NULL), m_detached(false), m_close_after_write(false), m_block_read(false), m_file_sending(
        NULL), m_attach(NULL), m_attach_destructor(NULL)
{

    {
        LockGuard<SpinMutexLock> guard(g_channel_id_mutex);
        if (kChannelIDSeed == MAX_CHANNEL_ID)
        {
            kChannelIDSeed = 0;
        }
        kChannelIDSeed++;
        m_id = kChannelIDSeed;
    }
    if (NULL != parent)
    {
        m_parent_id = parent->GetID();
    }
}

int Channel::GetWriteFD()
{
    return m_fd;
}

int Channel::GetReadFD()
{
    return m_fd;
}

bool Channel::BlockRead()
{
    if (GetReadFD() > 0 && !m_block_read)
    {
        aeDeleteFileEvent(GetService().GetRawEventLoop(), GetReadFD(), AE_READABLE);
        m_block_read = true;
    }
    return true;
}
bool Channel::UnblockRead()
{
    if (!m_block_read)
    {
        return true;
    }
    int fd = GetReadFD();
    if (fd != -1 && aeCreateFileEvent(GetService().GetRawEventLoop(), fd, AE_READABLE, Channel::IOEventCallback, this) == AE_ERR)
    {
        ::close(GetReadFD());
        ERROR_LOG("Failed to register event for fd:%d.", GetReadFD());
        return false;
    }
    m_block_read = false;
    if(m_inputBuffer.ReadableBytes() > 0)
    {
        fire_message_received<Buffer>(this, &m_inputBuffer, NULL);
    }
    return true;
}

bool Channel::AttachFD()
{
    int fd = GetReadFD();
    int mask = AE_READABLE;
    if(m_outputBuffer.Readable())
    {
    	mask |= AE_WRITABLE;
    }
    if (fd != -1 && aeCreateFileEvent(GetService().GetRawEventLoop(), fd, mask, Channel::IOEventCallback, this) == AE_ERR)
    {
        ::close(GetReadFD());
        ERROR_LOG("Failed to register event for fd:%d.", GetReadFD());
        return false;
    }
    m_detached = false;
    return true;
}

bool Channel::AttachFD(int fd)
{
    DEBUG_LOG("AttachFD with fd:%d", fd);
    if (m_fd != -1)
    {
        ERROR_LOG("Failed to attach FD since current fd is not -1");
        return false;
    }
    int mask = AE_READABLE;
    if(m_outputBuffer.Readable())
    {
    	mask |= AE_WRITABLE;
    }
    if (aeCreateFileEvent(GetService().GetRawEventLoop(), fd, mask, Channel::IOEventCallback, this) == AE_ERR)
    {
        ::close(fd);
        ERROR_LOG("Failed to register event for fd:%d.", m_fd);
        return false;
    }
    m_fd = fd;
    m_detached = false;
    return true;
}

bool Channel::SetIOEventCallback(ChannelIOEventCallback* cb, int mask, void* data)
{
    if (!m_detached)
    {
        ERROR_LOG("Channel::SetIOEventCallback should be invoked after detached.");
        return false;
    }
    int rd_fd = GetReadFD();
    int wr_fd = GetWriteFD();

    if (rd_fd == wr_fd)
    {
        if (aeCreateFileEvent(GetService().GetRawEventLoop(), rd_fd, mask, cb, data) == AE_ERR)
        {
            ::close(rd_fd);
            ERROR_LOG("Failed to register event for fd:%d.", rd_fd);
            return false;
        }
    }
    else
    {
        if (mask & AE_READABLE)
        {
            if (aeCreateFileEvent(GetService().GetRawEventLoop(), rd_fd,
            AE_READABLE, cb, data) == AE_ERR)
            {
                ::close(rd_fd);
                ERROR_LOG("Failed to register event for fd:%d.", rd_fd);
                return false;
            }
        }
        else if (mask & AE_WRITABLE)
        {
            if (aeCreateFileEvent(GetService().GetRawEventLoop(), wr_fd,
            AE_WRITABLE, cb, data) == AE_ERR)
            {
                ::close(wr_fd);
                ERROR_LOG("Failed to register event for fd:%d.", wr_fd);
                return false;
            }
        }
    }

    m_detached = false;
    return true;
}

void Channel::DetachFD()
{
    if (GetReadFD() > 0 && !m_detached)
    {
        DEBUG_LOG("Detach fd(%d) from event loop.", GetReadFD());
        aeDeleteFileEvent(GetService().GetRawEventLoop(), GetReadFD(),
        AE_WRITABLE | AE_READABLE);
        m_detached = true;
    }
}

int32 Channel::HandleExceptionEvent(int32 event)
{
    return 0;
}

int Channel::HandleIOError(int err)
{
    DEBUG_LOG("HandleIOError %d:%s for fd:%d", err, strerror(err), m_fd);
    APIException cause(err);
    fire_exception_caught(this, cause);
    DoClose(false);
    return -1;
}

int32 Channel::ReadNow(Buffer* buffer)
{
    int err;
    int ret = buffer->ReadFD(GetReadFD(), err);
    if (ret < 0)
    {
        if (IO_ERR_RW_RETRIABLE(err))
        {
            return 0;
        }
        else
        {
            return HandleIOError(err);
        }
    }
    if (ret == 0)
    {
        return HandleExceptionEvent(CHANNEL_EVENT_EOF);
    }
    return ret;
}

void Channel::Run()
{
    //TRACE_LOG("Flush time task trigger.");
    m_flush_timertask_id = -1;
    Flush();
}

bool Channel::IsEnableWriting()
{
    int fd = GetWriteFD();
    return fd > 0 && (aeGetFileEvents(GetService().GetRawEventLoop(), fd) & AE_WRITABLE);
}

void Channel::EnableWriting()
{
//    if(GetOutputBuffer().ReadableBytes() > 512)
//    {
//        Flush();
//    }
    if (IsEnableWriting())
    {
        return;
    }
    int write_fd = GetWriteFD();
    if (write_fd > 0)
    {
        aeCreateFileEvent(GetService().GetRawEventLoop(), GetWriteFD(),
        AE_WRITABLE, Channel::IOEventCallback, this);
    }
    else
    {
        WARN_LOG("Invalid fd:%d for enable writing for channel[%u].", write_fd, m_id);
    }
}
void Channel::DisableWriting()
{
    if (!IsEnableWriting())
    {
        return;
    }
    int write_fd = GetWriteFD();
    if (write_fd > 0)
    {
        aeDeleteFileEvent(GetService().GetRawEventLoop(), write_fd,
        AE_WRITABLE);
    }
    else
    {
        WARN_LOG("Invalid fd:%d for disable writing for channel[%u].", write_fd, m_id);
    }
}

void Channel::CancelFlushTimerTask()
{
    if (m_flush_timertask_id > 0)
    {
        //TRACE_LOG("Canecl flush time task.");
        GetService().GetTimer().Cancel(m_flush_timertask_id);
        m_flush_timertask_id = -1;
    }
}

void Channel::CreateFlushTimerTask()
{
    if (-1 == m_flush_timertask_id)
    {
        DisableWriting();
        m_flush_timertask_id = GetService().GetTimer().Schedule(this, m_options.user_write_buffer_flush_timeout_mills, -1, MILLIS);
    }
}

int32 Channel::WriteNow(Buffer* buffer)
{
    if (NULL != m_file_sending)
    {
        WARN_LOG("Can NOT write fd since channel is sending file.");
        return 0;
    }
    uint32 buf_len = NULL != buffer ? buffer->ReadableBytes() : 0;

    if (m_outputBuffer.Readable())
    {
        if (m_options.max_write_buffer_size > 0) //write buffer size limit enable
        {
            uint32 write_buffer_size = m_outputBuffer.ReadableBytes();
            if (write_buffer_size > (uint32) m_options.max_write_buffer_size || (write_buffer_size + buf_len) > (uint32) m_options.max_write_buffer_size)
            {
                //overflow
                WARN_LOG("Channel:%u write buffer exceed limit:%d", m_id, m_options.max_write_buffer_size);
                return 0;
            }
        }
        m_outputBuffer.Write(buffer, buf_len);
        if (m_options.user_write_buffer_water_mark > 0 && m_outputBuffer.ReadableBytes() < m_options.user_write_buffer_water_mark)
        {
            CreateFlushTimerTask();
        }
        else
        {
            //EnableWriting();
            if (m_options.async_write)
            {
                EnableWriting();
            }
            else
            {
                Flush();
            }
        }
        return buf_len;
    }
    else
    {
        bool enable_writing = IsEnableWriting();
        if (buf_len < m_options.user_write_buffer_water_mark || enable_writing || m_options.async_write)
        {
            m_outputBuffer.Write(buffer, buf_len);
            if (!enable_writing)
            {
                if (m_options.async_write)
                {
                    EnableWriting();
                }
                else
                {
                    CreateFlushTimerTask();
                }
            }
            return buf_len;
        }
        int err;
        int ret = buffer->WriteFD(GetWriteFD(), err);
        if (ret < 0)
        {
            if (IO_ERR_RW_RETRIABLE(err))
            {
                if (m_options.max_write_buffer_size == 0)
                {
                    //no write buffer allowed
                    return 0;
                }
                m_outputBuffer.Write(buffer, buf_len);
                EnableWriting();
                return buf_len;
            }
            else
            {
                return HandleIOError(err);
            }
        }
        else if (ret == 0)
        {
            return HandleExceptionEvent(CHANNEL_EVENT_EOF);
        }
        else
        {
            if ((size_t) ret < buf_len)
            {
                m_outputBuffer.Write(buffer, buf_len - ret);
                EnableWriting();
            }
            return buf_len;
        }
    }
}

bool Channel::DoConfigure(const ChannelOptions& options)
{
    if (options.user_write_buffer_water_mark > 0)
    {
        if (options.user_write_buffer_flush_timeout_mills > 0)
        {
            m_outputBuffer.EnsureWritableBytes(options.user_write_buffer_water_mark * 2);
            //NEW(m_user_write_buffer, ByteBuffer(options.user_write_buffer_water_mark * 2));
        }
        else
        {
            return false;
        }
    }
    return true;
}

bool Channel::DoClose()
{
    if (-1 != m_fd)
    {
        int ret = ::close(m_fd);
        if (ret < 0)
        {
            int err = errno;
            ERROR_LOG("Failed to close channel for reason:%s", strerror(err));
        }
        CancelFlushTimerTask();
        m_fd = -1;
    }
    return true;
}

bool Channel::DoFlush()
{
    //TRACE_LOG("Flush %u bytes for channel.", m_outputBuffer.ReadableBytes());
    if (m_outputBuffer.Readable())
    {
        uint32 send_buf_len = m_outputBuffer.ReadableBytes();
        int err;
        int ret = m_outputBuffer.WriteFD(GetWriteFD(), err);
        if (ret < 0)
        {
            if (IO_ERR_RW_RETRIABLE(err))
            {
                //nothing
                //EnableWriting();
                return true;
            }
            else
            {
                return HandleIOError(err);
            }
        }
        else if (ret == 0)
        {
            return HandleExceptionEvent(CHANNEL_EVENT_EOF);
        }
        m_outputBuffer.DiscardReadedBytes();
        m_outputBuffer.Compact(m_options.user_write_buffer_water_mark > 0 ? m_options.user_write_buffer_water_mark * 2 : 8192);
        if ((uint32) ret < send_buf_len)
        {
            //EnableWriting();
        }
        else
        {
            //fireWriteComplete(this);
        }
        return true;
    }
    else
    {
        m_outputBuffer.Compact(m_options.user_write_buffer_water_mark > 0 ? m_options.user_write_buffer_water_mark : 8192);
        return true;
    }

}

bool Channel::DoOpen()
{
    ERROR_LOG("DoOpen() is not implemented.");
    return false;
}
bool Channel::DoBind(Address* local)
{
    ERROR_LOG("DoBind() is not implemented.");
    return false;
}

bool Channel::DoConnect(Address* remote)
{
    ERROR_LOG("DoConnect() is not implemented.");
    return false;
}

void Channel::OnRead()
{
    int32 len = 0;
    if (m_block_read)
    {
        //just test error
        char buffer[32];
        len = ::recv(GetReadFD(), buffer, sizeof(buffer), MSG_PEEK | MSG_DONTWAIT);
        if (len < 0)
        {
            int err = errno;
            if (IO_ERR_RW_RETRIABLE(err))
            {
                return;
            }
            else
            {
                HandleIOError(err);
                return;
            }
        }
        if (len == 0)
        {
            HandleExceptionEvent(CHANNEL_EVENT_EOF);
            return;
        }
    }
    else
    {
        m_inputBuffer.DiscardReadedBytes();
        len = ReadNow(&m_inputBuffer);
    }
    if (len > 0)
    {
        //TRACE_LOG(
        //        "DataReceived with %d bytes in channel %u.", m_inputBuffer.ReadableBytes(), GetID());
        fire_message_received<Buffer>(this, &m_inputBuffer, NULL);
    }
    else
    {
        //TRACE_LOG(
        //        "[ERROR]Failed to read channel for ret:%d for fd:%d, channel id %u & type:%u", len, GetReadFD(), GetID(), GetID() & 0xf);
    }
}

void Channel::OnWrite()
{
    if (!DoFlush())
    {
        //fire_channel_closed(this);
        return;
    }
    else
    {
        if (m_close_after_write)
        {
            Close();
            return;
        }
    }
    if (m_outputBuffer.Readable())
    {
        return;
    }
    if (NULL != m_file_sending)
    {
        EnableWriting();
        off_t len = m_file_sending->file_rest_len;
#if defined(__APPLE__)
        int ret = ::sendfile(m_file_sending->fd, GetWriteFD(), m_file_sending->file_offset, &len, NULL, 0);
#elif defined(__FreeBSD__) || defined(__DragonFly__)
        int ret = sendfile(GetWriteFD(), m_file_sending->fd,
                m_file_sending->file_offset, len, NULL, &len, SF_MNOWAIT);
#else
        off_t current = m_file_sending->file_offset;
        int ret = sendfile(GetWriteFD(), m_file_sending->fd, &m_file_sending->file_offset, len);
#endif
        if (ret < 0)
        {
            int err = errno;
            if (err != EAGAIN && ret != EINTR)
            {
                DEBUG_LOG("Connection closed:%s  %d %d", strerror(err), m_file_sending->file_offset, len);
                Close();
                return;
            }
        }
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__DragonFly__)
        m_file_sending->file_rest_len -= len;
        m_file_sending->file_offset += len;
#else
        m_file_sending->file_rest_len -= (m_file_sending->file_offset - current);
#endif

        if (m_file_sending->file_rest_len == 0)
        {
            close(m_file_sending->fd);
            m_file_sending->fd = -1;
            if (NULL != m_file_sending->on_complete)
            {
                IOCallback* cb = m_file_sending->on_complete;
                void* cbdata = m_file_sending->data;
                DELETE(m_file_sending);
                cb(cbdata);
            }

        }
        else
        {
            return;
        }
    }
    fire_channel_writable(this);
    if (!m_outputBuffer.Readable() && m_options.auto_disable_writing)
    {
        DisableWriting();
    }
}

int Channel::SendFile(const SendFileSetting& setting)
{
    m_file_sending = new SendFileSetting(setting);
    EnableWriting();
    return 0;
}

bool Channel::Configure(const ChannelOptions& options)
{
    bool ret = DoConfigure(options);
    if (ret)
    {
        m_options = options;
        m_user_configed = true;
    }
    return ret;
}

bool Channel::Open()
{
    return DoOpen();
}

bool Channel::Bind(Address* local)
{
    return DoBind(local);
}

bool Channel::Connect(Address* remote)
{
    return DoConnect(remote);
}

bool Channel::Flush()
{
    if (IsEnableWriting())
    {
        return false;
    }
    bool ret = DoFlush();
    if (!ret)
    {
        return false;
    }
//	if (!m_outputBuffer.Readable())
//	{
//		DisableWriting();
//	}
    return true;
}

bool Channel::DoClose(bool inDestructor)
{
    bool hasfd = false;
    //DEBUG_LOG("Before close channel, read fd:%d, write fd:%d for channel[%u]", GetReadFD(),  GetWriteFD(), GetID());
    int read_fd = GetReadFD();
    int write_fd = GetWriteFD();
    //int mask = AE_READABLE | AE_WRITABLE;
    if (read_fd > 0)
    {
        aeDeleteFileEvent(GetService().GetRawEventLoop(), read_fd,
        AE_READABLE | AE_WRITABLE);
        hasfd = true;
    }
    if (read_fd != write_fd && write_fd > 0)
    {
        aeDeleteFileEvent(GetService().GetRawEventLoop(), write_fd,
        AE_WRITABLE);
        hasfd = true;
    }
    CancelFlushTimerTask();

    if (NULL != m_file_sending)
    {
        close(m_file_sending->fd);
        m_file_sending->fd = -1;
        if (NULL != m_file_sending->on_failure)
        {
            m_file_sending->on_failure(m_file_sending->data);
        }
    }

    bool ret = false;
    if (hasfd && DoClose() && !inDestructor)
    {
        ret = fire_channel_closed(this);
        if (0 != m_parent_id)
        {
            Channel* parent = GetService().GetChannel(m_parent_id);
            if (NULL != parent)
            {
                parent->OnChildClose(this);
            }
        }
    }

    DELETE(m_file_sending);
    if (!m_has_removed && !inDestructor)
    {
        m_has_removed = true;
        GetService().RemoveChannel(this);
    }
    return ret;
}

bool Channel::Close()
{
    if (m_outputBuffer.Readable() && GetWriteFD() > 0)
    {
        EnableWriting();
        m_close_after_write = true;
        return true;
    }
    return DoClose(false);
}

//bool Channel::AsyncWrite(ChannelAsyncWriteCallback* cb, void* data)
//{
//    ChannelAsyncWriteContext ctx;
//    ctx.channel_id = GetID();
//    ctx.cb = cb;
//    ctx.data = data;
//    m_service->AsyncWrite(ctx);
//    return true;
//}

Channel::~Channel()
{
    DoClose(true);
    m_has_removed = true;
    if (NULL != m_pipeline_finallizer)
    {
        m_pipeline_finallizer(&m_pipeline, m_pipeline_finallizer_user_data);
    }
    if (NULL != m_attach && NULL != m_attach_destructor)
    {
        m_attach_destructor(m_attach);
    }
}
