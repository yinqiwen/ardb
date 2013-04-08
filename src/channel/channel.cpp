/*
 * Channel.cpp
 *
 *  Created on: 2011-1-8
 *      Author: wqy
 */
#include "channel/all_includes.hpp"

using namespace rddb;

static uint32 kChannelIDSeed = 1;

void Channel::IOEventCallback(struct aeEventLoop *eventLoop, int fd,
        void *clientData, int mask)
{
    //DEBUG_LOG("############Mask is %d", mask);
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
    m_user_configed(false), m_has_removed(false), m_parent_id(0), m_service(
            service), m_id(0), m_fd(-1), m_flush_timertask_id(-1),
            m_pipeline_initializor(NULL),
            m_pipeline_initailizor_user_data(NULL),
            m_pipeline_finallizer(NULL), m_pipeline_finallizer_user_data(NULL),
            m_detached(false), m_writable(true),m_close_after_write(false)
{

    if (kChannelIDSeed == MAX_CHANNEL_ID)
    {
        kChannelIDSeed = 1;
    }
    m_id = kChannelIDSeed;
    kChannelIDSeed++;
    if (NULL != parent)
    {
        m_parent_id = parent->GetID();
    }
    //m_global_channel_table[m_id] = this;
}

int Channel::GetWriteFD()
{
    return m_fd;
}

int Channel::GetReadFD()
{
    return m_fd;
}

bool Channel::AttachFD()
{
    if (aeCreateFileEvent(m_service.GetRawEventLoop(), GetReadFD(),
            AE_READABLE, Channel::IOEventCallback, this) == AE_ERR)
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
    if (aeCreateFileEvent(m_service.GetRawEventLoop(), fd, AE_READABLE,
            Channel::IOEventCallback, this) == AE_ERR)
    {
        ::close(fd);
        ERROR_LOG("Failed to register event for fd:%d.", m_fd);
        return false;
    }
    m_fd = fd;
    m_detached = false;
    return true;
}

bool Channel::SetIOEventCallback(ChannelIOEventCallback* cb, int mask,
        void* data)
{
    if (!m_detached)
    {
        ERROR_LOG(
                "Channel::SetIOEventCallback should be invoked after detached.");
        return false;
    }
    int rd_fd = GetReadFD();
    int wr_fd = GetWriteFD();

    if (rd_fd == wr_fd)
    {
        if (aeCreateFileEvent(m_service.GetRawEventLoop(), rd_fd, mask, cb,
                data) == AE_ERR)
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
            if (aeCreateFileEvent(m_service.GetRawEventLoop(), rd_fd,
                    AE_READABLE, cb, data) == AE_ERR)
            {
                ::close(rd_fd);
                ERROR_LOG("Failed to register event for fd:%d.", rd_fd);
                return false;
            }
        }
        else if (mask & AE_WRITABLE)
        {
            if (aeCreateFileEvent(m_service.GetRawEventLoop(), wr_fd,
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
    if (GetReadFD() > 0)
    {
        DEBUG_LOG("Detach fd(%d) from event loop.", GetReadFD());
        aeDeleteFileEvent(m_service.GetRawEventLoop(), GetReadFD(), AE_WRITABLE
                | AE_READABLE);
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
    TRACE_LOG("Flush time task trigger.");
    m_flush_timertask_id = -1;
    Flush();
}

void Channel::EnableWriting()
{
    int write_fd = GetWriteFD();
    if (write_fd > 0)
    {
        m_writable = false;
        aeCreateFileEvent(m_service.GetRawEventLoop(), GetWriteFD(),
                AE_WRITABLE, Channel::IOEventCallback, this);
    }
    else
    {
        WARN_LOG("Invalid fd:%d for enable writing for channel[%u].", write_fd, m_id);
    }
}
void Channel::DisableWriting()
{
    int write_fd = GetWriteFD();
    if (write_fd > 0)
    {
        m_writable = true;
        aeDeleteFileEvent(m_service.GetRawEventLoop(), write_fd, AE_WRITABLE);
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
        TRACE_LOG("Canecl flush time task.");
        m_service.GetTimer().Cancel(m_flush_timertask_id);
        m_flush_timertask_id = -1;
    }
}

void Channel::CreateFlushTimerTask()
{
    if (-1 == m_flush_timertask_id)
    {
        m_flush_timertask_id = m_service.GetTimer().Schedule(this,
                m_options.user_write_buffer_flush_timeout_mills, -1,
                MILLIS);
        TRACE_LOG(
                "Create flush task(%u) triggered after %ums.", m_flush_timertask_id, m_options.user_write_buffer_flush_timeout_mills);
    }
}

int32 Channel::WriteNow(Buffer* buffer)
{
    uint32 buf_len = NULL != buffer ? buffer->ReadableBytes() : 0;
    TRACE_LOG(
            "Write %u bytes for channel:channel id %u & type:%u", buf_len, GetID(), GetID() & 0xf);
    if (m_outputBuffer.Readable())
    {
        if (m_options.max_write_buffer_size > 0) //write buffer size limit enable
        {
            uint32 write_buffer_size = m_outputBuffer.ReadableBytes();
            if (write_buffer_size > m_options.max_write_buffer_size
                    || (write_buffer_size + buf_len)
                            > m_options.max_write_buffer_size)
            {
                //overflow
                return 0;
            }
        }
        m_outputBuffer.Write(buffer, buf_len);
        if (m_options.user_write_buffer_water_mark > 0
                && m_outputBuffer.ReadableBytes()
                        < m_options.user_write_buffer_water_mark)
        {
            CreateFlushTimerTask();
        }
        else
        {
            //EnableWriting();
            Flush();
        }
        return buf_len;
    }
    else
    {
        if (buf_len < m_options.user_write_buffer_water_mark || !m_writable)
        {
            m_outputBuffer.Write(buffer, buf_len);
            CreateFlushTimerTask();
            return buf_len;
        }
        int err;
        int ret = buffer->WriteFD(GetWriteFD(), err);
        if (ret < 0)
        {
            if (IO_ERR_RW_RETRIABLE(err))
            {
                //nothing
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
            m_outputBuffer.EnsureWritableBytes(
                    options.user_write_buffer_water_mark * 2);
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
    TRACE_LOG("Flush %u bytes for channel.", m_outputBuffer.ReadableBytes());
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
                EnableWriting();
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
        m_outputBuffer.Compact(
                m_options.user_write_buffer_water_mark > 0 ? m_options.user_write_buffer_water_mark
                        * 2
                        : 8192);
        if ((uint32) ret < send_buf_len)
        {
            EnableWriting();
        }
        else
        {
            //fireWriteComplete(this);
        }
        return true;
    }
    else
    {
        m_outputBuffer.Compact(
                m_options.user_write_buffer_water_mark > 0 ? m_options.user_write_buffer_water_mark
                        : 8192);
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
    m_inputBuffer.DiscardReadedBytes();
    int32 len = ReadNow(&m_inputBuffer);
    if (len > 0)
    {
        TRACE_LOG(
                "DataReceived with %d bytes in channel %u.", m_inputBuffer.ReadableBytes(), GetID());
        fire_message_received<Buffer> (this, &m_inputBuffer, NULL);
    }
    else
    {
        TRACE_LOG(
                "[ERROR]Failed to read channel for ret:%d for fd:%d, channel id %u & type:%u", len, GetReadFD(), GetID(), GetID() & 0xf);
    }
}

void Channel::OnWrite()
{
    DisableWriting();
    if (!Flush())
    {
        fire_channel_closed(this);
    }else
    {
        if(m_close_after_write)
        {
            Close();
        }
    }
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
    if(!m_writable)
    {
        return false;
    }
    return DoFlush();
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
        aeDeleteFileEvent(m_service.GetRawEventLoop(), read_fd, AE_READABLE
                | AE_WRITABLE);
        hasfd = true;
    }
    if (read_fd != write_fd && write_fd > 0)
    {
        aeDeleteFileEvent(m_service.GetRawEventLoop(), write_fd, AE_WRITABLE);
        hasfd = true;
    }
    CancelFlushTimerTask();

    bool ret = false;
    if (hasfd && DoClose() && !inDestructor)
    {
        ret = fire_channel_closed(this);
        if (0 != m_parent_id)
        {
            Channel* parent = m_service.GetChannel(m_parent_id);
            if (NULL != parent)
            {
                parent->OnChildClose(this);
            }
        }
    }
    //DEBUG_LOG("After close channel, read fd:%d, write fd:%d", GetReadFD(),  GetWriteFD());
    if (!m_has_removed && !inDestructor)
    {
        m_has_removed = true;
        m_service.RemoveChannel(this);
    }
    return ret;
}

bool Channel::Close()
{
    if(m_outputBuffer.Readable() && GetWriteFD() > 0)
    {
        EnableWriting();
        m_close_after_write = true;
        return true;
    }
    return DoClose(false);
}

Channel::~Channel()
{
    DoClose(true);
    if (NULL != m_pipeline_finallizer)
    {
        m_pipeline_finallizer(&m_pipeline, m_pipeline_finallizer_user_data);
    }
}
