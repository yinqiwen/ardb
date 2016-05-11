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
#include "util/helpers.hpp"
#include <errno.h>
#include <string.h>
using namespace ardb;

SoftSignalChannel::SoftSignalChannel(ChannelService& factory) :
        PipeChannel(factory, -1, -1), m_thread_safe(true)
{
}

bool SoftSignalChannel::DoOpen()
{
    if (!PipeChannel::DoOpen())
    {
        return false;
    }
    GetPipeline().AddLast("decoder", &m_decoder);
    GetPipeline().AddLast("handler", this);
    return true;
}

void SoftSignalChannel::MessageReceived(ChannelHandlerContext& ctx, MessageEvent<uint64>& e)
{
    uint64 v = *(e.GetMessage());
    uint32 signo = v & 0xFFFFFFFF;
    uint32 info = ((v >> 32) & 0xFFFFFFFF);
    FireSignalReceived(signo, info);
}

void SoftSignalChannel::FireSignalReceived(uint32 signo, uint32 append_info)
{
    SignalHandlerMap::iterator it = m_hander_map.find(signo);
    if (it != m_hander_map.end())
    {
        std::vector<SoftSignalHandler*>& vec = it->second;
        std::vector<SoftSignalHandler*>::iterator vecit = vec.begin();
        while (vecit != vec.end())
        {
            SoftSignalHandler* handler = *vecit;
            if (NULL != handler)
            {
                handler->OnSoftSignal(signo, append_info);
            }
            vecit++;
        }
    }
    else
    {
        ERROR_LOG("Invalid signo:%u", signo);
    }
}

int SoftSignalChannel::FireSoftSignal(uint32 signo, uint32 info)
{
    uint64 v = info;
    v = (v << 32) + signo;
//    Buffer content((char*) &v, 0, sizeof(v));
//    if (m_thread_safe)
//    {
//        m_lock.Lock();
//    }
//    int ret = WriteNow(&content);
//    if (m_thread_safe)
//    {
//        m_lock.Unlock();
//    }
    int ret = ::write(GetWriteFD(), &v, sizeof(v));
    return ret;
}

void SoftSignalChannel::Register(uint32 signo, SoftSignalHandler* handler)
{
    SignalHandlerMap::iterator it = m_hander_map.find(signo);
    if (it == m_hander_map.end() || it->second.empty())
    {
        m_hander_map[signo].push_back(handler);
    }
    else
    {
        it->second.push_back(handler);
    }
}
void SoftSignalChannel::Unregister(SoftSignalHandler* handler)
{
    SignalHandlerMap::iterator it = m_hander_map.begin();
    while (it != m_hander_map.end())
    {
        std::vector<SoftSignalHandler*>& vec = it->second;
        std::vector<SoftSignalHandler*>::iterator vecit = vec.begin();
        while (vecit != vec.end())
        {
            if (handler == (*vecit))
            {
                vec.erase(vecit);
                break;
            }
            vecit++;
        }
        it++;
    }
}

void SoftSignalChannel::Clear()
{
    m_hander_map.clear();
}

SoftSignalChannel::~SoftSignalChannel()
{
    Clear();
}
