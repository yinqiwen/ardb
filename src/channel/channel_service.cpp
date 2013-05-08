/*
 * ChannelService.cpp
 *
 *  Created on: 2011-1-10
 *      Author: qiyingwang
 */

#include "channel/all_includes.hpp"
#include "util/helpers.hpp"
#include "util/datagram_packet.hpp"
#include "util/buffer_helper.hpp"
#include <list>

using namespace ardb;

ChannelService::ChannelService(uint32 setsize) :
		m_eventLoop(NULL), m_timer(NULL), m_signal_channel(NULL), m_self_soft_signal_channel(
				NULL), m_running(false)
{
	m_eventLoop = aeCreateEventLoop(setsize);
}

void ChannelService::OnSoftSignal(uint32 soft_signo, uint32 appendinfo)
{
	switch (soft_signo)
	{
		case CHANNEL_REMOVE:
		{
			//uint32 chanel_id = appendinfo;
			VerifyRemoveQueue();
			break;
		}
		default:
		{
			break;
		}
	}
}

bool ChannelService::EventSunk(ChannelPipeline* pipeline, ChannelStateEvent& e)
{
	return false;
}

bool ChannelService::EventSunk(ChannelPipeline* pipeline,
		MessageEvent<Buffer>& e)
{
	Buffer* buffer = e.GetMessage();
	RETURN_FALSE_IF_NULL(buffer);
	uint32 len = buffer->ReadableBytes();
	int32 ret = e.GetChannel()->WriteNow(buffer);
	return ret >= 0 ? (len == (uint32) ret) : false;
}

bool ChannelService::EventSunk(ChannelPipeline* pipeline,
		MessageEvent<DatagramPacket>& e)
{
	DatagramPacket* packet = e.GetMessage();
	Channel* ch = e.GetChannel();
	DatagramChannel* sc = (DatagramChannel*) ch;
	return sc->Send(packet->GetInetAddress(), &(packet->GetBuffer())) > 0;
}

Channel* ChannelService::GetChannel(uint32_t channelID)
{
	if (m_channel_table.count(channelID) > 0)
	{
		return m_channel_table[channelID];
	}
	return NULL;
}

bool ChannelService::DetachChannel(Channel* ch, bool remove)
{
	RETURN_FALSE_IF_NULL(ch);
	ch->DetachFD();
	if (remove)
	{
		m_channel_table.erase(ch->GetID());
	}
	return true;
}

Channel* ChannelService::AttachChannel(Channel* ch, bool transfer_service_only)
{
	RETURN_FALSE_IF_NULL(ch);
	if (transfer_service_only)
	{
		ch->m_service = this;
		ch->AttachFD();
		return ch;
	}
	if (m_channel_table.count(ch->GetID()) > 0)
	{
		return false;
	}
	ch->DetachFD();

	if (ch->GetReadFD() != ch->GetWriteFD())
	{
		ERROR_LOG(
				"Failed to attach channel since source channel has diff read fd & write fd.");
		return false;
	}
	Channel* newch = CloneChannel(ch);
	if (NULL != newch)
	{
		newch->AttachFD(ch->GetReadFD());
		newch->Configure(ch->m_options);
	}
	return newch;
}

Channel* ChannelService::CloneChannel(Channel* ch)
{
	RETURN_NULL_IF_NULL(ch);
	uint32 channelID = ch->GetID();
	uint32 type = channelID & 0xF;
	Channel* newch = NULL;
	switch (type)
	{
		case TCP_CLIENT_SOCKET_CHANNEL_ID_BIT_MASK:
		{
			newch = NewClientSocketChannel();
			break;
		}
		case TCP_SERVER_SOCKET_CHANNEL_ID_BIT_MASK:
		{
			ServerSocketChannel* tmp = NewServerSocketChannel();
			ServerSocketChannel* osc = static_cast<ServerSocketChannel*>(ch);
			tmp->SetSocketAcceptedCallBack(osc->m_accepted_cb);
			newch = tmp;
			break;
		}
		case UDP_SOCKET_CHANNEL_ID_BIT_MASK:
		{
			newch = NewDatagramSocketChannel();
			break;
		}
		default:
		{
			ERROR_LOG("Not support clone non TCP/UDP socket channel.");
			return NULL;
		}
	}
	if (NULL != newch)
	{

		if (NULL != ch->m_pipeline_initializor)
		{
			newch->SetChannelPipelineInitializor(ch->m_pipeline_initializor,
					ch->m_pipeline_initailizor_user_data);
		}
		if (NULL != ch->m_pipeline_finallizer)
		{
			newch->SetChannelPipelineFinalizer(ch->m_pipeline_finallizer,
					ch->m_pipeline_finallizer_user_data);
		}
	}

	return newch;
}

void ChannelService::Start(bool self_routine)
{
	if (!m_running)
	{
		m_self_soft_signal_channel = NewSoftSignalChannel();
		if (NULL != m_self_soft_signal_channel)
		{
			m_self_soft_signal_channel->Register(CHANNEL_REMOVE, this);
		}
		//if (self_routine)
		{
			GetTimer().Schedule(this, 1000, 500);
		}
		m_running = true;
		aeMain(m_eventLoop);
	}
}

void ChannelService::Stop()
{
	if (m_running)
	{
		m_running = false;
		aeStop(m_eventLoop);
	}
}

Timer& ChannelService::GetTimer()
{
	if (NULL == m_timer)
	{
		m_timer = NewTimerChannel();
	}
	return *m_timer;
}

SignalChannel& ChannelService::GetSignalChannel()
{
	if (NULL == m_signal_channel)
	{
		m_signal_channel = NewSignalChannel();
	}
	return *m_signal_channel;
}

TimerChannel* ChannelService::NewTimerChannel()
{
	TimerChannel* ch = NULL;
	NEW(ch, TimerChannel(*this));
	if (NULL != ch)
	{
		Channel* c = ch;
		c->m_id = (((c->m_id) << 4) + TIMER_CHANNEL_ID_BIT_MASK);
		m_channel_table[c->m_id] = c;
		//ch->GetPipeline().Attach(ch, this);
	}
	return ch;
}

SignalChannel* ChannelService::NewSignalChannel()
{
	SignalChannel* ch = NULL;
	NEW(ch, SignalChannel(*this));
	if (NULL != ch)
	{
		Channel* c = ch;
		c->m_id = (((c->m_id) << 4) + SIGNAL_CHANNEL_ID_BIT_MASK);
		m_channel_table[c->m_id] = c;
		//ch->GetPipeline().Attach(ch, this);
	}
	ch->Open();
	return ch;
}

SoftSignalChannel* ChannelService::NewSoftSignalChannel()
{
	SoftSignalChannel* ch = NULL;
	NEW(ch, SoftSignalChannel(*this));
	if (NULL != ch)
	{
		Channel* c = ch;
		c->m_id = (((c->m_id) << 4) + SOFT_SIGNAL_CHANNEL_ID_BIT_MASK);
		m_channel_table[c->m_id] = c;
		//ch->GetPipeline().Attach(ch, this);
	}
	ch->Open();
	return ch;
}

ClientSocketChannel* ChannelService::NewClientSocketChannel()
{
	ClientSocketChannel* ch = NULL;
	NEW(ch, ClientSocketChannel(*this));
	if (NULL != ch)
	{
		Channel* c = ch;
		c->m_id = (((c->m_id) << 4) + TCP_CLIENT_SOCKET_CHANNEL_ID_BIT_MASK);
		m_channel_table[c->m_id] = c;
		ch->GetPipeline().Attach(ch);
	}
	return ch;
}
ServerSocketChannel* ChannelService::NewServerSocketChannel()
{
	ServerSocketChannel* ch = NULL;
	NEW(ch, ServerSocketChannel(*this));
	if (NULL != ch)
	{
		Channel* c = ch;
		c->m_id = (((c->m_id) << 4) + TCP_SERVER_SOCKET_CHANNEL_ID_BIT_MASK);
		m_channel_table[c->m_id] = c;
		ch->GetPipeline().Attach(ch);
	}
	return ch;
}

PipeChannel* ChannelService::NewPipeChannel(int readFd, int writeFD)
{
	PipeChannel* ch = NULL;
	NEW(ch, PipeChannel(*this, readFd, writeFD));
	if (NULL != ch)
	{
		Channel* c = ch;
		c->m_id = (((c->m_id) << 4) + FIFO_CHANNEL_ID_BIT_MASK);
		m_channel_table[c->m_id] = c;
		ch->GetPipeline().Attach(ch);
	}
	return ch;
}
DatagramChannel* ChannelService::NewDatagramSocketChannel()
{
	DatagramChannel* ch = NULL;
	NEW(ch, DatagramChannel(*this));
	if (NULL != ch)
	{
		Channel* c = ch;
		c->m_id = (((c->m_id) << 4) + UDP_SOCKET_CHANNEL_ID_BIT_MASK);
		m_channel_table[c->m_id] = c;
		ch->GetPipeline().Attach(ch);
	}
	return ch;
}

void ChannelService::RemoveChannel(Channel* ch)
{
	if (NULL != ch)
	{
		if (NULL != m_self_soft_signal_channel)
		{
			m_remove_queue.push_back(ch->GetID());
			m_self_soft_signal_channel->FireSoftSignal(CHANNEL_REMOVE,
					ch->GetID());
		}
	}
}

void ChannelService::DeleteClientSocketChannel(ClientSocketChannel* ch)
{
	DELETE(ch);
}
void ChannelService::DeleteServerSocketChannel(ServerSocketChannel* ch)
{
	DELETE(ch);
}
void ChannelService::DeleteDatagramChannel(DatagramChannel* ch)
{
	DELETE(ch);
}

void ChannelService::VerifyRemoveQueue()
{
	RemoveChannelQueue::iterator it = m_remove_queue.begin();
	while (it != m_remove_queue.end())
	{
		Channel* ch = GetChannel(*it);
		if (NULL != ch)
		{
			DeleteChannel(ch);
		}
		it++;
	}
	m_remove_queue.clear();
}

void ChannelService::DeleteChannel(Channel* ch)
{
	RETURN_IF_NULL(ch);
	ChannelTable::iterator found = m_channel_table.find(ch->GetID());
	if (found != m_channel_table.end())
	{
		m_channel_table.erase(found);
	}
	DELETE(ch);
}

void ChannelService::Run()
{
	VerifyRemoveQueue();
}

void ChannelService::Routine()
{
	Run();
}

void ChannelService::CloseAllChannelFD(std::set<Channel*>& exceptions)
{
	ChannelTable::iterator it = m_channel_table.begin();
	while (it != m_channel_table.end())
	{
		Channel* ch = it->second;
		if (exceptions.find(ch) == exceptions.end())
		{
			::close(ch->GetReadFD());
			::close(ch->GetWriteFD());
		}
		it++;
	}
}

void ChannelService::CloseAllChannels(bool fireCloseEvent)
{
	ChannelTable::iterator it = m_channel_table.begin();
	std::list<Channel*> temp;
	while (it != m_channel_table.end())
	{
		Channel* ch = it->second;
		temp.push_back(ch);
		it++;
	}
	std::list<Channel*>::iterator tit = temp.begin();
	while (tit != temp.end())
	{
		Channel* ch = *tit;
		if(fireCloseEvent)
		{
			ch->Close();
		}
		DeleteChannel(ch);
		tit++;
	}
}

ChannelService::~ChannelService()
{
	CloseAllChannels(false);
	aeDeleteEventLoop(m_eventLoop);
}
