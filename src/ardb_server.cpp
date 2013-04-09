/*
 * ardb_server.cpp
 *
 *  Created on: 2013-4-8
 *      Author: wqy
 */
#include "ardb_server.hpp"

namespace ardb
{


	int ARDBServer::Ping()
	{

	}
	int ARDBServer::Echo(const std::string& message)
	{

	}
	int ARDBServer::Select(DBID id)
	{

	}


	static void ardbChannelPipelineInit(ChannelPipeline* pipeline, void* data)
	{
	    //TempHandler handler;


	}

	static void ardbChannelPipelineFinallize(ChannelPipeline* pipeline, void* data)
	{
	    DEBUG_LOG("############httpChannelPipelineFinallize");
	    ChannelHandler* handler = pipeline->Get("decoder");
	    DELETE(handler);
	    handler = pipeline->Get("encoder");
	    DELETE(handler);
	}

	int ARDBServer::Start()
	{


		m_server->Start();
	}
}

