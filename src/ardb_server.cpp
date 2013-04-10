/*
 * ardb_server.cpp
 *
 *  Created on: 2013-4-8
 *      Author: wqy
 */
#include "ardb_server.hpp"
#include "util/config_helper.hpp"
#include <stdarg.h>

#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6

namespace ardb
{
	static inline void fill_error_reply(ArdbReply& reply, const char* fmt, ...)
	{
		va_list ap;
		va_start(ap, fmt);
		char buf[1024];
		vsnprintf(buf, sizeof(buf) - 1, fmt, ap);
		va_end(ap);
		reply.type = REDIS_REPLY_ERROR;
		reply.str = buf;
	}

	static inline void fill_status_reply(ArdbReply& reply, const char* fmt, ...)
	{
		va_list ap;
		va_start(ap, fmt);
		char buf[1024];
		vsnprintf(buf, sizeof(buf) - 1, fmt, ap);
		va_end(ap);
		reply.type = REDIS_REPLY_STATUS;
		reply.str = buf;
	}

	static void encode_reply(Buffer& buf, ArdbReply& reply)
	{
		switch (reply.type)
		{
			case REDIS_REPLY_NIL:
			{
				buf.Printf("$-1\r\n");
				break;
			}
			case REDIS_REPLY_STRING:
			{
				buf.Printf("$%d\r\n", reply.str.size());
				if (reply.str.size() > 0)
				{
					buf.Printf("%s\r\n", reply.str.c_str());
				} else
				{
					buf.Printf("\r\n");
				}
				break;
			}
			case REDIS_REPLY_ERROR:
			{
				buf.Printf("-%s\r\n", reply.str.c_str());
				break;
			}
			case REDIS_REPLY_INTEGER:
			{
				buf.Printf(":%lld\r\n", reply.integer);
				break;
			}
			case REDIS_REPLY_ARRAY:
			{
				buf.Printf("*%d\r\n", reply.elements.size());
				size_t i = 0;
				while (i < reply.elements.size())
				{
					encode_reply(buf, reply.elements[i]);
					i++;
				}
				break;
			}
			case REDIS_REPLY_STATUS:
			{
				buf.Printf("+%s\r\n", reply.str.c_str());
				break;
			}
			default:
			{
				ERROR_LOG("Recv unexpected redis reply type:%d", reply.type);
				break;
			}
		}
	}

	int ArdbServer::ParseConfig(const std::string& file, ArdbServerConfig& cfg)
	{
		Properties props;
		if (!parse_conf_file(file, props, " "))
		{
			fprintf(stderr, "Failed to parse config file:%s", file.c_str());
			return -1;
		}
		conf_get_int64(props, "port", cfg.listen_port);
		conf_get_string(props, "bind", cfg.listen_host);
		conf_get_string(props, "unixsocket", cfg.listen_unix_path);
		std::string daemonize;
		conf_get_string(props, "daemonize", daemonize);
		daemonize = string_tolower(daemonize);
		if (daemonize == "yes")
		{
			cfg.daemonize = true;
		}
		return 0;
	}

	ArdbServer::ArdbServer() :
			m_service(NULL)
	{
		struct RedisCommandHandlerSetting settingTable[] = { { "ping",
				&ArdbServer::Ping, 0 }, { "echo", &ArdbServer::Echo, 1 }, {
				"quit", &ArdbServer::Quit, 0 }, { "select", &ArdbServer::Select,
				1 }, };

		uint32 arraylen = arraysize(settingTable);
		for (uint32 i = 0; i < arraylen; i++)
		{
			m_handler_table[settingTable[i].name] = settingTable[i];
		}
	}
	ArdbServer::~ArdbServer()
	{
		DELETE(m_service);
	}

	int ArdbServer::Ping(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		fill_status_reply(ctx.reply, "PONG");
		return 0;
	}
	int ArdbServer::Echo(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		ctx.reply.str = cmd[0];
		ctx.reply.type = REDIS_REPLY_STRING;
		return 0;
	}
	int ArdbServer::Select(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int64 idx = 0;
		if (!string_toint64(cmd[0], idx) || idx < 0)
		{
			fill_error_reply(ctx.reply, "ERR invalid DB index");
		} else
		{
			ctx.currentDB = idx;
			fill_status_reply(ctx.reply, "OK");
			DEBUG_LOG("db is %s  %d", cmd[0].c_str(), idx);
		}
		return 0;
	}

	int ArdbServer::Quit(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		fill_status_reply(ctx.reply, "OK");
		return -1;
	}

	void ArdbServer::ProcessRedisCommand(ArdbConnContext& ctx,
			RedisCommandFrame& args)
	{
		ctx.reply.Clear();
		std::string* cmd = args.GetArgument(0);
		if (NULL != cmd)
		{
			int ret = 0;
			lower_string(*cmd);
			RedisCommandHandlerSettingTable::iterator found =
					m_handler_table.find(*cmd);
			if (found != m_handler_table.end())
			{
				RedisCommandHandler handler = found->second.handler;
				args.GetArguments().pop_front();

				if (found->second.arity >= 0
						&& args.GetArguments().size() != found->second.arity)
				{
					fill_error_reply(ctx.reply,
							"ERR wrong number of arguments for '%s' command",
							cmd->c_str());
				} else
				{
					ret = (this->*handler)(ctx, args.GetArguments());
				}

			} else
			{
				ERROR_LOG("No handler found for:%s", cmd->c_str());
				fill_error_reply(ctx.reply, "ERR unknown command '%s'",
						cmd->c_str());
			}

			Buffer buf;
			if (ctx.reply.type != 0)
			{
				Buffer buf;
				encode_reply(buf, ctx.reply);
				ctx.conn->Write(buf);
			}
			if (ret < 0)
			{
				ctx.conn->Close();
			}
		}
	}

	static void ardb_pipeline_init(ChannelPipeline* pipeline, void* data)
	{
		ChannelUpstreamHandler<RedisCommandFrame>* handler =
				(ChannelUpstreamHandler<RedisCommandFrame>*) data;
		pipeline->AddLast("decoder", new RedisFrameDecoder);
		pipeline->AddLast("handler", handler);
	}

	static void ardb_pipeline_finallize(ChannelPipeline* pipeline, void* data)
	{
		ChannelHandler* handler = pipeline->Get("decoder");
		DELETE(handler);
	}

	int ArdbServer::Start(const ArdbServerConfig& cfg)
	{
		m_cfg = cfg;
		struct RedisRequestHandler: public ChannelUpstreamHandler<
				RedisCommandFrame>
		{
				ArdbServer* server;
				ArdbConnContext ardbctx;
				void MessageReceived(ChannelHandlerContext& ctx,
						MessageEvent<RedisCommandFrame>& e)
				{
					ardbctx.conn = ctx.GetChannel();
					server->ProcessRedisCommand(ardbctx, *(e.GetMessage()));
				}
				RedisRequestHandler(ArdbServer* s) :
						server(s)
				{
				}
		};
		m_service = new ChannelService(m_cfg.max_clients + 32);
		RedisRequestHandler handler(this);

		ChannelOptions ops;
		ops.tcp_nodelay = true;
		if (m_cfg.listen_host.empty() && m_cfg.listen_unix_path.empty())
		{
			m_cfg.listen_host = "0.0.0.0";
			if (m_cfg.listen_port == 0)
			{
				m_cfg.listen_port = 6379;
			}
		}
		if (!m_cfg.listen_host.empty())
		{
			SocketHostAddress address(m_cfg.listen_host.c_str(),
					m_cfg.listen_port);
			ServerSocketChannel* server = m_service->NewServerSocketChannel();
			if (!server->Bind(&address))
			{
				ERROR_LOG(
						"Failed to bind on %s:%d", m_cfg.listen_host.c_str(), m_cfg.listen_port);
				return -1;
			}
			server->Configure(ops);
			server->SetChannelPipelineInitializor(ardb_pipeline_init, &handler);
			server->SetChannelPipelineFinalizer(ardb_pipeline_finallize, NULL);
		}
		if (!m_cfg.listen_unix_path.empty())
		{
			SocketUnixAddress address(m_cfg.listen_unix_path);
			ServerSocketChannel* server = m_service->NewServerSocketChannel();
			if (!server->Bind(&address))
			{
				ERROR_LOG(
						"Failed to bind on %s", m_cfg.listen_unix_path.c_str());
				return -1;
			}
			server->Configure(ops);
			server->SetChannelPipelineInitializor(ardb_pipeline_init, &handler);
			server->SetChannelPipelineFinalizer(ardb_pipeline_finallize, NULL);
		}
		m_service->Start();
		return 0;
	}
}

