/*
 * lua_scripting.cpp
 *
 *  Created on: 2013-8-6
 *      Author: yinqiwen@gmail.com
 */
#include "lua_scripting.hpp"
#include "logger.hpp"
#include <string.h>

#define MAX_LUA_STR_SIZE 1024

namespace ardb
{
	extern "C"
	{
		int (luaopen_cjson)(lua_State *L);
		int (luaopen_struct)(lua_State *L);
		int (luaopen_cmsgpack)(lua_State *L);
	}
	static void luaLoadLib(lua_State *lua, const char *libname,
			lua_CFunction luafunc)
	{
		lua_pushcfunction(lua, luafunc);
		lua_pushstring(lua, libname);
		lua_call(lua, 1, 0);
	}

	static void luaPushError(lua_State *lua, char *error) {
	    lua_Debug dbg;

	    lua_newtable(lua);
	    lua_pushstring(lua,"err");

	    /* Attempt to figure out where this function was called, if possible */
	    if(lua_getstack(lua, 1, &dbg) && lua_getinfo(lua, "nSl", &dbg)) {
	    	    char tmp[MAX_LUA_STR_SIZE];
	    	    snprintf(tmp, MAX_LUA_STR_SIZE - 1, "%s: %d: %s",
	            dbg.source, dbg.currentline, error);
	        //sds msg = sdscatprintf(sdsempty(), "%s: %d: %s",
	        //    dbg.source, dbg.currentline, error);
	        lua_pushstring(lua, tmp);
	    } else {
	        lua_pushstring(lua, error);
	    }
	    lua_settable(lua,-3);
	}

	static int luaReplyToRedisReply(lua_State *lua, RedisReply& reply)
	{
		int t = lua_type(lua, -1);
		switch (t)
		{
			case LUA_TSTRING:
			{
				reply.type = REDIS_REPLY_STRING;
				reply.str.append((char*) lua_tostring(lua,-1),
				lua_strlen(lua, -1));
				break;
			}
			case LUA_TBOOLEAN:
				if (lua_toboolean(lua, -1))
				{
					reply.type = REDIS_REPLY_INTEGER;
					reply.integer = 1;
				} else
				{
					reply.type = REDIS_REPLY_NIL;
				}
				break;
			case LUA_TNUMBER:
				reply.type = REDIS_REPLY_INTEGER;
				reply.integer = (long long) lua_tonumber(lua, -1);
				break;
			case LUA_TTABLE:
				/* We need to check if it is an array, an error, or a status reply.
				 * Error are returned as a single element table with 'err' field.
				 * Status replies are returned as single element table with 'ok' field */
				lua_pushstring(lua, "err");
				lua_gettable(lua, -2);
				t = lua_type(lua, -1);
				if (t == LUA_TSTRING)
				{
					std::string err = lua_tostring(lua,-1);
					string_replace(err, "\r\n", " ");
					//sds err = sdsnew(lua_tostring(lua,-1));
					//sdsmapchars(err, "\r\n", "  ", 2);
					reply.type = REDIS_REPLY_ERROR;
					reply.str = err;
					//addReplySds(c, sdscatprintf(sdsempty(), "-%s\r\n", err));
					//sdsfree(err);
					lua_pop(lua, 2);
					return 0;
				}

				lua_pop(lua, 1);
				lua_pushstring(lua, "ok");
				lua_gettable(lua, -2);
				t = lua_type(lua, -1);
				if (t == LUA_TSTRING)
				{
					std::string ok = lua_tostring(lua,-1);
					string_replace(ok, "\r\n", " ");
					//sds ok = sdsnew(lua_tostring(lua,-1));
					//sdsmapchars(ok, "\r\n", "  ", 2);
					//addReplySds(c, sdscatprintf(sdsempty(), "+%s\r\n", ok));
					//sdsfree(ok);
					reply.str = ok;
					reply.type = REDIS_REPLY_STATUS;
					//reply.str =
					lua_pop(lua, 1);
				} else
				{
					//void *replylen = addDeferredMultiBulkLength(c);
					int j = 1, mbulklen = 0;

					lua_pop(lua, 1); /* Discard the 'ok' field value we popped */
					reply.type = REDIS_REPLY_ARRAY;
					while (1)
					{
						lua_pushnumber(lua, j++);
						lua_gettable(lua, -2);
						t = lua_type(lua, -1);
						if (t == LUA_TNIL)
						{
							lua_pop(lua, 1);
							break;
						}
						RedisReply r;
						luaReplyToRedisReply(lua, r);
						reply.elements.push_back(r);
						mbulklen++;
					}
				}
				break;
			default:
			{
				reply.type = REDIS_REPLY_NIL;
				break;
			}

		}
		lua_pop(lua, 1);
		return 0;
	}

	LUAInterpreter::LUAInterpreter() :
			m_lua(NULL)
	{

	}

	/* Define a lua function with the specified function name and body.
	 * The function name musts be a 2 characters long string, since all the
	 * functions we defined in the Lua context are in the form:
	 *
	 *   f_<hex sha1 sum>
	 *
	 * On success REDIS_OK is returned, and nothing is left on the Lua stack.
	 * On error REDIS_ERR is returned and an appropriate error is set in the
	 * client context. */
	int LUAInterpreter::CreateLuaFunction(const std::string& funcname,
			const std::string& body, std::string& err)
	{
		std::string funcdef = "function ";
		funcdef.append(funcname);
		funcdef.append("() ");
		funcdef.append(body);
		funcdef.append(" end");

		if (luaL_loadbuffer(m_lua, funcdef.c_str(), funcdef.size(),
				"@user_script"))
		{
			err.append("Error compiling script (new function): ").append(
			lua_tostring(m_lua,-1)).append("\n");
			lua_pop(m_lua, 1);
			return -1;
		}
		if (lua_pcall(m_lua, 0, 0, 0))
		{
			err.append("Error running script (new function): ").append(
			lua_tostring(m_lua,-1)).append("\n");
			lua_pop(m_lua, 1);
			return -1;
		}

		/* We also save a SHA1 -> Original script map in a dictionary
		 * so that we can replicate / write in the AOF all the
		 * EVALSHA commands as EVAL using the original script. */
		return 0;
	}

	int LUAInterpreter::LoadLibs()
	{
		luaLoadLib(m_lua, "", luaopen_base);
		luaLoadLib(m_lua, LUA_TABLIBNAME, luaopen_table);
		luaLoadLib(m_lua, LUA_STRLIBNAME, luaopen_string);
		luaLoadLib(m_lua, LUA_MATHLIBNAME, luaopen_math);
		luaLoadLib(m_lua, LUA_DBLIBNAME, luaopen_debug);

		luaLoadLib(m_lua, "cjson", luaopen_cjson);
		luaLoadLib(m_lua, "struct", luaopen_struct);
		luaLoadLib(m_lua, "cmsgpack", luaopen_cmsgpack);
		return 0;
	}

	int LUAInterpreter::RemoveUnsupportedFunctions()
	{
		lua_pushnil(m_lua);
		lua_setglobal(m_lua, "loadfile");
		return 0;
	}

	int LUAInterpreter::PCall(lua_State *lua)
	{
		return 0;
	}

	int LUAInterpreter::Call(lua_State *lua)
	{
		return 0;
	}

	int LUAInterpreter::Log(lua_State *lua)
	{
		int j, argc = lua_gettop(lua);
		int level;
	    std::string log;
		if (argc < 2)
		{
			luaPushError(lua, "redis.log() requires two arguments or more.");
			return 1;
		} else if (!lua_isnumber(lua, -argc))
		{
			INFO_LOG("####%d",lua_isnumber(lua, -argc));
			luaPushError(lua, "First argument must be a number (log level).");
			return 1;
		}
		level = lua_tonumber(lua, -argc);
		if (level < FATAL_LOG_LEVEL || level > TRACE_LOG_LEVEL)
		{
			luaPushError(lua, "Invalid debug level.");
			return 1;
		}

		/* Glue together all the arguments */
		for (j = 1; j < argc; j++)
		{
			size_t len;
			char *s;

			s = (char*) lua_tolstring(lua, (-argc) + j, &len);
			if (s)
			{
				if (j != 1){
					log.append(" ");
				}
				log.append(s, len);
				//log = sdscatlen(log, " ", 1);
				//log = sdscatlen(log, s, len);
			}
		}
		INFO_LOG("####%s",log.c_str());
		LOG_WITH_LEVEL((LogLevel)level, "%s", log.c_str());
		//sdsfree(log);
		return 0;
	}

	int LUAInterpreter::SHA1Hex(lua_State *lua)
	{
		return 0;
	}

	int LUAInterpreter::ErrorReplyCommand(lua_State *lua)
	{
		return 0;
	}
	int LUAInterpreter::StatusReplyCommand(lua_State *lua)
	{
		return 0;
	}
	int LUAInterpreter::MathRandom(lua_State *lua)
	{
		INFO_LOG("###MathRandom");
		return 0;
	}
	int LUAInterpreter::MathRandomSeed(lua_State *lua)
	{
		return 0;
	}

	int LUAInterpreter::Init()
	{
		m_lua = lua_open();

		LoadLibs();
		RemoveUnsupportedFunctions();

		/* Register the redis commands table and fields */
		lua_newtable(m_lua);

		/* redis.call */
		lua_pushstring(m_lua, "call");
		lua_pushcfunction(m_lua, LUAInterpreter::Call);
		lua_settable(m_lua, -3);

		/* redis.pcall */
		lua_pushstring(m_lua, "pcall");
		lua_pushcfunction(m_lua, LUAInterpreter::PCall);
		lua_settable(m_lua, -3);

		/* redis.log and log levels. */
		lua_pushstring(m_lua, "log");
		lua_pushcfunction(m_lua, LUAInterpreter::Log);
		lua_settable(m_lua, -3);

		lua_pushstring(m_lua, "LOG_DEBUG");
		lua_pushnumber(m_lua, DEBUG_LOG_LEVEL);
		lua_settable(m_lua, -3);

		lua_pushstring(m_lua, "LOG_VERBOSE");
		lua_pushnumber(m_lua, TRACE_LOG_LEVEL);
		lua_settable(m_lua, -3);

		lua_pushstring(m_lua, "LOG_NOTICE");
		lua_pushnumber(m_lua, INFO_LOG_LEVEL);
		lua_settable(m_lua, -3);

		lua_pushstring(m_lua, "LOG_WARNING");
		lua_pushnumber(m_lua, WARN_LOG_LEVEL);
		lua_settable(m_lua, -3);

		/* redis.sha1hex */
		lua_pushstring(m_lua, "sha1hex");
		lua_pushcfunction(m_lua, LUAInterpreter::SHA1Hex);
		lua_settable(m_lua, -3);

		/* redis.error_reply and redis.status_reply */
		lua_pushstring(m_lua, "error_reply");
		lua_pushcfunction(m_lua, LUAInterpreter::ErrorReplyCommand);
		lua_settable(m_lua, -3);
		lua_pushstring(m_lua, "status_reply");
		lua_pushcfunction(m_lua, LUAInterpreter::StatusReplyCommand);
		lua_settable(m_lua, -3);

		/* Finally set the table as 'redis' global var. */
		lua_setglobal(m_lua, "redis");
		lua_getglobal(m_lua, "redis");
		lua_setglobal(m_lua, "ardb");

		/* Replace math.random and math.randomseed with our implementations. */
		lua_getglobal(m_lua, LUA_MATHLIBNAME);
		if (lua_isnil(m_lua, -1))
		{
			ERROR_LOG("Failed to load lib math");
		}
		lua_pushstring(m_lua, "random");
		lua_pushcfunction(m_lua, LUAInterpreter::MathRandom);
		lua_settable(m_lua, -3);

		lua_pushstring(m_lua, "randomseed");
		lua_pushcfunction(m_lua, LUAInterpreter::MathRandomSeed);
		lua_settable(m_lua, -3);

		lua_setglobal(m_lua, "math");
		return 0;
	}

	int LUAInterpreter::Eval(const std::string& func, SliceArray& keys,
			SliceArray& args, bool isSHA1Func, RedisReply& reply)
	{
		std::string err;
		std::string funcname = "f_";
		if (isSHA1Func)
		{
			funcname.append(func);
		} else
		{
			funcname.append(sha1_sum(func));
		}

		if (CreateLuaFunction(funcname, func, err))
		{
			return -1;
		} else
		{
			/* Now the following is guaranteed to return non nil */
			lua_getglobal(m_lua, funcname.c_str());
			int err = lua_pcall(m_lua, 0, 1, -2);
			lua_gc(m_lua, LUA_GCSTEP, 1);

			if (err)
			{
				reply.type = REDIS_REPLY_ERROR;
				char tmp[1024];
				snprintf(tmp, 1023, "Error running script (call to %s): %s\n",
						funcname.c_str(), lua_tostring(m_lua,-1));
				reply.str = tmp;
//				addReplyErrorFormat(c,
//						"Error running script (call to %s): %s\n", funcname,
//						lua_tostring(lua,-1));
				lua_pop(m_lua, 1); /* Consume the Lua reply. */
			} else
			{
				/* On success convert the Lua return value into Redis protocol, and
				 * send it to * the client. */
				luaReplyToRedisReply(m_lua, reply);
			}
		}
		return 0;
	}

	LUAInterpreter::~LUAInterpreter()
	{
		lua_close(m_lua);
	}
}

