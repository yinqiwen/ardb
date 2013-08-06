/*
 * lua_scripting.cpp
 *
 *  Created on: 2013-8-6
 *      Author: yinqiwen@gmail.com
 */
#include "lua_scripting.hpp"
#include "logger.hpp"

namespace ardb
{
	static void luaLoadLib(lua_State *lua, const char *libname,
	        lua_CFunction luafunc)
	{
		lua_pushcfunction(lua, luafunc);
		lua_pushstring(lua, libname);
		lua_call(lua, 1, 0);
	}

	LUAInterpreter::LUAInterpreter() :
			m_lua(NULL)
	{

	}

	int LUAInterpreter::LoadLibs()
	{
		luaLoadLib(m_lua, "", luaopen_base);
		luaLoadLib(m_lua, LUA_TABLIBNAME, luaopen_table);
		luaLoadLib(m_lua, LUA_STRLIBNAME, luaopen_string);
		luaLoadLib(m_lua, LUA_MATHLIBNAME, luaopen_math);
		luaLoadLib(m_lua, LUA_DBLIBNAME, luaopen_debug);
		//luaLoadLib(lua, "cjson", luaopen_cjson);
		//luaLoadLib(m_lua, "struct", luaopen_struct);
		//luaLoadLib(lua, "cmsgpack", luaopen_cmsgpack);
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
		return 0;
	}
	int LUAInterpreter::MathRandomSeed(lua_State *lua)
	{
		return 0;
	}

	int LUAInterpreter::Init()
	{
		m_lua = luaL_newstate();

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
		lua_getglobal(m_lua, "math");

		lua_pushstring(m_lua, "random");
		lua_pushcfunction(m_lua, LUAInterpreter::MathRandom);
		lua_settable(m_lua, -3);

		lua_pushstring(m_lua, "randomseed");
		lua_pushcfunction(m_lua, LUAInterpreter::MathRandomSeed);
		lua_settable(m_lua, -3);

		lua_setglobal(m_lua, "math");
		return 0;
	}

	LUAInterpreter::~LUAInterpreter()
	{

	}
}

