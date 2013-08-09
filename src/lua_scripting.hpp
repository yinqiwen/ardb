/*
 * lua_scripting.hpp
 *
 *  Created on: 2013-8-6
 *      Author: yinqiwen@gmail.com
 */

#ifndef LUA_SCRIPTING_HPP_
#define LUA_SCRIPTING_HPP_

#include <string>
#include <btree_map.h>
#include <btree_set.h>
#ifdef __cplusplus
extern "C"
{
#endif
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#ifdef __cplusplus
}
#endif

#include "channel/all_includes.hpp"
#include "ardb_data.hpp"

using namespace ardb::codec;

namespace ardb
{
	class ArdbServer;
	class LUAInterpreter
	{
		private:
			lua_State *m_lua;
			ArdbServer* m_server;

			static int PCall(lua_State *lua);
			static int Call(lua_State *lua);
			static int Log(lua_State *lua);
			static int SHA1Hex(lua_State *lua);
			static int ErrorReplyCommand(lua_State *lua);
			static int StatusReplyCommand(lua_State *lua);
			static int MathRandom(lua_State *lua);
			static int MathRandomSeed(lua_State *lua);
			int LoadLibs();
			int RemoveUnsupportedFunctions();
			int CreateLuaFunction(const std::string& funcname, const std::string& body, std::string& err);
			int Init();
		public:
			LUAInterpreter(ArdbServer* server);
			int Eval(const std::string& func, SliceArray& keys,
					SliceArray& args, bool isSHA1Func, RedisReply& reply);
			~LUAInterpreter();
	};
}

#endif /* LUA_SCRIPTING_HPP_ */
