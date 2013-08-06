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
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

namespace ardb
{
	class LUAInterpreter
	{
		private:
			lua_State *m_lua;

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

		public:
			LUAInterpreter();
			int Init();
			~LUAInterpreter();
	};
}

#endif /* LUA_SCRIPTING_HPP_ */
