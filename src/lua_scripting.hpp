/*
 * lua_scripting.hpp
 *
 *  Created on: 2013-8-6
 *      Author: yinqiwen@gmail.com
 */

#ifndef LUA_SCRIPTING_HPP_
#define LUA_SCRIPTING_HPP_

#include <string>
extern "C"
{
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
}
#include "channel/all_includes.hpp"
#include "data_format.hpp"

using namespace ardb::codec;

#define SCRIPT_KILL_EVENT 1
#define SCRIPT_FLUSH_EVENT 2

namespace ardb
{
    class ArdbServer;
    class LUAInterpreter
    {
        private:
            lua_State *m_lua;

            static ArdbServer* m_server;
            static std::string m_killing_func;

            static int CallArdb(lua_State *lua, bool raise_error);
            static int PCall(lua_State *lua);
            static int Call(lua_State *lua);
            static int Log(lua_State *lua);
            static int SHA1Hex(lua_State *lua);
            static int ReturnSingleFieldTable(lua_State *lua,
                            const std::string& field);
            static int ErrorReplyCommand(lua_State *lua);
            static int StatusReplyCommand(lua_State *lua);
            static int MathRandom(lua_State *lua);
            static int MathRandomSeed(lua_State *lua);
            static void MaskCountHook(lua_State *lua, lua_Debug *ar);
            int LoadLibs();
            int RemoveUnsupportedFunctions();
            int CreateLuaFunction(const std::string& funcname,
                            const std::string& body, std::string& err);
            int Init();
            void Reset();
        public:
            LUAInterpreter(ArdbServer* server);
            int Eval(const std::string& func, SliceArray& keys,
                            SliceArray& args, bool isSHA1Func,
                            RedisReply& reply);
            bool Exists(const std::string& sha);
            int Load(const std::string& func, std::string& ret);
            int Flush();
            int Kill(const std::string& funcname);

            static void ScriptEventCallback(ChannelService* serv, uint32 ev,
                            void* data);
            ~LUAInterpreter();
    };
}

#endif /* LUA_SCRIPTING_HPP_ */
