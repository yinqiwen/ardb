/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
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
#ifndef LUA_SCRIPTING_HPP_
#define LUA_SCRIPTING_HPP_

#include <string>
extern "C"
{
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
}
#include "context.hpp"


using namespace ardb::codec;

namespace ardb
{
    class LUAInterpreter
    {
        private:
            lua_State *m_lua;

            static int CallArdb(lua_State *lua, bool raise_error);
            static int PCall(lua_State *lua);
            static int Call(lua_State *lua);
            static int Log(lua_State *lua);
            static int Assert2(lua_State *lua);
            static int IsMergeSupported(lua_State *lua);
            static int SHA1Hex(lua_State *lua);
            static int ReturnSingleFieldTable(lua_State *lua, const std::string& field);
            static int ErrorReplyCommand(lua_State *lua);
            static int StatusReplyCommand(lua_State *lua);
            static int MathRandom(lua_State *lua);
            static int MathRandomSeed(lua_State *lua);
            static void MaskCountHook(lua_State *lua, lua_Debug *ar);
            int LoadLibs();
            int RemoveUnsupportedFunctions();
            int CreateLuaFunction(const std::string& funcname, const std::string& body, std::string& err);
            int Init();
            void Reset();
        public:
            LUAInterpreter();
            int Eval(Context& ctx, const std::string& func, const StringArray& keys, const StringArray& args, bool isSHA1Func);
            int EvalFile(Context& ctx, const std::string& file);
            int Load(const std::string& func, std::string& ret);

            ~LUAInterpreter();
    };
}

#endif /* LUA_SCRIPTING_HPP_ */
