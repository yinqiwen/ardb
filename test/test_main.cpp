/*
 *Copyright (c) 2016-2016, yinqiwen <yinqiwen@gmail.com>
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

#include <stdio.h>
#include "util/file_helper.hpp"
#include "util/config_helper.hpp"
#include "util/time_helper.hpp"
#include "command/lua_scripting.hpp"
#include "db/db.hpp"
#include "config.hpp"

using namespace ardb;

int main()
{
    Ardb db;
    if (db.Init("../test/ardb-test.conf") != 0)
    {
        printf("Failed to init db.\n");
        return -1;
    }
    LUAInterpreter interpreter;
    std::deque<std::string> fs;
    std::string command_test_path = "../commands/";
    list_subfiles(command_test_path, fs);

    for (size_t i = 0; i < fs.size(); i++)
    {
        if(has_suffix(fs[i], ".lua"))
        {
            printf("=======================%s Test Begin============================\n", fs[i].c_str());
            uint64 start = get_current_epoch_millis();
            Context tmpctx;
            interpreter.EvalFile(tmpctx, command_test_path + fs[i]);
            RedisReply& r = tmpctx.GetReply();
            if(r.IsErr())
            {
                fprintf(stderr, "%s %s\n", fs[i].c_str(), r.Error().c_str());
            }
            uint64 end = get_current_epoch_millis();
            printf("=======================%s Test End(%llums)============================\n\n", fs[i].c_str(), (end - start));
            if(r.IsErr())
            {
                return -1;
            }
        }
    }
    return 0;
}

