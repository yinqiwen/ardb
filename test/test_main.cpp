/*
 * command_test.cpp
 *
 *  Created on: 2016��3��7��
 *      Author: wangqiying
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
    std::string command_test_path = "../test/commands/";
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

