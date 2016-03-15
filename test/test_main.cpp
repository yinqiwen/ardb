/*
 * command_test.cpp
 *
 *  Created on: 2016��3��7��
 *      Author: wangqiying
 */

#include <stdio.h>
#include "util/file_helper.hpp"
#include "util/config_helper.hpp"
#include "command/lua_scripting.hpp"
#include "db/db.hpp"
#include "config.hpp"

using namespace ardb;

int main()
{
    Properties props;
    const char *configfile = "../test/ardb.conf";
    if (!parse_conf_file(configfile, props, " "))
    {
        printf("Error: Failed to parse conf file:%s\n", configfile);
        return -1;
    }
    ArdbConfig cfg;
    if (!cfg.Parse(props))
    {
        printf("Failed to parse config file.\n");
        return -1;
    }
    g_config = &cfg;
    Ardb db;
    if (db.Init() != 0)
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
            Context tmpctx;
            interpreter.EvalFile(tmpctx, command_test_path + fs[i]);
            RedisReply& r = tmpctx.GetReply();
            if(r.IsErr())
            {
                fprintf(stderr, "%s %s\n", fs[i].c_str(), r.Error().c_str());
            }
        }
    }
    return 0;
}

