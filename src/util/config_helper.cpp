/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
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

#include "util/config_helper.hpp"
#include "util/string_helper.hpp"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "logger.hpp"

namespace ardb
{
    static const uint32 kConfigLineMax = 1024;
    bool parse_conf_file(const std::string& path, Properties& result, const char* sep)
    {
        char buf[kConfigLineMax + 1];
        FILE *fp;
        if ((fp = fopen(path.c_str(), "r")) == NULL)
        {
            return false;
        }
        uint32 lineno = 1;
        while (fgets(buf, kConfigLineMax, fp) != NULL)
        {
            char* line = trim_str(buf, "\r\n\t ");
            if (line[0] == '#' || line[0] == '\0')
            {
                lineno++;
                continue;
            }
            std::vector<char*> sp_ret = split_str(line, sep);
            if (sp_ret.size() != 2)
            {
                ERROR_LOG("Invalid config line at line:%u", lineno);
                fclose(fp);
                return false;
            }
            char* key = trim_str(sp_ret[0], "\r\n\t ");
            char* value = trim_str(sp_ret[1], "\r\n\t ");
            if (result.find(key) != result.end())
            {
                ERROR_LOG("Duplicate key:%s in config.", key);
                fclose(fp);
                return false;
            }
            result[key] = value;
            lineno++;
        }
        fclose(fp);
        return true;
    }

    bool conf_get_int64(const Properties& conf, const std::string& name, int64& value, bool ignore_nonexist)
    {
        Properties::const_iterator found = conf.find(name);
        if (found == conf.end())
        {
            return ignore_nonexist;
        }
        if (string_toint64(found->second, value))
        {
            return true;
        }
        std::string size_str = string_toupper(found->second);
        value = atoll(size_str.c_str());
        if (size_str.find("M") == (size_str.size() - 1) || size_str.find("MB") == (size_str.size() - 2))
        {
            value *= (1024 * 1024); // convert to megabytes
        }
        else if (size_str.find("G") == (size_str.size() - 1) || size_str.find("GB") == (size_str.size() - 2))
        {
            value *= (1024 * 1024 * 1024); // convert to kilobytes
        }
        else if (size_str.find("K") == (size_str.size() - 1) || size_str.find("KB") == (size_str.size() - 2))
        {
            value *= 1024; // convert to kilobytes
        }
        else
        {
            return false;
        }
        return true;
    }
    bool conf_get_string(const Properties& conf, const std::string& name, std::string& value, bool ignore_nonexist)
    {
        Properties::const_iterator found = conf.find(name);
        if (found == conf.end())
        {
            return ignore_nonexist;
        }
        value = found->second;
        return true;
    }

    bool conf_get_bool(const Properties& conf, const std::string& name, bool& value, bool ignore_nonexist)
    {
        Properties::const_iterator found = conf.find(name);
        if (found == conf.end())
        {
            return ignore_nonexist;
        }
        if(!strcasecmp(found->second.c_str(), "true") || !strcasecmp(found->second.c_str(), "1"))
        {
            value = true;
        }else
        {
            value = false;
        }
        return true;
    }
    bool conf_get_double(const Properties& conf, const std::string& name, double& value, bool ignore_nonexist)
    {
        Properties::const_iterator found = conf.find(name);
        if (found == conf.end())
        {
            return ignore_nonexist;
        }
        return string_todouble(found->second, value);
    }

    bool parse_ini_conf_file(const std::string& path, INIProperties& result, const char* sep)
    {
        char buf[kConfigLineMax + 1];
        FILE *fp;
        if ((fp = fopen(path.c_str(), "r")) == NULL)
        {
            return false;
        }
        uint32 lineno = 1;
        std::string current_tag = "";
        while (fgets(buf, kConfigLineMax, fp) != NULL)
        {
            char* line = trim_str(buf, "\r\n\t ");
            if (line[0] == '#' || line[0] == '\0')
            {
                lineno++;
                continue;
            }
            if (line[0] == '[' && line[strlen(line) - 1] == ']')
            {
                current_tag = std::string(line + 1, strlen(line) - 2);
                lineno++;
                continue;
            }
            std::vector<char*> sp_ret = split_str(line, sep);
            if (sp_ret.size() != 2)
            {
                ERROR_LOG("Invalid config line at line:%u", lineno);
                fclose(fp);
                return false;
            }
            char* key = trim_str(sp_ret[0], "\r\n\t ");
            char* value = trim_str(sp_ret[1], "\r\n\t ");
            Properties& current_prop = result[current_tag];
            if (current_prop.find(key) != current_prop.end())
            {
                ERROR_LOG("Duplicate key:%s in config.", key);
                fclose(fp);
                return false;
            }
            current_prop[key] = value;
            lineno++;
        }
        fclose(fp);
        return true;
    }

    void replace_env_var(Properties& props)
    {
        Properties::iterator it = props.begin();
        while (it != props.end())
        {
            std::string& prop_value = it->second;
            while (true)
            {
                size_t pos = prop_value.find("${");
                if (pos != std::string::npos)
                {
                    size_t next_pos = prop_value.find('}', pos + 1);
                    if (next_pos != std::string::npos)
                    {
                        std::string env_key = prop_value.substr(pos + 2, next_pos - pos - 2);
                        char* env_value = getenv(env_key.c_str());
                        if (NULL != env_value)
                        {
                            prop_value.replace(pos, next_pos - pos + 1, env_value);
                            // this prop value may contain multiple env var
                            continue;
                        }
                    }
                }
                break;
            }
            it++;
        }
    }

    void replace_env_var(INIProperties& props)
    {
        INIProperties::iterator it = props.begin();
        while (it != props.end())
        {
            Properties& sub_props = it->second;
            replace_env_var(sub_props);
            it++;
        }
    }
}
