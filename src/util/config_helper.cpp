/*
 * config_helper.cpp
 *
 *  Created on: 2011-3-15
 *      Author: qiyingwang
 */
#include "util/config_helper.hpp"
#include "util/string_helper.hpp"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "logger.hpp"

namespace rddb
{
	static const uint32 kConfigLineMax = 1024;
	bool parse_conf_file(const string& path, Properties& result,
			const char* sep)
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

	bool parse_ini_conf_file(const string& path, INIProperties& result,
			const char* sep)
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
						std::string env_key = prop_value.substr(pos + 2,
								next_pos - pos - 2);
						char* env_value = getenv(env_key.c_str());
						if (NULL != env_value)
						{
							prop_value.replace(pos, next_pos - pos + 1,
									env_value);
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
