/*
 * config_helper.hpp
 *
 *  Created on: 2011-3-15
 *      Author: qiyingwang
 */

#ifndef CONFIG_HELPER_HPP_
#define CONFIG_HELPER_HPP_
#include "common.hpp"
#include <string>
#include <map>

namespace ardb
{

	typedef std::map<std::string, std::string> Properties;
	typedef std::map<std::string, Properties> INIProperties;
	bool parse_conf_file(const std::string& path, Properties& result,
			const char* sep = "=");
	bool conf_get_int64(const Properties& conf, const std::string& name, int64& value, bool ignore_nonexist = false);
	bool conf_get_string(const Properties& conf, const std::string& name, std::string& value, bool ignore_nonexist = false);
	bool conf_get_double(const Properties& conf, const std::string& name, double& value, bool ignore_nonexist = false);

	void replace_env_var(Properties& props);
	void replace_env_var(INIProperties& props);

	bool parse_ini_conf_file(const std::string& path, INIProperties& result,
			const char* sep = "=");
}

#endif /* CONFIG_HELPER_HPP_ */
