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
using std::map;
using std::string;

namespace rddb
{

	typedef std::map<std::string, std::string> Properties;
	typedef std::map<std::string, Properties> INIProperties;
	bool parse_conf_file(const string& path, Properties& result,
			const char* sep = "=");

	void replace_env_var(Properties& props);
	void replace_env_var(INIProperties& props);

	bool parse_ini_conf_file(const string& path, INIProperties& result,
			const char* sep = "=");
}

#endif /* CONFIG_HELPER_HPP_ */
