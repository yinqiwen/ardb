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

#ifndef CONFIG_HELPER_HPP_
#define CONFIG_HELPER_HPP_
#include "common.hpp"
#include <string>
#include <map>
#include <vector>

namespace ardb
{
    typedef std::vector<std::string>  ConfItems;
    typedef std::vector<ConfItems> ConfItemsArray;
	typedef std::map<std::string, ConfItemsArray> Properties;
	typedef std::map<std::string, Properties> INIProperties;
	bool parse_conf_file(const std::string& path, Properties& result,
			const char* sep = "=");
	bool conf_get_int64(const Properties& conf, const std::string& name, int64& value, bool ignore_nonexist = false);
	bool conf_get_size(const Properties& conf, const std::string& name, size_t& value, bool ignore_nonexist = false);
	bool conf_get_uint64(const Properties& conf, const std::string& name, uint64& value, bool ignore_nonexist = false);
	bool conf_get_uint16(const Properties& conf, const std::string& name, uint16& value, bool ignore_nonexist = false);
	bool conf_get_uint32(const Properties& conf, const std::string& name, uint32& value, bool ignore_nonexist = false);
	bool conf_get_uint8(const Properties& conf, const std::string& name, uint8& value, bool ignore_nonexist = false);
	bool conf_get_string(const Properties& conf, const std::string& name, std::string& value, bool ignore_nonexist = false);
	bool conf_get_bool(const Properties& conf, const std::string& name, bool& value, bool ignore_nonexist = false);
	bool conf_get_double(const Properties& conf, const std::string& name, double& value, bool ignore_nonexist = false);

	void conf_set(Properties& conf, const std::string& name, const std::string& value, bool replace = true);
	void conf_del(Properties& conf, const std::string& name, const std::string& value);

	void replace_env_var(Properties& props);
	void replace_env_var(INIProperties& props);

	bool parse_ini_conf_file(const std::string& path, INIProperties& result,
			const char* sep = "=");

	bool rewrite_conf_file(const std::string& file, const Properties& conf, const char* sep);

	bool parse_conf_content(const std::string& content, Properties& result,
	            const char* item_sep = ",", const char* key_value_seq = "=");
}

#endif /* CONFIG_HELPER_HPP_ */
