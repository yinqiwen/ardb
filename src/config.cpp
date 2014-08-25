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
#include "config.hpp"
#include "util/config_helper.hpp"

#define ARDB_AUTHPASS_MAX_LEN 512

OP_NAMESPACE_BEGIN

    static bool verify_config(const ArdbConfig& cfg)
    {
        if (!cfg.master_host.empty() && cfg.repl_backlog_size <= 0)
        {
            ERROR_LOG("[Config]Invalid value for 'slaveof' since 'repl-backlog-size' is not set correctly.");
            return false;
        }
        if (cfg.requirepass.size() > ARDB_AUTHPASS_MAX_LEN)
        {
            ERROR_LOG("[Config]Password is longer than %u", ARDB_AUTHPASS_MAX_LEN);
            return false;
        }
        return true;
    }
    bool ArdbConfig::Parse(const Properties& props)
    {
        conf_get_string(props, "home", home);
        if (home.empty())
        {
            home = "../ardb";
        }
        make_dir(home);
        home = real_path(home);

        setenv("ARDB_HOME", home.c_str(), 1);
        replace_env_var(const_cast<Properties&>(props));

        conf_get_string(props, "pidfile", pidfile);

        conf_get_int64(props, "tcp-keepalive", tcp_keepalive);
        conf_get_int64(props, "timeout", timeout);
        conf_get_int64(props, "unixsocketperm", unixsocketperm);
        conf_get_int64(props, "slowlog-log-slower-than", slowlog_log_slower_than);
        conf_get_int64(props, "slowlog-max-len", slowlog_max_len);
        conf_get_int64(props, "maxclients", max_clients);
        Properties::const_iterator listen_it = props.find("listen");
        if (listen_it != props.end())
        {
            const ConfItemsArray& cs = listen_it->second;
            for (uint32 i = 0; i < cs.size(); i++)
            {
                if (cs[i].size() != 1)
                {
                    WARN_LOG("Invalid config 'listen'");
                }
                else
                {
                    const std::string& str = cs[i][0];
                    listen_addresses.push_back(str);
                }
            }
        }
        if (listen_addresses.empty())
        {
            listen_addresses.push_back("0.0.0.0:16379");
        }
        Properties::const_iterator tp_it = props.find("thread-pool-size");
        if (tp_it != props.end())
        {
            const ConfItemsArray& cs = tp_it->second;
            for (uint32 i = 0; i < cs.size(); i++)
            {
                uint32 size = 0;
                if (cs[i].size() != 1 || !string_touint32(cs[i][0], size))
                {
                    WARN_LOG("Invalid config 'thread-pool-size'");
                }
                else
                {
                    thread_pool_sizes.push_back((int64) size);
                }
            }
        }
        Properties::const_iterator qp_it = props.find("qps-limit");
        if (qp_it != props.end())
        {
            const ConfItemsArray& cs = qp_it->second;
            for (uint32 i = 0; i < cs.size(); i++)
            {
                uint32 limit = 0;
                if (cs[i].size() != 1 || !string_touint32(cs[i][0], limit))
                {
                    WARN_LOG("Invalid config 'qps-limit'");
                }
                else
                {
                    qps_limits.push_back((int64) limit);
                }
            }
        }
        thread_pool_sizes.resize(listen_addresses.size());
        qps_limits.resize(listen_addresses.size());

        conf_get_string(props, "data-dir", data_base_path);
        conf_get_string(props, "backup-dir", backup_dir);
        conf_get_string(props, "repl-dir", repl_data_dir);
        make_dir(repl_data_dir);
        make_dir(backup_dir);
        repl_data_dir = real_path(repl_data_dir);

        std::string backup_file_format;
        conf_get_string(props, "backup-file-format", backup_file_format);
        if (!strcasecmp(backup_file_format.c_str(), "redis"))
        {
            backup_redis_format = true;
        }

        conf_get_string(props, "zookeeper-servers", zookeeper_servers);

        conf_get_string(props, "loglevel", loglevel);
        conf_get_string(props, "logfile", logfile);
        conf_get_bool(props, "daemonize", daemonize);

        conf_get_int64(props, "repl-backlog-size", repl_backlog_size);
        conf_get_int64(props, "repl-ping-slave-period", repl_ping_slave_period);
        conf_get_int64(props, "repl-timeout", repl_timeout);
        conf_get_int64(props, "repl-state-persist-period", repl_state_persist_period);
        conf_get_int64(props, "repl-backlog-ttl", repl_backlog_time_limit);
        conf_get_int64(props, "lua-time-limit", lua_time_limit);

        conf_get_int64(props, "hash-max-ziplist-entries", hash_max_ziplist_entries);
        conf_get_int64(props, "hash_max-ziplist-value", hash_max_ziplist_value);
        conf_get_int64(props, "set-max-ziplist-entries", set_max_ziplist_entries);
        conf_get_int64(props, "set-max-ziplist-value", set_max_ziplist_value);
        conf_get_int64(props, "list-max-ziplist-entries", list_max_ziplist_entries);
        conf_get_int64(props, "list-max-ziplist-value", list_max_ziplist_value);
        conf_get_int64(props, "zset-max-ziplist-entries", zset_max_ziplist_entries);
        conf_get_int64(props, "zset_max_ziplist_value", zset_max_ziplist_value);

        conf_get_int64(props, "L1-zset-max-cache-size", L1_zset_max_cache_size);
        conf_get_int64(props, "L1-set-max-cache-size", L1_set_max_cache_size);
        conf_get_int64(props, "L1-hash-max-cache-size", L1_hash_max_cache_size);
        conf_get_int64(props, "L1-list-max-cache-size", L1_list_max_cache_size);
        conf_get_int64(props, "L1-string-max-cache-size", L1_string_max_cache_size);

        conf_get_bool(props, "L1-zset-read-fill-cache", L1_zset_read_fill_cache);
        conf_get_bool(props, "L1-zset-seek-load-cache", L1_zset_seek_load_cache);
        conf_get_bool(props, "L1-set-read-fill-cache", L1_set_read_fill_cache);
        conf_get_bool(props, "L1-set-seek-load-cache", L1_set_seek_load_cache);
        conf_get_bool(props, "L1-hash-read-fill-cache", L1_hash_read_fill_cache);
        conf_get_bool(props, "L1-hash-seek-load-cache", L1_hash_seek_load_cache);
        conf_get_bool(props, "L1-list-read-fill-cache", L1_list_read_fill_cache);
        conf_get_bool(props, "L1-list-seek-load-cache", L1_list_seek_load_cache);
        conf_get_bool(props, "L1-string-read-fill-cache", L1_string_read_fill_cache);

        conf_get_int64(props, "hll-sparse-max-bytes", hll_sparse_max_bytes);

        conf_get_bool(props, "slave-read-only", slave_readonly);
        conf_get_bool(props, "slave-serve-stale-data", slave_serve_stale_data);
        conf_get_int64(props, "slave-priority", slave_priority);

        std::string slaveof;
        if (conf_get_string(props, "slaveof", slaveof))
        {
            std::vector<std::string> ss = split_string(slaveof, ":");
            if (ss.size() == 2)
            {
                master_host = ss[0];
                if (!string_touint32(ss[1], master_port))
                {
                    master_host = "";
                    WARN_LOG("Invalid 'slaveof' config.");
                }
            }
            else
            {
                WARN_LOG("Invalid 'slaveof' config.");
            }
        }

        std::string include_dbs, exclude_dbs;
        repl_includes.clear();
        repl_excludes.clear();
        conf_get_string(props, "replicate-include-db", include_dbs);
        conf_get_string(props, "replicate-exclude-db", exclude_dbs);
        if (0 != split_uint32_array(include_dbs, "|", repl_includes))
        {
            ERROR_LOG("Invalid 'replicate-include-db' config.");
            repl_includes.clear();
        }
        if (0 != split_uint32_array(exclude_dbs, "|", repl_excludes))
        {
            ERROR_LOG("Invalid 'replicate-exclude-db' config.");
            repl_excludes.clear();
        }

        if (data_base_path.empty())
        {
            data_base_path = ".";
        }
        make_dir(data_base_path);
        data_base_path = real_path(data_base_path);

        conf_get_string(props, "additional-misc-info", additional_misc_info);

        conf_get_string(props, "requirepass", requirepass);

        Properties::const_iterator fit = props.find("rename-command");
        if (fit != props.end())
        {
            rename_commands.clear();
            StringSet newcmdset;
            const ConfItemsArray& cs = fit->second;
            ConfItemsArray::const_iterator cit = cs.begin();
            while (cit != cs.end())
            {
                if (cit->size() != 2 || newcmdset.count(cit->at(1)) > 0)
                {
                    ERROR_LOG("Invalid 'rename-command' config.");
                }
                else
                {
                    rename_commands[cit->at(0)] = cit->at(1);
                    newcmdset.insert(cit->at(1));
                }
                cit++;
            }
        }

        conf_get_int64(props, "compact-min-interval", compact_min_interval);
        conf_get_int64(props, "compact-max-interval", compact_max_interval);
        conf_get_int64(props, "compact-after-write", compact_trigger_write_count);
        conf_get_bool(props, "compact-enable", compact_enable);

        conf_get_int64(props, "reply-pool-size", reply_pool_size);
        conf_get_bool(props, "replace-all-for-multi-sadd", replace_for_multi_sadd);
        conf_get_bool(props, "replace-all-for-hmset", replace_for_hmset);

        conf_get_int64(props, "slave-client-output-buffer-limit", slave_client_output_buffer_limit);
        conf_get_int64(props, "pubsub-client-output-buffer-limit", pubsub_client_output_buffer_limit);

        trusted_ip.clear();
        Properties::const_iterator ip_it = props.find("trusted-ip");
        if (ip_it != props.end())
        {
            const ConfItemsArray& cs = ip_it->second;
            for (uint32 i = 0; i < cs.size(); i++)
            {
                trusted_ip.insert(cs[i][0]);
            }
        }
        if (!verify_config(*this))
        {
            return false;
        }

        ArdbLogger::SetLogLevel(loglevel);
        return true;
    }

    uint32 ArdbConfig::PrimayPort()
    {
        return primary_port;
    }
OP_NAMESPACE_END

