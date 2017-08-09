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
#include "util/file_helper.hpp"
#include "util/string_helper.hpp"
#include "util/system_helper.hpp"
#include <errno.h>

#define ARDB_AUTHPASS_MAX_LEN 512
/* Static server configuration */
#define CONFIG_DEFAULT_HZ        10      /* Time interrupt calls/sec. */
#define CONFIG_MIN_HZ            1
#define CONFIG_MAX_HZ            500
#define DEFAULT_STAT_LOG_PERIOD_SECS   600

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
        conf_props = props;
        conf_get_string(props, "home", home);
        if (home.empty())
        {
            home = "../ardb";
        }
        make_dir(home);
        int err = real_path(home, home);
        if (0 != err)
        {
            ERROR_LOG("Invalid 'home' config:%s for reason:%s", home.c_str(), strerror(err));
            return false;
        }
        err = access(home.c_str(), R_OK | W_OK);
        if (0 != err)
        {
            err = errno;
            ERROR_LOG("Invalid 'home' config:%s for reason:%s", home.c_str(), strerror(err));
            return false;
        }

        setenv("ARDB_HOME", home.c_str(), 1);
        replace_env_var(const_cast<Properties&>(props));

        conf_get_string(props, "pidfile", pidfile);
        conf_get_int64(props, "thread-pool-size", thread_pool_size);
        if (thread_pool_size <= 0)
        {
            thread_pool_size = available_processors();
        }
        conf_get_int64(props, "hz", hz);
        if (hz < CONFIG_MIN_HZ)
            hz = CONFIG_MIN_HZ;
        if (hz > CONFIG_MAX_HZ)
            hz = CONFIG_MAX_HZ;
        conf_get_int64(props, "tcp-keepalive", tcp_keepalive);
        conf_get_int64(props, "timeout", timeout);
        //conf_get_int64(props, "unixsocketperm", unixsocketperm);
        conf_get_int64(props, "slowlog-log-slower-than", slowlog_log_slower_than);
        conf_get_int64(props, "slowlog-max-len", slowlog_max_len);
        conf_get_int64(props, "maxclients", max_clients);
        if(max_clients <= 0)
        {
            max_clients = 10000;
        }

        for (int i = 0;; i++)
        {
            char config_key[256];
            sprintf(config_key, "server[%d].listen", i);
            ListenPoint lp;
            std::string address;
            if (!conf_get_string(props, config_key, address))
            {
                break;
            }
            if (address.find(":") == std::string::npos)
            {
                lp.host = address;
            }
            else
            {
                std::vector<std::string> ss = split_string(address, ":");
                uint32 port;
                if (ss.size() < 2 || !string_touint32(ss[ss.size() - 1], port) || port > 65535)
                {
                    ERROR_LOG("Invalid listen address %s", address.c_str());
                    return false;
                }

                lp.host = address.substr(0, address.size() - ss[ss.size() - 1].size() - 1);
                lp.port = port;
            }
            sprintf(config_key, "server[%d].qps-limit", i);
            conf_get_int64(props, config_key, lp.qps_limit);
            sprintf(config_key, "server[%d].unixsocketperm", i);
            conf_get_int64(props, config_key, lp.qps_limit);
            servers.push_back(lp);
        }

        if (servers.empty())
        {
            ListenPoint lp;
            lp.host = "0.0.0.0";
            lp.port = 16379;
            servers.push_back(lp);
        }
        if (strcasecmp(engine.c_str(), "rocksdb") == 0)
        {
            conf_get_string(props, "rocksdb.compaction", rocksdb_compaction);
            conf_get_bool(props, "rocksdb.disableWAL", rocksdb_disablewal);
            conf_get_bool(props, "rocksdb.scan-total-order", rocksdb_scan_total_order);
        }

        conf_get_string(props, "engine", engine);
        conf_get_string(props, "data-dir", data_base_path);
        conf_get_string(props, "backup-dir", backup_dir);
        conf_get_string(props, "repl-dir", repl_data_dir);
        make_dir(repl_data_dir);
        make_dir(backup_dir);

        err = real_path(repl_data_dir, repl_data_dir);
        if (0 != err)
        {
            ERROR_LOG("Invalid 'repl-dir' config:%s for reason:%s", repl_data_dir.c_str(), strerror(err));
            return false;
        }

        std::string backup_file_format;
        conf_get_string(props, "backup-file-format", backup_file_format);
        if (!strcasecmp(backup_file_format.c_str(), "redis"))
        {
            backup_redis_format = true;
        }

        conf_get_string(props, "zookeeper-servers", zookeeper_servers);
        conf_get_string(props, "zk-clientid-file", zk_clientid_file);

        conf_get_string(props, "loglevel", loglevel);
        conf_get_string(props, "logfile", logfile);
        conf_get_bool(props, "daemonize", daemonize);

        conf_get_int64(props, "repl-backlog-size", repl_backlog_size);
        conf_get_int64(props, "repl-backlog-cache-size", repl_backlog_cache_size);
        conf_get_int64(props, "repl-ping-slave-period", repl_ping_slave_period);
        conf_get_int64(props, "repl-timeout", repl_timeout);
        conf_get_int64(props, "repl-backlog-sync-period", repl_backlog_sync_period);
        conf_get_int64(props, "repl-backlog-ttl", repl_backlog_time_limit);
        conf_get_int64(props, "min-slaves-to-write", repl_min_slaves_to_write);
        conf_get_int64(props, "min-slaves-max-lag", repl_min_slaves_max_lag);
        conf_get_bool(props, "slave-serve-stale-data", repl_serve_stale_data);
        conf_get_int64(props, "max-slave-worker-queue", max_slave_worker_queue);
        if(max_slave_worker_queue <= 0)
        {
            max_slave_worker_queue = 1024;
        }

        conf_get_bool(props, "repl-disable-tcp-nodelay", repl_disable_tcp_nodelay);
        conf_get_int64(props, "lua-time-limit", lua_time_limit);

        conf_get_int64(props, "snapshot-max-lag-offset", snapshot_max_lag_offset);
        conf_get_int64(props, "maxsnapshots", maxsnapshots);
        if(maxsnapshots < 1)
        {
            maxsnapshots = 1;
        }
        if (snapshot_max_lag_offset > repl_backlog_size / 2)
        {
            snapshot_max_lag_offset = repl_backlog_size / 2;
        }

        conf_get_int64(props, "hll-sparse-max-bytes", hll_sparse_max_bytes);

        conf_get_bool(props, "slave-read-only", slave_readonly);
        conf_get_bool(props, "slave-serve-stale-data", slave_serve_stale_data);
        conf_get_int64(props, "slave-priority", slave_priority);
        conf_get_bool(props, "slave-ignore-expire", slave_ignore_expire);
        conf_get_bool(props, "slave-ignore-del", slave_ignore_del);

        conf_get_bool(props, "slave-cleardb-before-fullresync", slave_cleardb_before_fullresync);

        conf_get_int64(props, "statistics-log-period", statistics_log_period);
        if (statistics_log_period <= 0)
        {
            statistics_log_period = DEFAULT_STAT_LOG_PERIOD_SECS;
        }

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
        
        conf_get_string(props, "masterauth", masterauth);

        if (data_base_path.empty())
        {
            data_base_path = ".";
        }
        make_dir(data_base_path);
        err = real_path(data_base_path, data_base_path);
        if (0 != err)
        {
            ERROR_LOG("Invalid 'data-dir' config:%s for reason:%s", data_base_path.c_str(), strerror(err));
            return false;
        }

        conf_get_string(props, "requirepass", requirepass);

        Properties::const_iterator fit = props.find("rename-command");
        if (fit != props.end())
        {
            rename_commands.clear();
            const ConfItemsArray& cs = fit->second;
            ConfItemsArray::const_iterator cit = cs.begin();
            while (cit != cs.end())
            {
                if (cit->size() != 2)
                {
                    ERROR_LOG("Invalid 'rename-command' config with %u args.", cit->size());
                }
                else
                {
                    rename_commands[cit->at(0)] = cit->at(1);
                }
                cit++;
            }
        }

        conf_get_int64(props, "reply-pool-size", reply_pool_size);

        conf_get_int64(props, "slave-client-output-buffer-limit", slave_client_output_buffer_limit);
        conf_get_int64(props, "pubsub-client-output-buffer-limit", pubsub_client_output_buffer_limit);

        conf_get_string(props, "redis-compatible-version", redis_compatible_version);

        conf_get_bool(props, "redis-compatible-mode", redis_compatible);
        conf_get_bool(props, "compact-after-snapshot-load", compact_after_snapshot_load);

        conf_get_int64(props, "qps-limit-per-host", qps_limit_per_host);
        conf_get_int64(props, "qps-limit-per-connection", qps_limit_per_connection);
        conf_get_int64(props, "range-delete-min-size", range_delete_min_size);

        //trusted_ip.clear();
        Properties::const_iterator ip_it = props.find("trusted-ip");
        if (ip_it != props.end())
        {
            const ConfItemsArray& cs = ip_it->second;
            for (uint32 i = 0; i < cs.size(); i++)
            {
                //trusted_ip.insert(cs[i][0]);
            }
        }

        if (!verify_config(*this))
        {
            return false;
        }

        ArdbLogger::SetLogLevel(loglevel);
        return true;
    }

    uint32 ArdbConfig::PrimaryPort() const
    {
        for (size_t i = 0; i < servers.size(); i++)
        {
            if (servers[i].port != 0)
            {
                return servers[i].port;
            }
        }
        return 0;
    }
OP_NAMESPACE_END

