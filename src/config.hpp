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

#ifndef CONFIG_HPP_
#define CONFIG_HPP_

#include "common/common.hpp"
#include "util/config_helper.hpp"
#include "options.hpp"
#include "codec.hpp"

OP_NAMESPACE_BEGIN

    struct ArdbConfig
    {
            bool daemonize;
            StringArray listen_addresses;
            Int64Array thread_pool_sizes;
            Int64Array qps_limits;

            int64 unixsocketperm;
            int64 max_clients;
            int64 tcp_keepalive;
            int64 timeout;
            std::string home;
            std::string data_base_path;
            int64 slowlog_log_slower_than;
            int64 slowlog_max_len;

            std::string repl_data_dir;
            std::string backup_dir;
            bool backup_redis_format;

            int64 repl_ping_slave_period;
            int64 repl_timeout;
            int64 repl_backlog_size;
            int64 repl_state_persist_period;
            int64 repl_backlog_time_limit;
            bool slave_cleardb_before_fullresync;
            bool slave_readonly;
            bool slave_serve_stale_data;
            int64 slave_priority;

            int64 lua_time_limit;

            std::string master_host;
            uint32 master_port;

            DBIDArray repl_includes;
            DBIDArray repl_excludes;

            //int64 worker_count;
            std::string loglevel;
            std::string logfile;

            std::string pidfile;

            std::string zookeeper_servers;

            std::string additional_misc_info;

            std::string requirepass;

            StringStringMap rename_commands;

            int64 hash_max_ziplist_entries;
            int64 hash_max_ziplist_value;
            int64 list_max_ziplist_entries;
            int64 list_max_ziplist_value;
            int64 zset_max_ziplist_entries;
            int64 zset_max_ziplist_value;
            int64 set_max_ziplist_entries;
            int64 set_max_ziplist_value;

            int64 L1_zset_max_cache_size;
            int64 L1_set_max_cache_size;
            int64 L1_list_max_cache_size;
            int64 L1_hash_max_cache_size;
            int64 L1_string_max_cache_size;

            bool L1_zset_read_fill_cache;
            bool L1_zset_seek_load_cache;
            bool L1_set_read_fill_cache;
            bool L1_set_seek_load_cache;
            bool L1_hash_read_fill_cache;
            bool L1_hash_seek_load_cache;
            bool L1_list_read_fill_cache;
            bool L1_list_seek_load_cache;
            bool L1_string_read_fill_cache;

            bool check_type_before_set_string;

            int64 hll_sparse_max_bytes;

            int64 compact_min_interval;
            int64 compact_max_interval;
            int64 compact_trigger_write_count;
            bool compact_enable;

            bool replace_for_multi_sadd;
            bool replace_for_hmset;

            StringSet trusted_ip;
            int64 reply_pool_size;

            uint32 primary_port;

            int64 slave_client_output_buffer_limit;
            int64 pubsub_client_output_buffer_limit;

            bool slave_ignore_expire;
            bool slave_ignore_del;
            bool repl_disable_tcp_nodelay;

            bool scan_redis_compatible;
            int64 scan_cursor_expire_after;

            std::string conf_path;
            Properties conf_props;

            ArdbConfig() :
                    daemonize(false), unixsocketperm(755), max_clients(10000), tcp_keepalive(0), timeout(0), slowlog_log_slower_than(
                            10000), slowlog_max_len(128), repl_data_dir("./repl"), backup_dir("./backup"), backup_redis_format(
                            false), repl_ping_slave_period(10), repl_timeout(60), repl_backlog_size(100 * 1024 * 1024), repl_state_persist_period(
                            1), repl_backlog_time_limit(3600), slave_cleardb_before_fullresync(true), slave_readonly(
                            true), slave_serve_stale_data(true), slave_priority(100), lua_time_limit(0), master_port(0), loglevel(
                            "INFO"), hash_max_ziplist_entries(128), hash_max_ziplist_value(256), list_max_ziplist_entries(
                            128), list_max_ziplist_value(256), zset_max_ziplist_entries(128), zset_max_ziplist_value(256), set_max_ziplist_entries(
                            128), set_max_ziplist_value(256), L1_zset_max_cache_size(0), L1_set_max_cache_size(0), L1_list_max_cache_size(
                            0), L1_hash_max_cache_size(0), L1_string_max_cache_size(0), L1_zset_read_fill_cache(false), L1_zset_seek_load_cache(
                            false), L1_set_read_fill_cache(false), L1_set_seek_load_cache(false), L1_hash_read_fill_cache(
                            false), L1_hash_seek_load_cache(false), L1_list_read_fill_cache(false), L1_list_seek_load_cache(
                            false), L1_string_read_fill_cache(false), check_type_before_set_string(false), hll_sparse_max_bytes(
                            3000), compact_min_interval(1200), compact_max_interval(7200), compact_trigger_write_count(
                            10000), compact_enable(true), replace_for_multi_sadd(false), replace_for_hmset(false), reply_pool_size(
                            5000), primary_port(0), slave_client_output_buffer_limit(256 * 1024 * 1024), pubsub_client_output_buffer_limit(
                            32 * 1024 * 1024), slave_ignore_expire(false), slave_ignore_del(false), repl_disable_tcp_nodelay(
                            false), scan_redis_compatible(true), scan_cursor_expire_after(60)
            {
            }
            bool Parse(const Properties& props);
            uint32 PrimayPort();

    };

OP_NAMESPACE_END

#endif /* CONFIG_HPP_ */
