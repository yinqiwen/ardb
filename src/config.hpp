/*
 *Copyright (c) 2013-2015, yinqiwen <yinqiwen@gmail.com>
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
#include "thread/spin_rwlock.hpp"
#include "types.hpp"

OP_NAMESPACE_BEGIN
    struct ListenPoint
    {
            std::string address;
            int64 thread_pool_size;
            int64 qps_limit;
            int64 unixsocketperm;
            ListenPoint() :
                    thread_pool_size(1), qps_limit(0), unixsocketperm(755)
            {
            }
            int64 GetThreadPoolSize() const;
    };
    typedef std::vector<ListenPoint> ListenPointArray;
    struct ArdbConfig
    {
            SpinRWLock lock;
            bool daemonize;

            ListenPointArray servers;

            int64 hz;
            //int64 unixsocketperm;
            int64 max_open_files;
            int64 tcp_keepalive;
            int64 timeout;
            std::string engine;
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
            int64 repl_backlog_cache_size;
            int64 repl_backlog_sync_period;
            int64 repl_backlog_time_limit;
            bool slave_cleardb_before_fullresync;
            bool slave_readonly;
            bool slave_serve_stale_data;
            int64 slave_priority;

            StringTreeSet trusted_ip;

            int64 lua_time_limit;

            std::string master_host;
            uint32 master_port;

            StringStringMap rename_commands;

            std::string loglevel;
            std::string logfile;

            std::string pidfile;

            std::string zookeeper_servers;

            std::string requirepass;

            int64 hll_sparse_max_bytes;

            int64 reply_pool_size;

            uint32 primary_port;

            int64 slave_client_output_buffer_limit;
            int64 pubsub_client_output_buffer_limit;

            bool slave_ignore_expire;
            bool slave_ignore_del;
            bool repl_disable_tcp_nodelay;

            bool scan_redis_compatible;
            int64 scan_cursor_expire_after;

            int64 snapshot_max_lag;

            std::string conf_path;
            Properties conf_props;

            bool lua_exec_atomic;
            bool redis_compatible;

            std::string masterauth;

            std::string rocksdb_options;

            ArdbConfig() :
                    daemonize(false), hz(10), max_open_files(100000), tcp_keepalive(0), timeout(0), engine("rocksdb"), slowlog_log_slower_than(10000), slowlog_max_len(
                            128), repl_data_dir("./repl"), backup_dir("./backup"), backup_redis_format(false), repl_ping_slave_period(10), repl_timeout(60), repl_backlog_size(
                            100 * 1024 * 1024), repl_backlog_cache_size(100 * 1024 * 1024), repl_backlog_sync_period(1), repl_backlog_time_limit(3600), slave_cleardb_before_fullresync(
                            true), slave_readonly(true), slave_serve_stale_data(true), slave_priority(100), lua_time_limit(0), master_port(0), loglevel("INFO"), hll_sparse_max_bytes(
                            3000), reply_pool_size(5000), primary_port(0), slave_client_output_buffer_limit(256 * 1024 * 1024), pubsub_client_output_buffer_limit(
                            32 * 1024 * 1024), slave_ignore_expire(false), slave_ignore_del(false), repl_disable_tcp_nodelay(false), scan_redis_compatible(
                            true), scan_cursor_expire_after(60), snapshot_max_lag(500 * 1024 * 1024), lua_exec_atomic(true), redis_compatible(true)
            {
            }
            bool Parse(const Properties& props);
            uint32 PrimayPort();
    };

OP_NAMESPACE_END

#endif /* CONFIG_HPP_ */
