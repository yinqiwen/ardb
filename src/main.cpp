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

#include "ardb_server.hpp"
#ifdef __USE_KYOTOCABINET__
#include "engine/kyotocabinet_engine.hpp"
typedef ardb::KCDBEngineFactory SelectedDBEngineFactory;
#elif defined __USE_LMDB__
#include "engine/lmdb_engine.hpp"
typedef ardb::LMDBEngineFactory SelectedDBEngineFactory;
#elif defined __USE_ROCKSDB__
#include "engine/rocksdb_engine.hpp"
typedef ardb::RocksDBEngineFactory SelectedDBEngineFactory;
#else
#include "engine/leveldb_engine.hpp"
typedef ardb::LevelDBEngineFactory SelectedDBEngineFactory;
#endif

#include <signal.h>

void version()
{
    printf("Ardb server v=%s bits=%d \n", ARDB_VERSION,
                    sizeof(long) == 4 ? 32 : 64);
    exit(0);
}

void usage()
{
    fprintf(stderr, "Usage: ./ardb-server [/path/to/ardb.conf] [options]\n");
    fprintf(stderr, "       ./ardb-server -v or --version\n");
    fprintf(stderr, "       ./ardb-server -h or --help\n");
    fprintf(stderr, "Examples:\n");
    fprintf(stderr,
                    "       ./ardb-server (run the server with default conf)\n");
    fprintf(stderr, "       ./ardb-server /etc/ardb/16379.conf\n");
    fprintf(stderr, "       ./ardb-server /etc/myardb.conf \n\n");
    exit(1);
}

int signal_setting()
{
    signal(SIGPIPE, SIG_IGN);
    return 0;
}

int main(int argc, char** argv)
{
    Properties props;
    if (argc >= 2)
    {
        int j = 1; /* First option to parse in argv[] */
        char *configfile = NULL;

        /* Handle special options --help and --version */
        if (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "--version") == 0) version();
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0) usage();

        /* First argument is the config file name? */
        if (argv[j][0] != '-' || argv[j][1] != '-')
        {
            configfile = argv[j++];
            if (!parse_conf_file(configfile, props, " "))
            {
                printf("Error: Failed to parse conf file:%s\n", configfile);
                return -1;
            }
        }
    }
    else
    {
        printf(
                        "Warning: no config file specified, using the default config. In order to specify a config file use %s /path/to/ardb.conf\n",
                        argv[0]);
    }
    signal_setting();
    ArdbServerConfig cfg;
    if (0 != ArdbServer::ParseConfig(props, cfg))
    {
        return -1;
    }
    SelectedDBEngineFactory engine(props);
    ArdbServer server(engine);
    server.Start(props);
    return 0;
}

