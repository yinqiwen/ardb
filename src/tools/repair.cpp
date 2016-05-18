/*
 *Copyright (c) 2013-2016, yinqiwen <yinqiwen@gmail.com>
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
#include <signal.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "network.hpp"
#include "db/db.hpp"
#include "util/file_helper.hpp"

void version()
{
    printf("Ardb repair v=%s bits=%d engine=%s \n", ARDB_VERSION, sizeof(long) == 4 ? 32 : 64, g_engine_name);
    exit(0);
}

void usage()
{
    fprintf(stderr, "Usage: ./ardb-repair [db_dir]\n");
    fprintf(stderr, "       ./ardb-repair -v or --version\n");
    fprintf(stderr, "       ./ardb-repair -h or --help\n");
    fprintf(stderr, "Examples:\n");
    fprintf(stderr, "       ./ardb-repair ./data/rocksdb\n");
    exit(1);
}

int main(int argc, char** argv)
{
    std::string dir;
    if (argc >= 2)
    {
        int j = 1; /* First option to parse in argv[] */
        char *dirfile = NULL;

        /* Handle special options --help and --version */
        if (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "--version") == 0)
            version();
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0)
            usage();

        /* First argument is the config file name? */
        if (argv[j][0] != '-' || argv[j][1] != '-')
        {
            dirfile = argv[j++];
            dir = dirfile;
        }
    }
    else
    {
        printf("Warning: no database dir specified to repair.\n");
        return -1;
    }

    Ardb db;
    return db.Repair(dir);
}




