/*
 * main.cpp
 *
 *  Created on: 2013-4-10
 *      Author: wqy
 */

#include "ardb_server.hpp"
#include <signal.h>

void version()
{
	printf("Ardb server v=%s bits=%d \n", ARDB_VERSION,
			sizeof(long) == 4 ? 32 : 64);
	exit(0);
}

void usage()
{
	fprintf(stderr, "Usage: ./ardb-server [/path/to/redis.conf] [options]\n");
	fprintf(stderr, "       ./ardb-server -v or --version\n");
	fprintf(stderr, "       ./ardb-server -h or --help\n");
	fprintf(stderr, "Examples:\n");
	fprintf(stderr,
			"       ./ardb-server (run the server with default conf)\n");
	fprintf(stderr, "       ./ardb-server /etc/ardb/16379.conf\n");
	fprintf(stderr, "       ./ardb-server /etc/myredis.conf \n\n");
	exit(1);
}

int signal_setting()
{
	signal(SIGPIPE, SIG_IGN);
}

int main(int argc, char** argv)
{
	Properties props;
	if (argc >= 2)
	{
		int j = 1; /* First option to parse in argv[] */
		char *configfile = NULL;

		/* Handle special options --help and --version */
		if (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "--version") == 0)
			version();
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0)
			usage();

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
	} else
	{
		printf(
				"Warning: no config file specified, using the default config. In order to specify a config file use %s /path/to/ardb.conf\n",
				argv[0]);
	}
	signal_setting();
	ArdbServer server;
	server.Start(props);
	return 0;
}

