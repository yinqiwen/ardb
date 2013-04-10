/*
 * main.cpp
 *
 *  Created on: 2013-4-10
 *      Author: wqy
 */

#include "ardb_server.hpp"

int main(int argc, char** argv)
{
	ArdbServer server;
	ArdbServerConfig cfg;
	server.Start(cfg);
	return 0;
}

