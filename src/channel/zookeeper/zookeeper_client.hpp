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

/*
 * zookeeper_client.hpp
 *
 *  Created on: 2013-9-22
 *      Author: wqy
 */

#ifndef ZOOKEEPER_CLIENT_HPP_
#define ZOOKEEPER_CLIENT_HPP_

#include "zookeeper.h"
#include "channel/all_includes.hpp"

namespace ardb
{
	typedef std::vector<std::string> ZKServerList;

	class ZookeeperClient;
	struct ZKAsyncCallback
	{
			virtual void OnWatch(int type, int state, const char *path) = 0;
			virtual void OnStat(const std::string& path, const struct Stat* stat, int rc) = 0;
			virtual void OnCreated(const std::string& path, int rc) = 0;
			virtual void OnAuth(const std::string& scheme,const std::string& cert, int rc) = 0;
			virtual ~ZKAsyncCallback()
			{
			}
	};

	struct ZKOptions
	{
			clientid_t clientid;
			int recv_timeout;
			ZKAsyncCallback* cb;
			ZKOptions() :
					recv_timeout(10000), cb(NULL)
			{
				memset(&clientid, 0, sizeof(clientid));
			}
	};

	struct ZKACL
	{
			int perm;
			std::string scheme;
			std::string id;
	};
	typedef std::vector<ZKACL> ZKACLArray;

	class ZookeeperClient:public Runnable
	{
		private:
			ChannelService& m_serv;
			zhandle_t* m_zk;
			ZKAsyncCallback* m_callback;
			int m_zk_fd;
			void Run();
			static void WatchCallback(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx);
			static void CreateCB(int rc, const char *value, const void *data);
			static void RecursiveCreateCB(int rc, const char *value, const void *data);
			static void ExistsCompletionCB(int rc, const struct Stat *stat, const void *data);
		    static void AuthCB(int rc, const void *data);
		    static void ZKIOCallback(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
		    void CheckConn();
		public:
			ZookeeperClient(ChannelService& serv);
			int Connect(const std::string& servers, const ZKOptions& options);
			int Exists(const std::string& path, bool watch);
			int Create(const std::string& path, const std::string& value, const ZKACLArray& acls, int flags, bool recursive = true);
			int Auth(const std::string& scheme,const std::string& cert);
	        int GetClientID(clientid_t& id);
	        int Lock(const std::string& path);
	        int UnLock(const std::string& path);
	        void Close();
	        ~ZookeeperClient();
	};
}

#endif /* ZOOKEEPER_CLIENT_HPP_ */
