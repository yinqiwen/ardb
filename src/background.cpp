/*
 *Copyright (c) 2013-2018, yinqiwen <yinqiwen@gmail.com>
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
#include "network.hpp"
#include "db/db.hpp"

OP_NAMESPACE_BEGIN

    class BackGroundThread: public Thread
    {
        private:
	        typedef std::deque<KeyPrefix> KeyQueue;
	        KeyQueue async_delete_keys;
	        ThreadMutexLock async_delete_lock;
	        bool running;
	        void Run()
	        {
	        	Context dctx;
	        	while(running)
	        	{
	        		KeyQueue delete_ks;
	        		{
		        		LockGuard<ThreadMutexLock> guard(async_delete_lock);
		        		if(!async_delete_keys.empty())
		        		{
		        			delete_ks = async_delete_keys;
		        			async_delete_keys.clear();
		        		}
	        		}
	        		while(!delete_ks.empty())
	        		{
	        			KeyPrefix& k = delete_ks.front();
	        			KeyObject dk(k.ns, KEY_META, k.key);
	        			g_db->DelKey(dctx, dk);
	        			g_db->UnlockKey(k);
	        			if(!g_db->GetConf().master_host.empty())
	        			{
		        			std::string kstr;
		        			k.key.ToString(kstr);
	        				g_db->FeedReplicationDelOperation(dctx, k.ns, kstr);
	        			}
	        			delete_ks.pop_front();
	        		}
	        		LockGuard<ThreadMutexLock> guard(async_delete_lock);
	        		async_delete_lock.Wait(500);
	        	}
	        }
        public:
	        BackGroundThread():running(true)
            {

            }
	        void Shutdown()
	        {
	        	running = false;
	        	LockGuard<ThreadMutexLock> guard(async_delete_lock);
	            async_delete_lock.Notify();
	        }
	        void AsyncDelete(const KeyPrefix& k)
	        {
	        	LockGuard<ThreadMutexLock> guard(async_delete_lock);
	        	g_db->LockKey(k);
	        	async_delete_keys.push_back(k);
	        	async_delete_lock.Notify();
	        }

            virtual ~BackGroundThread()
            {
            }
    };

    int Ardb::AsyncDeleteKey(Context& ctx, const Data& ns, const std::string& key)
    {
    	KeyPrefix k;
    	k.ns = ns;
    	k.key.SetString(key, false);
    	g_background->AsyncDelete(k);
    	return 0;
    }

    int Ardb::CreateBackGroundThread()
    {
    	NEW(g_background, BackGroundThread);
    	g_background->Start();
    	return 0;
    }
    int Ardb::StopBackGroundThread()
    {
    	if(NULL != g_background)
    	{
    		g_background->Shutdown();
    		g_background->Join();
    		DELETE(g_background);
    	}
    	return 0;
    }
OP_NAMESPACE_END




