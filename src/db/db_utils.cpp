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
#include "thread/spin_mutex_lock.hpp"
#include "thread/lock_guard.hpp"
#include "db_utils.hpp"
#include "util/file_helper.hpp"
#include "db.hpp"
OP_NAMESPACE_BEGIN
    static DataSet g_nss;
    static SpinMutexLock g_nss_lock;
    static bool g_nss_loaded = false;
    int DBHelper::ListNameSpaces(DataArray& nss)
    {
        LockGuard<SpinMutexLock> guard(g_nss_lock);
        std::string dbs_file = g_db->GetConf().data_base_path + "/.dbs";
        if (!g_nss_loaded)
        {
            std::string content;
            file_read_full(dbs_file, content);
            std::vector<std::string> ss = split_string(content, "\n");
            for (size_t i = 0; i < ss.size(); i++)
            {
                Data ns;
                ns.SetString(trim_string(ss[i], "\r\n\t "), false);

                g_nss.insert(ns);
            }
            g_nss_loaded = true;
        }
        DataSet::iterator it = g_nss.begin();
        while (it != g_nss.end())
        {
            nss.push_back(*it);
            it++;
        }
        return 0;
    }
    int DBHelper::AddNameSpace(const Data& ns)
    {
        LockGuard<SpinMutexLock> guard(g_nss_lock);
        if (g_nss.insert(ns).second)
        {
            std::string dbs_file = g_db->GetConf().data_base_path + "/.dbs";
            std::string content = ns.AsString() + "\r\n";
            file_append_content(dbs_file, content);
        }
        return 0;
    }
    int DBHelper::DelNamespace(const Data& ns)
    {
        LockGuard<SpinMutexLock> guard(g_nss_lock);
        if (g_nss.erase(ns) > 0)
        {
            std::string dbs_file = g_db->GetConf().data_base_path + "/.dbs";
            std::string content;
            DataSet::iterator it = g_nss.begin();
            while (it != g_nss.end())
            {
                content.append(it->AsString()).append("\r\n");
                it++;
            }
            file_write_content(dbs_file, content);
        }
        return 0;
    }

OP_NAMESPACE_END

