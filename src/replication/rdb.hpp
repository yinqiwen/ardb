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
 * rdb.hpp
 *
 *  Created on: 2013年8月19日
 *      Author: yinqiwen
 */

#ifndef RDB_HPP_
#define RDB_HPP_
#include <string>
#include "common.hpp"
#include "ardb.hpp"

namespace ardb
{
	typedef void LoadRoutine(void* cb);
	class RedisDumpFile
	{
		private:
			FILE* m_read_fp;
			FILE* m_write_fp;
			std::string m_file_path;
			DBID m_current_db;
			Ardb* m_db;
			uint64 m_cksm;
			LoadRoutine* m_load_cb;
			void *m_load_cbdata;
			bool Read(void* buf, size_t buflen, bool cksm = true);
			void Close();
			int ReadType();
			time_t ReadTime();
			int64 ReadMillisecondTime();
			uint32_t ReadLen(int *isencoded);
			bool ReadInteger(int enctype, int64& v);
			bool ReadLzfStringObject(std::string& str);
			bool ReadString(std::string& str);
			bool ReadDoubleValue(double&val);
			bool LoadObject(int type, const std::string& key);

			void LoadListZipList(unsigned char* data, const std::string& key);
			void LoadHashZipList(unsigned char* data, const std::string& key);
			void LoadZSetZipList(unsigned char* data, const std::string& key);
			void LoadSetIntSet(unsigned char* data, const std::string& key);
		public:
			RedisDumpFile(Ardb* db, const std::string& file);
			int Load(LoadRoutine* cb, void *data);
			int Write(const char* buf, size_t buflen);
			void Flush();
			~RedisDumpFile();
	};
}

#endif /* RDB_HPP_ */
