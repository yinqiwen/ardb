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

#include "system_helper.hpp"
#include <string.h>
#if  __APPLE__
#include <sys/param.h>
#include <sys/sysctl.h>
#endif

namespace ardb
{
	uint32 available_processors()
	{
		uint32 ret = 1;
#ifdef __linux__
		int num = sysconf(_SC_NPROCESSORS_ONLN);
		if (num < 0)
		{
			ret = 1;
		}
		else
		{
			ret = static_cast<uint32>(num);
		}
#elif __APPLE__
		int nm[2];
		size_t len = 4;
		uint32_t count;

		nm[0] = CTL_HW;
		nm[1] = HW_AVAILCPU;
		sysctl(nm, 2, &count, &len, NULL, 0);

		if (count < 1)
		{
			nm[1] = HW_NCPU;
			sysctl(nm, 2, &count, &len, NULL, 0);
			if (count < 1)
			{
				count = 1;
			}
		}
		return count;
#endif
		return ret;
	}

#if defined(HAVE_PROC_STAT)
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
	size_t mem_rss_size()
	{
		int page = sysconf(_SC_PAGESIZE);
		size_t rss;
		char buf[4096];
		char filename[256];
		int fd, count;
		char *p, *x;

		snprintf(filename,256,"/proc/%d/stat",getpid());
		if ((fd = open(filename,O_RDONLY)) == -1) return 0;
		if (read(fd,buf,4096) <= 0)
		{
			close(fd);
			return 0;
		}
		close(fd);

		p = buf;
		count = 23; /* RSS is the 24th field in /proc/<pid>/stat */
		while(p && count--)
		{
			p = strchr(p,' ');
			if (p) p++;
		}
		if (!p) return 0;
		x = strchr(p,' ');
		if (!x) return 0;
		*x = '\0';

		rss = strtoll(p,NULL,10);
		rss *= page;
		return rss;
	}
#elif defined(HAVE_TASKINFO)
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <mach/task.h>
#include <mach/mach_init.h>
	size_t mem_rss_size()
	{
		task_t task = MACH_PORT_NULL;
		struct task_basic_info t_info;
		mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;
		if (task_for_pid(current_task(), getpid(), &task) != KERN_SUCCESS)
			return 0;
		task_info(task, TASK_BASIC_INFO, (task_info_t) &t_info, &t_info_count);
		return t_info.resident_size;
	}
#else
size_t mem_rss_size()
{
	return 0;
}
#endif
}
