/*
 * pipe.hpp
 *
 *  Created on: 2013-5-6
 *      Author: wqy
 */

#ifndef PIPE_HPP_
#define PIPE_HPP_

#include <unistd.h>
#include "util/file_helper.hpp"

namespace ardb
{
	class Pipe
	{
		protected:
			int m_readFD;
			int m_writeFD;
		public:
			Pipe() :
					m_readFD(-1), m_writeFD(-1)
			{

			}
			int GetReadFD()
			{
				return m_readFD;
			}
			int GetWriteFD()
			{
				return m_writeFD;
			}
			int Open()
			{
				int fds[2];
				pipe(fds);
				m_readFD = fds[0];
				m_writeFD = fds[1];
				make_fd_nonblocking(m_readFD);
				make_fd_nonblocking(m_writeFD);
				return 0;
			}
			void Close()
			{
				if (-1 != m_readFD)
				{
					close(m_readFD);
					m_readFD = -1;
				}
				if (-1 != m_writeFD)
				{
					close(m_writeFD);
					m_writeFD = -1;
				}
			}
			~Pipe()
			{
				Close();
			}
	};

}

#endif /* PIPE_HPP_ */
