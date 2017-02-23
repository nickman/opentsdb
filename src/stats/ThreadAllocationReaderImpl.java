// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.stats;

import java.lang.management.ManagementFactory;

/**
 * <p>Title: ThreadAllocationReaderImpl</p>
 * <p>Description: The default implementation of {@link ThreadAllocationReader}</p> 
 * <p><code>net.opentsdb.stats.ThreadAllocationReaderImpl</code></p>
 */
@SuppressWarnings("restriction")
public class ThreadAllocationReaderImpl implements ThreadAllocationReader {
	private final com.sun.management.ThreadMXBean ti;
	/**
	 * Creates a new ThreadAllocationReaderImpl
	 */
	public ThreadAllocationReaderImpl() {
		ti = (com.sun.management.ThreadMXBean)ManagementFactory.getThreadMXBean();
		try {
			ti.getThreadAllocatedBytes(Thread.currentThread().getId());
		} catch (Throwable t) {
			throw new UnsupportedOperationException("Thread Allocation Not Supported"); 			
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadAllocationReader#getAllocatedBytes(long)
	 */
	@Override
	public long getAllocatedBytes(final long threadId) {
		return ti.getThreadAllocatedBytes(threadId);
	}

}
