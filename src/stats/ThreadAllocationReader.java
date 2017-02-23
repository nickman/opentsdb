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

/**
 * <p>Title: ThreadAllocationReader</p>
 * <p>Description: Defines a monitor that reads the number bytes of heap allocated by a specified thread</p> 
 * <p><code>net.opentsdb.stats.ThreadAllocationReader</code></p>
 */

public interface ThreadAllocationReader {
	/**
	 * Returns the number of bytes of memory allocated by the specified thread
	 * @param threadId The thread id
	 * @return the number of bytes 
	 */
	public long getAllocatedBytes(final long threadId);
}
