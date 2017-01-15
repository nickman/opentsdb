// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
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
package net.opentsdb.utils.buffermgr;

/**
 * <p>Title: BufferArenaMonitorMBean</p>
 * <p>Description: JMX MBean interface for {@link BufferArenaMonitor} instances</p> 
 * <p><code>com.heliosapm.utils.buffer.BufferArenaMonitorMBean</code></p>
 */

public interface BufferArenaMonitorMBean {
	/**
	 * Returns the number of allocations allocations
	 * @return the number of allocations allocations
	 */
	public long getAllocations();

	/**
	 * Returns the number of tiny allocations
	 * @return the number of tiny allocations
	 */
	public long getTinyAllocations();

	/**
	 * Returns the number of small allocations
	 * @return the number of small allocations
	 */
	public long getSmallAllocations();

	/**
	 * Returns the number of normal allocations
	 * @return the number of normal allocations
	 */
	public long getNormalAllocations();

	/**
	 * Returns the number of huge allocations
	 * @return the number of huge allocations
	 */
	public long getHugeAllocations();

	/**
	 * Returns the number of deallocations allocations
	 * @return the number of deallocations allocations
	 */
	public long getDeallocations();

	/**
	 * Returns the number of tiny allocations
	 * @return the number of tiny allocations
	 */
	public long getTinyDeallocations();

	/**
	 * Returns the number of small allocations
	 * @return the number of small allocations
	 */
	public long getSmallDeallocations();

	/**
	 * Returns the number of normal allocations
	 * @return the number of normal allocations
	 */
	public long getNormalDeallocations();

	/**
	 * Returns the number of huge allocations
	 * @return the number of huge allocations
	 */
	public long getHugeDeallocations();

	/**
	 * Returns the number of active allocations
	 * @return the number of active allocations
	 */
	public long getActiveAllocations();

	/**
	 * Returns the number of activeTiny allocations
	 * @return the number of activeTiny allocations
	 */
	public long getActiveTinyAllocations();

	/**
	 * Returns the number of activeSmall allocations
	 * @return the number of activeSmall allocations
	 */
	public long getActiveSmallAllocations();

	/**
	 * Returns the number of activeNormal allocations
	 * @return the number of activeNormal allocations
	 */
	public long getActiveNormalAllocations();

	/**
	 * Returns the number of activeHuge allocations
	 * @return the number of activeHuge allocations
	 */
	public long getActiveHugeAllocations();
	
	/**
	 * Returns the type of arena
	 * @return the type of arena
	 */
	public String getType();
	
	/**
	 * Indicates if the arena is direct
	 * @return true if the arena is direct, false if it is heap
	 */
	public boolean isDirect();
	
	/**
	 * Returns the number of chunk lists
	 * @return the number of chunk lists
	 */
	public int getChunkLists();

	/**
	 * Returns the number of small subpages
	 * @return the small subpages
	 */
	public int getSmallSubPages();

	/**
	 * Returns the number of tiny subpages
	 * @return the tiny subpages
	 */
	public int getTinySubPages();
	
	/**
	 * Returns the total chunk allocated bytes for this pooled allocator
	 * @return the total chunk allocated bytes for this pooled allocator
	 */
	public long getTotalChunkSize();
	
	/**
	 * Returns the free chunk allocated bytes for this pooled allocator
	 * @return the free chunk allocated bytes for this pooled allocator
	 */
	public long getChunkFreeBytes();
	
	/**
	 * Returns the used chunk allocated bytes for this pooled allocator
	 * @return the used chunk allocated bytes for this pooled allocator
	 */
	public long getChunkUsedBytes();
	
	/**
	 * Return the percentage of the current usage of the total chunk space
	 * @return the percentage of the current usage of the total chunk space
	 */
	public int getChunkUsage();
	
	/**
	 * Returns the elapsed time of the last stats aggregation in ms.
	 * @return the elapsed time of the last stats aggregation in ms.
	 */
	public long getLastElapsed();
	

}
