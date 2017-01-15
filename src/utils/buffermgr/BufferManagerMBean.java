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
 * <p>Title: BufferManagerMBean</p>
 * <p>Description: JMX MBean interface for {@link BufferManager}</p> 
 * <p><code>com.heliosapm.utils.buffer.BufferManagerMBean</code></p>
 */
public interface BufferManagerMBean {
	/** The JMX ObjectName string for the BufferManager management interface */
	public static final String OBJECT_NAME = "com.heliosapm.streams.buffers:service=BufferManager";
	
	/**
	 * Indicates if leak detection is enabled
	 * @return true if leak detection is enabled, false otherwise
	 */
	public boolean isLeakDetectionEnabled();
	
	/**
	 * Indicates if the server allocator is using pooled buffers
	 * @return true if the server allocator is using pooled buffers, false otherwise
	 */
	public boolean isPooledBuffers();

	/**
	 * Indicates if the server allocator is using direct buffers
	 * @return true if the server allocator is using direct buffers, false otherwise
	 */
	public boolean isDirectBuffers();

	/**
	 * Returns the number of heap arenas
	 * @return the number of heap arenas
	 */
	public int getHeapArenas();

	/**
	 * Returns the number of direct arenas
	 * @return the number of direct arenas
	 */
	public int getDirectArenas();

	/**
	 * Returns the memory page size for allocated pool buffer pages
	 * @return the memory page size for allocated pool buffer pages
	 */
	public int getPageSize();

	/**
	 * Returns the max order (I don't know what this is) 
	 * @return the max order
	 */
	public int getMaxOrder();

	/**
	 * Returns the size of the cache for tiny buffers
	 * @return the size of the cache for tiny buffers
	 */
	public int getTinyCacheSize();

	/**
	 * Returns the size of the cache for small buffers
	 * @return the size of the cache for small buffers
	 */
	public int getSmallCacheSize();

	/**
	 * Returns the size of the cache for normal buffers
	 * @return the size of the cache for normal buffers
	 */
	public int getNormalCacheSize();

	/**
	 * Returns a formatted report of the buffer stats
	 * @return a formatted report of the buffer stats
	 */
	public String printStats();
	
}
