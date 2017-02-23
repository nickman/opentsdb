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
 * <p>Title: ThreadPoolMonitorMBean</p>
 * <p>Description: JMX MBean interface for {@link ThreadPoolMonitor} instances</p> 
 * <p><code>net.opentsdb.stats.ThreadPoolMonitorMBean</code></p>
 */

public interface ThreadPoolMonitorMBean {
	
	/**
	 * Indicates if the pool is shutdown
	 * @return true if the pool is shutdown, false otherwise
	 */
	public boolean isShutdown();

	/**
	 * Indicates if the pool is terminating
	 * @return true if the pool is terminating, false otherwise
	 */
	public boolean isTerminating();

	/**
	 * Indicates if the pool is terminated
	 * @return true if the pool is terminated, false otherwise
	 */
	public boolean isTerminated();

	/**
	 * Modifies the core pool size
	 * @param corePoolSize The core pool size to set
	 */
	public void setCorePoolSize(int corePoolSize);

	/**
	 * Returns the core pool size
	 * @return the core pool size
	 */
	public int getCorePoolSize();

	/**
	 * Indicates if core threads can timeout
	 * @return true if core threads can timeout, false otherwise
	 */
	public boolean allowsCoreThreadTimeOut();

	/**
	 * Modifies the core max size
	 * @param maxPoolSize The max pool size to set
	 */
	public void setMaximumPoolSize(int maxPoolSize);

	/**
	 * Returns the max pool size
	 * @return the max pool size
	 */
	public int getMaximumPoolSize();

	/**
	 * Sets the non-core thread keep alive time in seconds
	 * @param time the non-core thread keep alive time in seconds
	 */
	public void setKeepAliveTime(long time);

	/**
	 * Returns the non-core thread keep alive time in seconds
	 * @return the non-core thread keep alive time in seconds
	 */
	public long getKeepAliveTime();

	/**
	 * Returns the number of pending tasks in the thread pool queue
	 * @return the number of pending tasks
	 */
	public int getQueueDepth();
	
	/**
	 * Returns the free space in the thread pool queue
	 * @return the free space in the thread pool queue
	 */
	public int getQueueCapacity();

	/**
	 * Purges all pending tasks in the pool
	 */
	public void purge();

	/**
	 * Returns the current pool size
	 * @return the pool size
	 */
	public int getPoolSize();

	/**
	 * Returns the current number of active tasks
	 * @return the active task count
	 */
	public int getActiveCount();

	/**
	 * Returns the maximum size the pool has ever been
	 * @return the maximum size the pool has ever been
	 */
	public int getLargestPoolSize();

	/**
	 * Returns the total number of tasks that have been scheduled for execution
	 * @return the total number of tasks that have been scheduled for execution
	 */
	public long getTaskCount();

	/**
	 * Returns the total number of tasks that have completed execution
	 * @return the total number of tasks that have completed execution
	 */
	public long getCompletedTaskCount();
	
	/**
	 * Indicates if thread memory allocation monitoring is enabled
	 * @return true if enabled, false otherwise
	 */
	public boolean isAllocationMonitored();
	
	/**
	 * Indicates if thread contention monitoring is enabled
	 * @return true if enabled, false otherwise
	 */
	public boolean isContentionMonitored();

	/**
	 * Indicates if thread cpu time monitoring is enabled
	 * @return true if enabled, false otherwise
	 */
	public boolean isCpuMonitored();
	

}
