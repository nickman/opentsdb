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
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.hbase.async.jsr166e.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.servers.ExecutorThreadFactory;

/**
 * <p>Title: ThreadPoolMonitor</p>
 * <p>Description: JMX managed thread pool monitor</p> 
 * <p><code>net.opentsdb.stats.ThreadPoolMonitor</code></p>
 */

public class ThreadPoolMonitor implements ThreadPoolMonitorMBean {
	/** The thread pool being monitored */
	protected final ThreadPoolExecutor threadPoolExecutor;
	/** The thread pool name */
	protected final String name;
	/** The monitor JMX ObjectName */
	protected final ObjectName objectName;
	/** The thread pool queue */
	protected final BlockingQueue<Runnable> queue;
	/** The executor thread factory (if it is one) */
	protected final ExecutorThreadFactory etf;
	
	/** Static class logger */
	protected static final Logger LOG = LoggerFactory.getLogger(ThreadPoolMonitor.class);
	/** The JMX ObjectName pattern for monitors */
	public static final String OBJECT_NAME = "net.opentsdb.threads:service=ThreadPool,name=%s"; 
	/** The registered thread pool monitors keyed by name */
	private static final Map<String, ThreadPoolMonitor> monitors = new ConcurrentHashMap<String, ThreadPoolMonitor>();
	
	public static final ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
	public static final boolean threadContentionMonitoringEnabled;
	public static final boolean threadAllocatedMemoryEnabled;
	public static final boolean threadCpuTimeEnabled;
	public static final String ENABLE_THREAD_CONTENTION_PROP = "java.thread.contention.enable";
	public static final String ENABLE_THREAD_ALLOC_PROP = "java.thread.allocation.enable";
	public static final String ENABLE_THREAD_CPU_PROP = "java.thread.cpu.enable";
	
	protected final LongAdder priorWaits;
	protected final LongAdder priorBlocks;
	protected final LongAdder priorWaitTime;
	protected final LongAdder priorBlockTime;
	protected final LongAdder priorCpuTime;
	protected final LongAdder priorAllocation;
	
	
	static {
		final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
		boolean contention = false;
		boolean allocation = false;
		boolean cpu = false;
		
		if(tmx.isThreadCpuTimeSupported()) {
			if(!tmx.isThreadCpuTimeEnabled()) {
				if(System.getProperties().containsKey(ENABLE_THREAD_CPU_PROP)) {
					try { tmx.setThreadCpuTimeEnabled(true); } catch (Exception x) {/* No Op */}
					cpu = tmx.isThreadCpuTimeEnabled();
				}
			}
		}
		if(tmx.isThreadContentionMonitoringSupported()) {
			if(!tmx.isThreadContentionMonitoringEnabled()) {
				if(System.getProperties().containsKey(ENABLE_THREAD_CONTENTION_PROP)) {
					try { tmx.setThreadContentionMonitoringEnabled(true); } catch (Exception x) {/* No Op */}
					contention = tmx.isThreadContentionMonitoringEnabled();
				}
			}
		}
		try {
			final ObjectName on = new ObjectName(ManagementFactory.THREAD_MXBEAN_NAME);
			final boolean supported = (Boolean)server.getAttribute(on, "ThreadAllocatedMemorySupported");
			if(supported) {
				final boolean enabled = (Boolean)server.getAttribute(on, "ThreadAllocatedMemoryEnabled");
				if(!enabled) {
					if(System.getProperties().containsKey(ENABLE_THREAD_ALLOC_PROP)) {
						server.setAttribute(on, new Attribute("ThreadAllocatedMemoryEnabled", true));
						allocation = (Boolean)server.getAttribute(on, "ThreadAllocatedMemoryEnabled");
					}
				}
			}
		} catch (Exception x) {/* No Op */}		
		threadContentionMonitoringEnabled = contention;
		threadAllocatedMemoryEnabled = allocation;
		threadCpuTimeEnabled = cpu;		
	}
	
	/**
	 * Collects the stats and metrics for all thread pools
	 * @param collector The collector to use.
	 */
	public static void collectStats(final StatsCollector collector) {
		if(!monitors.isEmpty()) {
			for(ThreadPoolMonitor tpm: monitors.values()) {
				tpm.doCollectStats(collector);
			}
		}
	}

	/**
	 * Collects the stats and metrics for this thread pools
	 * @param collector The collector to use.
	 */
	public void doCollectStats(final StatsCollector collector) {
		if(etf!=null) {
			
		}
		
	}
	  
	
	/**
	 * Creates and registers a new ThreadPoolMonitor
	 * @param threadPoolExecutor The thread pool to monitor
	 * @param name The thread pool name
	 */
	public static void installMonitor(final ThreadPoolExecutor threadPoolExecutor, final String name) {
		if(threadPoolExecutor==null) throw new IllegalArgumentException("The passed ThreadPoolExecutor was null");
		if(name==null || name.trim().isEmpty()) throw new IllegalArgumentException("The passed name was null or empty");
		final String _name = name.trim().replace("-%d", "").replace("%d", "").replace("#", "");
		try {
			final ObjectName objectName = new ObjectName(String.format(OBJECT_NAME, _name));
			final ThreadPoolMonitor tpm = new ThreadPoolMonitor(threadPoolExecutor, _name, objectName);
			final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
			if(server.isRegistered(objectName)) {
				LOG.warn("Thread pool named [{}] already registered. Duplicate name ?");
			}
			ManagementFactory.getPlatformMBeanServer().registerMBean(tpm, objectName);
			monitors.put(_name, tpm);
			LOG.info("Registered ThreadPoolMonitor for [{}]", _name);
		} catch (Exception ex) {
			LOG.warn("Failed to create monitor for thread pool named [{}]", _name, ex);
		}
	}
	
	/**
	 * Creates a new ThreadPoolMonitor
	 * @param threadPoolExecutor The thread pool being monitored
	 * @param name The thread pool name
	 * @param objectName The monitor JMX ObjectName
	 */
	private ThreadPoolMonitor(final ThreadPoolExecutor threadPoolExecutor, final String name, final ObjectName objectName) {
		this.threadPoolExecutor = threadPoolExecutor;
		this.name = name;
		this.objectName = objectName;
		this.queue = threadPoolExecutor.getQueue();
		final ThreadFactory tf = threadPoolExecutor.getThreadFactory(); 
		if(tf instanceof ExecutorThreadFactory) {
			etf = (ExecutorThreadFactory)tf;
			priorWaits = new LongAdder();
			priorBlocks = new LongAdder();
			priorWaitTime = new LongAdder();
			priorBlockTime = new LongAdder();
			priorCpuTime = new LongAdder();
			priorAllocation = new LongAdder();
			
			priorWaits.decrement();
			priorBlocks.decrement();
			priorWaitTime.decrement();
			priorBlockTime.decrement();
			priorCpuTime.decrement();
			priorAllocation.decrement();
		} else {
			etf = null;
			priorWaits = null;
			priorBlocks = null;
			priorWaitTime = null;
			priorBlockTime = null;
			priorCpuTime = null;
			priorAllocation = null;			
		}		
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#isShutdown()
	 */
	@Override
	public boolean isShutdown() {
		return threadPoolExecutor.isShutdown();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#isTerminating()
	 */
	@Override
	public boolean isTerminating() {
		return threadPoolExecutor.isTerminating();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#isTerminated()
	 */
	@Override
	public boolean isTerminated() {
		return threadPoolExecutor.isTerminated();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#setCorePoolSize(int)
	 */
	@Override
	public void setCorePoolSize(final int corePoolSize) {
		threadPoolExecutor.setCorePoolSize(corePoolSize);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#getCorePoolSize()
	 */
	@Override
	public int getCorePoolSize() {
		return threadPoolExecutor.getCorePoolSize();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#allowsCoreThreadTimeOut()
	 */
	@Override
	public boolean allowsCoreThreadTimeOut() {
		return threadPoolExecutor.allowsCoreThreadTimeOut();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#setMaximumPoolSize(int)
	 */
	@Override
	public void setMaximumPoolSize(final int maximumPoolSize) {
		threadPoolExecutor.setMaximumPoolSize(maximumPoolSize);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#getMaximumPoolSize()
	 */
	@Override
	public int getMaximumPoolSize() {
		return threadPoolExecutor.getMaximumPoolSize();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#purge()
	 */
	@Override
	public void purge() {
		threadPoolExecutor.purge();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#getPoolSize()
	 */
	@Override
	public int getPoolSize() {
		return threadPoolExecutor.getPoolSize();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#getActiveCount()
	 */
	@Override
	public int getActiveCount() {
		return threadPoolExecutor.getActiveCount();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#getLargestPoolSize()
	 */
	@Override
	public int getLargestPoolSize() {
		return threadPoolExecutor.getLargestPoolSize();
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#getTaskCount()
	 */
	@Override
	public long getTaskCount() {
		return threadPoolExecutor.getTaskCount();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#getCompletedTaskCount()
	 */
	@Override
	public long getCompletedTaskCount() {
		return threadPoolExecutor.getCompletedTaskCount();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#setKeepAliveTime(long)
	 */
	@Override
	public void setKeepAliveTime(final long time) {
		threadPoolExecutor.setKeepAliveTime(time, TimeUnit.SECONDS);		
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#getKeepAliveTime()
	 */
	@Override
	public long getKeepAliveTime() {
		return threadPoolExecutor.getKeepAliveTime(TimeUnit.SECONDS);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#getQueueDepth()
	 */
	@Override
	public int getQueueDepth() {		
		return queue.size();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.stats.ThreadPoolMonitorMBean#getQueueCapacity()
	 */
	@Override
	public int getQueueCapacity() {		
		return queue.remainingCapacity();
	}

}
