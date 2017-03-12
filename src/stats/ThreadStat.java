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
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.hbase.async.jsr166e.LongAdder;

/**
 * <p>Title: ThreadStat</p>
 * <p>Description: Enumerates different metrics that can be collected from a thread</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.stats.ThreadStat</code></p>
 */

public enum ThreadStat implements ThreadMetricCollector {
	ALLOCATION(1, ThreadStatConfiguration.threadAllocatedMemoryEnabled, "bytes"){
		@Override
		public long collect(final long threadId, final ThreadInfo threadInfo, LongAdder invCount) {
			return enabled ? ThreadStatConfiguration.threadAllocationReader.getAllocatedBytes(threadId) : -1L;
		}
	},
	CPU(2, ThreadStatConfiguration.threadCpuTimeEnabled, "ns"){
		@Override
		public long collect(final long threadId, final ThreadInfo threadInfo, LongAdder invCount) {
			return enabled ? ThreadStatConfiguration.tmx.getThreadCpuTime(threadId) + ThreadStatConfiguration.tmx.getThreadUserTime(threadId) : -1L;
		}
	},
	BLOCKS(4, true, "blockcount"){
		@Override
		public long collect(final long threadId, final ThreadInfo threadInfo, LongAdder invCount) {
			return threadInfo.getBlockedCount();
		}
	},
	BLOCK_TIME(8, ThreadStatConfiguration.threadContentionMonitoringEnabled, "ms"){
		@Override
		public long collect(final long threadId, final ThreadInfo threadInfo, LongAdder invCount) {
			return enabled ? threadInfo.getBlockedTime() : -1L;
		}
	},
	WAITS(16, true, "waitcount"){
		@Override
		public long collect(final long threadId, final ThreadInfo threadInfo, LongAdder invCount) {
			return threadInfo.getWaitedCount();
		}
	},
	WAIT_TIME(32, ThreadStatConfiguration.threadContentionMonitoringEnabled, "ms"){
		@Override
		public long collect(final long threadId, final ThreadInfo threadInfo, LongAdder invCount) {
			return enabled ? threadInfo.getWaitedTime() : -1L;
		}
	},
	COUNT(64, true, "executions"){
		@Override
		public long collect(final long threadId, final ThreadInfo threadInfo, LongAdder invCount) {
			if(invCount==null) return -1;
			invCount.increment();
			return 0;
		}
	},
	ELAPSED(128, true, "ns"){
		@Override
		public long collect(final long threadId, final ThreadInfo threadInfo, LongAdder invCount) {
			return System.nanoTime();
		}
	};	
	
	

	
	private ThreadStat(final int mask, final boolean enabled, final String unit) {
		this.mask = mask;
		this.enabled = enabled;
		this.unit = unit;
	}
	
	public final int mask; 
	public final boolean enabled;
	public final String unit;
	
	private static final ThreadStat[] values = values();
	public static final int SIZE = values.length;
	private static final ThreadStat[] valuesExceptCount = EnumSet.complementOf(EnumSet.of(COUNT)).toArray(new ThreadStat[0]);
	
	
	public static final byte THREAD_ALLOCATION = 0;
	public static final byte THREAD_CPU = 1;
	public static final byte THREAD_BLOCKS = 2;
	public static final byte THREAD_BLOCK_TIME = 3;
	public static final byte THREAD_WAITS = 4;
	public static final byte THREAD_WAIT_TIME = 5;
	public static final byte INVOCATION_COUNT = 6;
	public static final byte ELAPSED_TIME = 7;
	
	public static long[] enter(final Map<ThreadStat, LongAdder> stats) {
		return enter(stats.get(COUNT));
	}
	
	public static void exit(final Map<ThreadStat, LongAdder> stats, final long[] values) {
		final long[] computed = exit(values);
		for(ThreadStat stat: valuesExceptCount) {
			stats.get(stat).add(computed[stat.ordinal()]);
		}
	}
	
	public static long[] compute(final Map<ThreadStat, LongAdder> stats, final boolean reset) {
		final long[] computed = new long[SIZE];
		computed[INVOCATION_COUNT] = reset ? stats.get(COUNT).sumThenReset() : stats.get(COUNT).longValue();
		for(ThreadStat stat: valuesExceptCount) {
			computed[stat.ordinal()] = reset ? stats.get(stat).sumThenReset() : stats.get(stat).longValue();
		}
		return computed;
	}
	
	
	public static long[] enter(final Thread t, final LongAdder invCount) {		
		final long id = t.getId();
		final long[] values = new long[SIZE];
		if(invCount!=null) invCount.increment();
		final ThreadInfo ti = ThreadStatConfiguration.tmx.getThreadInfo(id);
		values[THREAD_WAITS] = ti.getWaitedCount();
		values[THREAD_BLOCKS] = ti.getBlockedCount();
		if(ThreadStatConfiguration.threadContentionMonitoringEnabled) {
			values[THREAD_WAIT_TIME] = ti.getWaitedTime();
			values[THREAD_BLOCK_TIME] = ti.getBlockedTime();
		}
		if(ThreadStatConfiguration.threadCpuTimeEnabled) {
			values[THREAD_CPU] = ThreadStatConfiguration.tmx.getCurrentThreadCpuTime() + ThreadStatConfiguration.tmx.getCurrentThreadUserTime();
		}
		if(ThreadStatConfiguration.threadAllocatedMemoryEnabled) {
			values[THREAD_ALLOCATION] = ThreadStatConfiguration.threadAllocationReader.getAllocatedBytes(id);
		}
		values[ELAPSED_TIME] = System.nanoTime();
		return values;
	}
	
	public static long[] enter(final LongAdder invCount) {
		return enter(Thread.currentThread(), invCount);
	}
	
	
	public static long[] exit(final Thread t, final long[] values) {		
		final long id = t.getId();
		final ThreadInfo ti = ThreadStatConfiguration.tmx.getThreadInfo(id);
		values[THREAD_WAITS] = ti.getWaitedCount() - values[THREAD_WAITS];
		values[THREAD_BLOCKS] = ti.getBlockedCount() - values[THREAD_BLOCKS];
		if(ThreadStatConfiguration.threadContentionMonitoringEnabled) {
			values[THREAD_WAIT_TIME] = ti.getWaitedTime() - values[THREAD_WAIT_TIME];
			values[THREAD_BLOCK_TIME] = ti.getBlockedTime() - values[THREAD_BLOCK_TIME];
		}
		if(ThreadStatConfiguration.threadCpuTimeEnabled) {
			values[THREAD_CPU] = ThreadStatConfiguration.tmx.getCurrentThreadCpuTime() + ThreadStatConfiguration.tmx.getCurrentThreadUserTime() - values[THREAD_CPU];
		}
		if(ThreadStatConfiguration.threadAllocatedMemoryEnabled) {
			values[THREAD_ALLOCATION] = ThreadStatConfiguration.threadAllocationReader.getAllocatedBytes(id) - values[THREAD_ALLOCATION];
		}
		values[ELAPSED_TIME] = System.nanoTime() - values[ELAPSED_TIME];
		return values;
	}

	public static long[] exit(final long[] values) {
		return exit(Thread.currentThread(), values);
	}
	
	
	public static void record(final StatsCollector collector, final String xtratag_key, final String xtratag_value, final long[] computed) {
		try {
			
			if(xtratag_key!=null) {
				collector.addExtraTag(xtratag_key, xtratag_value);
			}
			for(ThreadStat stat: values) {
				collector.record("rpc.thread." + stat.name().toLowerCase(), computed[stat.ordinal()], "unit=" + stat.unit);
			}
		} finally {
			if(xtratag_key!=null) collector.clearExtraTag(xtratag_key);
		}
	}
	

	
	public static class ThreadStatConfiguration {
		/** The thread memory allocation reader */
		private static final ThreadAllocationReader threadAllocationReader;
		
		public static final ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
		public static final boolean threadContentionMonitoringEnabled;
		public static final boolean threadAllocatedMemoryEnabled;
		public static final boolean threadCpuTimeEnabled;
		public static final String ENABLE_THREAD_CONTENTION_PROP = "java.thread.contention.enable";
		public static final String ENABLE_THREAD_ALLOC_PROP = "java.thread.allocation.enable";
		public static final String ENABLE_THREAD_CPU_PROP = "java.thread.cpu.enable";

		final static MBeanServer server = ManagementFactory.getPlatformMBeanServer();
		static boolean contention = false;
		static boolean allocation = false;
		static boolean cpu = false;
		static ThreadAllocationReader memReader = null;
		
		static {
		
			if(tmx.isThreadCpuTimeSupported()) {
				if(!tmx.isThreadCpuTimeEnabled()) {
					if(System.getProperties().containsKey(ENABLE_THREAD_CPU_PROP)) {
						try { tmx.setThreadCpuTimeEnabled(true); } catch (Exception x) {/* No Op */}					
					}
				}
				cpu = tmx.isThreadCpuTimeEnabled();
			}
			if(tmx.isThreadContentionMonitoringSupported()) {
				if(!tmx.isThreadContentionMonitoringEnabled()) {
					if(System.getProperties().containsKey(ENABLE_THREAD_CONTENTION_PROP)) {
						try { tmx.setThreadContentionMonitoringEnabled(true); } catch (Exception x) {/* No Op */}					
					}
				}
				contention = tmx.isThreadContentionMonitoringEnabled();
			}
			try {
				final ObjectName on = new ObjectName(ManagementFactory.THREAD_MXBEAN_NAME);
				final boolean supported = (Boolean)server.getAttribute(on, "ThreadAllocatedMemorySupported");
				if(supported) {
					final boolean enabled = (Boolean)server.getAttribute(on, "ThreadAllocatedMemoryEnabled");
					if(!enabled) {
						if(System.getProperties().containsKey(ENABLE_THREAD_ALLOC_PROP)) {
							server.setAttribute(on, new Attribute("ThreadAllocatedMemoryEnabled", true));
						}
					}
					allocation = (Boolean)server.getAttribute(on, "ThreadAllocatedMemoryEnabled");
					if(allocation) {
						memReader = (ThreadAllocationReader)
								Class.forName("net.opentsdb.stats.ThreadAllocationReaderImpl").newInstance();						
					}
				}
			} catch (Throwable x) {/* No Op */}
			threadAllocationReader = memReader;
			threadContentionMonitoringEnabled = contention;
			threadAllocatedMemoryEnabled = allocation;
			threadCpuTimeEnabled = cpu;
		}
		
	}
}
