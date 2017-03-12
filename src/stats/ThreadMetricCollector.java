package net.opentsdb.stats;

import java.lang.management.ThreadInfo;

import org.hbase.async.jsr166e.LongAdder;

public interface ThreadMetricCollector {
	/**
	 * Collects a stat for the passed thread if
	 * @param threadId The thread id
	 * @param threadInfo The thread info
	 * @param invCount The invocation counter
	 * @return the stat value
	 */
	public long collect(final long threadId, ThreadInfo threadInfo, LongAdder invCount);
	
	
}