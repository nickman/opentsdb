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
package net.opentsdb.utils.buffermgr;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.ObjectName;

import org.hbase.async.jsr166e.LongAdder;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: ResourceLeakManager</p>
 * <p>Description: Management interface to track resource leaks</p> 
 * <p><code>net.opentsdb.utils.buffermgr.ResourceLeakManager</code></p>
 */

public class ResourceLeakManager implements ResourceLeakManagerMBean {
	/** The singleton instance */
	private static volatile ResourceLeakManager instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	/** Instance logger */
	private final Logger log = LoggerFactory.getLogger(getClass());
	/** The total number of leaks reported */
	private final LongAdder totalLeaks = new LongAdder();
	/** The classes being tracked */
	private final ConcurrentHashMap<String, LongAdder> trackedClasses = new ConcurrentHashMap<String, LongAdder>(24, 0.75f, CORES);
	/** The leaker hints */
	private final ConcurrentHashMap<String, LongAdder> leakerHints = new ConcurrentHashMap<String, LongAdder>(128, 0.75f, CORES);
	
	/** Available processors for initing concurrent maps */
	private static final int CORES = Runtime.getRuntime().availableProcessors();
	/** Place holder object */
	private static final LongAdder PLACE_HOLDER = new LongAdder();
	/** Pattern for extracting leak hints from the records */
	private static final Pattern HINT_PATTERN = Pattern.compile("Hint:\\s(.*?)\\s+");
	
	/** The system property setting the maximum number of leak records to retain */
	public static final String MAX_RECORDS_PROP = "io.netty.leakDetection.maxRecords";
	/** The default maximum number of leak records to retain */
	public static final String DEFAULT_MAX_RECORDS = "1000";
	
	/**
	 * Acquires the ResourceLeakManager singleton
	 * @return the ResourceLeakManager singleton
	 */
	public static ResourceLeakManager getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new ResourceLeakManager();
				}
			}
		}
		return instance;
	}
	
	private ResourceLeakManager() {
		System.setProperty(MAX_RECORDS_PROP, DEFAULT_MAX_RECORDS);
		try {
			final ObjectName objectName = new ObjectName(OBJECT_NAME);
			ManagementFactory.getPlatformMBeanServer().registerMBean(this, objectName);
		} catch (Exception ex) {
			log.warn("Failed to register ResourceLeakManager management interface.", ex);
		}
	}
	
	/**
	 * Adds a tracked class
	 * @param clazz The tracked class
	 */
	void addTrackedClass(final Class<?> clazz) {
		if(clazz!=null) {
			final String key = clazz.getName();
			LongAdder la = trackedClasses.putIfAbsent(key, PLACE_HOLDER);
			if(la==null || la==PLACE_HOLDER) {
				trackedClasses.replace(key, new LongAdder());
			}
		}
	}
	
	/**
	 * 
	 * @see io.netty.util.ResourceLeakDetector#reportInstancesLeak(java.lang.String)
	 */	
	void reportInstancesLeak(final String className, final String resourceType) {
		totalLeaks.increment();
		trackedClasses.get(className).increment();
		log.warn("---INSTANCES LEAK: [{}]/[{}]", className, resourceType);		
	}
	
	/**
	 * 
	 * @see io.netty.util.ResourceLeakDetector#reportTracedLeak(java.lang.String, java.lang.String)
	 */
	void reportTracedLeak(final String className, final String resourceType, final String records) {
		totalLeaks.increment();
		trackedClasses.get(className).increment();
		final Matcher m = HINT_PATTERN.matcher(records);
		if(m.find()) {
			final String hint = m.group(1).trim();
			//leakerHints
			LongAdder la = leakerHints.putIfAbsent(hint, PLACE_HOLDER);
			if(la==null || la==PLACE_HOLDER) {
				la = new LongAdder();
				la.increment();
				leakerHints.replace(hint, la);
			}
		}
		log.warn("---TRACED LEAK: [{}]/[{}/{}]", className, resourceType, records);
	}
	
	/**
	 * 
	 * @see io.netty.util.ResourceLeakDetector#reportUntracedLeak(java.lang.String)
	 */
	void reportUntracedLeak(final String className, final String resourceType) {	
		totalLeaks.increment();
		trackedClasses.get(className).increment();
		log.warn("---UNTRACED LEAK: [{}/{}]", className, resourceType);
	}
	
	/**
	 * Returns a map of the number of leaks keyed by the tracked class
	 * @return the tracked class leak counts
	 */
	public Map<String, Long> getTrackedClasses() {
		final Map<String, Long> map = new HashMap<String, Long>(trackedClasses.size());
		for(Map.Entry<String, LongAdder> entry: trackedClasses.entrySet()) {
			map.put(entry.getKey(), entry.getValue().longValue());
		}
		return map;
	}
	
	/**
	 * Returns a map of the number of leaks keyed by the leak hint
	 * @return the hint leak counts
	 */
	public Map<String, Long> getHints() {
		final Map<String, Long> map = new HashMap<String, Long>(leakerHints.size());
		for(Map.Entry<String, LongAdder> entry: leakerHints.entrySet()) {
			map.put(entry.getKey(), entry.getValue().longValue());
		}
		return map;
	}
	
	
	/**
	 * Returns the total number of reported leaks since start or the last reset
	 * @return the total number of reported leaks
	 */
	public long getTotalLeaks() {
		return totalLeaks.longValue();
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.utils.buffermgr.ResourceLeakManagerMBean#getMaxLeakRecords()
	 */
	@Override
	public int getMaxLeakRecords() {
		return Integer.parseInt(System.getProperty(MAX_RECORDS_PROP, DEFAULT_MAX_RECORDS));
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.utils.buffermgr.ResourceLeakManagerMBean#setMaxLeakRecords(int)
	 */
	@Override
	public void setMaxLeakRecords(final int max) {
		if(max < 1) throw new IllegalArgumentException("Invalid MaxLeakRecords:" + max);
		System.setProperty(MAX_RECORDS_PROP, "" + max);
	}
}
