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

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.ResourceLeakDetector;

/**
 * <p>Title: TSDBResourceLeakDetector</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.utils.buffermgr.TSDBResourceLeakDetector</code></p>
 */

public class TSDBResourceLeakDetector<T> extends ResourceLeakDetector<T> {

	/** Instance logger */
	private final Logger log = LoggerFactory.getLogger(getClass());
	/** The resource leak manager leaks are reported up to */
	private final ResourceLeakManager leakManager = ResourceLeakManager.getInstance(); 

	
	/** The JMX ObjectName for the leak detector */
	public static final String OBJECT_NAME = "net.opentsdb.buffers:service=LeakDetector";
	
	private static final AtomicLong INSTANCE_COUNT = new AtomicLong(0L);
	private final String resourceTypeName;
	
	
	/**
	 * Creates a new TSDBResourceLeakDetector
	 * @param resourceType
	 * @param samplingInterval
	 * @param maxActive
	 */
	public TSDBResourceLeakDetector(Class<?> resourceType, int samplingInterval, long maxActive) {
		super(resourceType, samplingInterval, maxActive);
		leakManager.addTrackedClass(resourceType);
		resourceTypeName = resourceType.getName();  
		log.info("Created [{}] TSDBResourceLeakDetector. Instances: {}", resourceType.getName(), INSTANCE_COUNT.incrementAndGet());
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.util.ResourceLeakDetector#reportInstancesLeak(java.lang.String)
	 */
	@Override
	protected void reportInstancesLeak(final String resourceType) {		
		leakManager.reportInstancesLeak(resourceTypeName, resourceType);
		//super.reportInstancesLeak(resourceType);
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.util.ResourceLeakDetector#reportTracedLeak(java.lang.String, java.lang.String)
	 */
	@Override
	protected void reportTracedLeak(final String resourceType, final String records) {
		leakManager.reportTracedLeak(resourceTypeName, resourceType, records);
		//super.reportTracedLeak(resourceType, records);
		
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.util.ResourceLeakDetector#reportUntracedLeak(java.lang.String)
	 */
	@Override
	protected void reportUntracedLeak(final String resourceType) {
		leakManager.reportUntracedLeak(resourceTypeName, resourceType);
		//super.reportUntracedLeak(resourceType);
	}

}
