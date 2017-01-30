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

import java.util.Map;

/**
 * <p>Title: ResourceLeakManagerMBean</p>
 * <p>Description: JMX MBean interface for {@link ResourceLeakManager}</p> 
 * <p><code>net.opentsdb.utils.buffermgr.ResourceLeakManagerMBean</code></p>
 */

public interface ResourceLeakManagerMBean {
	
	/** The JMX ObjectName for the ResourceLeakManager */
	public static final String OBJECT_NAME = "net.opentsdb.buffers:service=ResourceLeakManager";
	
	/**
	 * Returns a map of the number of leaks keyed by the tracked class
	 * @return the tracked class leak counts
	 */
	public Map<String, Long> getTrackedClasses();
	
	/**
	 * Returns the total number of reported leaks since start or the last reset
	 * @return the total number of reported leaks
	 */
	public long getTotalLeaks();
	
	/**
	 * Returns a map of the number of leaks keyed by the leak hint
	 * @return the hint leak counts
	 */
	public Map<String, Long> getHints();	

}
