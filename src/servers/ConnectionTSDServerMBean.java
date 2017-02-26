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
package net.opentsdb.servers;

import java.util.Map;

/**
 * <p>Title: ConnectionTSDServerMBean</p>
 * <p>Description: Base JMX MBean interface for connection based TSD servers</p> 
 * <p><code>net.opentsdb.servers.ConnectionTSDServerMBean</code></p>
 */

public interface ConnectionTSDServerMBean extends AbstractSocketTSDServerMBean {
//	/**
//	 * Sets the maximum number of connections, enforced as of the next connection attempt.
//	 * @param maxConnections the new maximum number of connections
//	 */
//	public void setMaxConnections(final int maxConnections);
//
//	/**
//	 * Sets the maximum connection idle time in seconds enforced on future connections
//	 * @param maxIdleTime the new maxIdleTime to set
//	 */
//	public void setMaxIdleTime(final long maxIdleTime);
	
	/**
	 * Returns a map of the server child channel's configuiration options
	 * @return a map of the server child  channel's configuiration options
	 */
	public Map<String, String> childChannelOptions();
	
	
	/**
	 * Returns the total number of closed idle connections
	 * @return the total number of closed idle connections
	 */
	public long getIdleConnectionsClosed();	

	/**
	 * Returns the number of clients connected through TSDServer
	 * @return the number of client connections
	 */
	public int getActiveConnections();
	
	/**
	 * Returns the maximum number of connections; Zero means unlimited
	 * @return the maximum number of connections
	 */
	public int getMaxConnections();

	/**
	 * Returns the maximum idle time in seconds; Zero means unlimited
	 * @return the the maximum idle time
	 */
	public long getMaxIdleTime();
	
	/**
	 * Returns the total monotonic count of established connections
	 * @return the established connections count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getConnectionsEstablished()
	 */
	public long getConnectionsEstablished();

	/**
	 * Returns the total monotonic  count of closed connections
	 * @return the closed connections count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getClosedConnections()
	 */
	public long getClosedConnections();

	/**
	 * Returns the total monotonic  count of rejected connections
	 * @return the rejected connections count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getRejectedConnections()
	 */
	public long getRejectedConnections();

	/**
	 * Returns the total monotonic  count of unknown connection exceptions
	 * @return the unknown connection exceptions count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getUnknownExceptions()
	 */
	public long getUnknownExceptions();

	/**
	 * Returns the total monotonic  count of connection close exceptions
	 * @return the connection close exceptions count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getCloseExceptions()
	 */
	public long getCloseExceptions();

	/**
	 * Returns the total monotonic  count of connection reset exceptions
	 * @return the connection reset exceptions count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getResetExceptions()
	 */
	public long getResetExceptions();

	/**
	 * Returns the total monotonic  count of idle connection closes
	 * @return the idle connection closes count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getTimeoutExceptions()
	 */
	public long getTimeoutExceptions();
	
}
