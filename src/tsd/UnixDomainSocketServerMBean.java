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
package net.opentsdb.tsd;

/**
 * <p>Title: UnixDomainSocketServerMBean</p>
 * <p>Description: JMX MBean interface for {@link UnixDomainSocketServer}</p> 
 * <p><code>net.opentsdb.tsd.UnixDomainSocketServerMBean</code></p>
 */

public interface UnixDomainSocketServerMBean {

	/** The JMX ObjectName of the UnixDomainSocketServer */
	public static final String OBJECT_NAME = "net.opentsdb:service=UnixDomainSocketServer";

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
	 * @see net.opentsdb.tools.TSDServerEventMonitor#getConnectionsEstablished()
	 */
	public long getConnectionsEstablished();

	/**
	 * Returns the total monotonic  count of closed connections
	 * @return the closed connections count
	 * @see net.opentsdb.tools.TSDServerEventMonitor#getClosedConnections()
	 */
	public long getClosedConnections();

	/**
	 * Returns the total monotonic  count of rejected connections
	 * @return the rejected connections count
	 * @see net.opentsdb.tools.TSDServerEventMonitor#getRejectedConnections()
	 */
	public long getRejectedConnections();

	/**
	 * Returns the total monotonic  count of unknown connection exceptions
	 * @return the unknown connection exceptions count
	 * @see net.opentsdb.tools.TSDServerEventMonitor#getUnknownExceptions()
	 */
	public long getUnknownExceptions();

	/**
	 * Returns the total monotonic  count of connection close exceptions
	 * @return the connection close exceptions count
	 * @see net.opentsdb.tools.TSDServerEventMonitor#getCloseExceptions()
	 */
	public long getCloseExceptions();

	/**
	 * Returns the total monotonic  count of connection reset exceptions
	 * @return the connection reset exceptions count
	 * @see net.opentsdb.tools.TSDServerEventMonitor#getResetExceptions()
	 */
	public long getResetExceptions();

	/**
	 * Returns the total monotonic  count of idle connection closes
	 * @return the idle connection closes count
	 * @see net.opentsdb.tools.TSDServerEventMonitor#getTimeoutExceptions()
	 */
	public long getTimeoutExceptions();
	

}
