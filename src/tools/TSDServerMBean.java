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
package net.opentsdb.tools;

/**
 * <p>Title: TSDServerMBean</p>
 * <p>Description: JMX MBean interface for {@link TSDServer}</p> 
 * <p><code>net.opentsdb.tools.TSDServerMBean</code></p>
 */

public interface TSDServerMBean {
	
	/** The JMX ObjectName of the TSDServer */
	public static final String OBJECT_NAME = "net.opentsdb:service=TSDServer";
	
	/**
	 * Indicates if the TSDServer is started
	 * @return true if started, false otherwise
	 */
	public boolean isStarted();

	/**
	 * Returns the main listening port for telnet 
	 * and http requests
	 * @return the main listening port
	 */
	public int getPort();

	/**
	 * Returns the configured binding interface
	 * @return the binding interface
	 */
	public String getBindInterface();

	/**
	 * The socket address the server is bound to
	 * @return the socket address
	 */
	public String getBindSocket();

	/**
	 * Indicates if the Netty stack is async/non-blocking/Nio (true)
	 * or sync/blocking/Oio
	 * @return true if async, false otherwise
	 */
	public boolean isAsync();
	
	/**
	 * Indicates if epoll is supported
	 * @return true if supported, false otherwise
	 */
	public boolean isEpollSupported();
	

	/**
	 * Indicates if epoll has been disabled via configuration
	 * @return true if disabled, false otherwise
	 */
	public boolean isDisableEpoll();

	/**
	 * Returns the number of allocated worker threads
	 * @return the number of allocated worker threads
	 */
	public int getWorkerThreads();

	/**
	 * Returns the class name of the channel type configured for Netty
	 * @return the Netty channel type
	 */
	public String getChannelType();

	/**
	 * Returns the socket listener's configured backlog
	 * @return the socket backlog
	 */
	public int getBacklog();

	/**
	 * Returns the server's sotimeout in ms.
	 * @return the sotimeout
	 */
	public int getConnectTimeout();

	/**
	 * Indicates if Nagle's algorithm is disabled
	 * @return true if disabled, false otherwise
	 */
	public boolean isTcpNoDelay();

	/**
	 * Indicates if keepalive is enabled for server connections
	 * @return true if enabled, false otherwise
	 */
	public boolean isKeepAlive();

	/**
	 * Returns the number of times the underlying <code>Socket.write(...)</code>
	 * is called per Netty write operation
	 * @return the write spin count
	 */
	public int getWriteSpins();

	/**
	 * Returns the size of the connection receive buffers in bytes
	 * @return the receive buffer size
	 */
	public int getRecvBuffer();

	/**
	 * Returns the size of the connection send buffers in bytes
	 * @return the send buffer size
	 */
	public int getSendBuffer();

	/**
	 * Represents the server listener protocol and binding
	 * @return the server listener protocol and binding
	 */
	public String getServerURI();
	
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
	
	/**
	 * Resets the connection and exception counters
	 */
	public void resetCounters();
	
	/**
	 * Sets the maximum number of connections, enforced as of the next connection attempt.
	 * @param maxConnections the new maximum number of connections
	 */
	public void setMaxConnections(final int maxConnections);

	/**
	 * Sets the maximum connection idle time in seconds enforced on future connections
	 * @param maxIdleTime the new maxIdleTime to set
	 */
	public void setMaxIdleTime(final long maxIdleTime);

}
