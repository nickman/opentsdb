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
 * <p>Title: AbstractSocketTSDServerMBean</p>
 * <p>Description: Base JMX MBean interface for {@link AbstractSocketTSDServer} instances</p> 
 * <p><code>net.opentsdb.servers.AbstractSocketTSDServerMBean</code></p>
 */

public interface AbstractSocketTSDServerMBean extends AbstractTSDServerMBean {
	/**
	 * Returns a map of the server channel's configuiration options
	 * @return a map of the server channel's configuiration options
	 */
	public Map<String, String> serverChannelOptions();
	

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
	 * Resets the connection and exception counters
	 */
	public void resetCounters();
	

	/**
	 * Indicates if SO_REUSEADDR is enabled 
	 * @return true if SO_REUSEADDR is enabled, false otherwise
	 */
	public boolean isReuseAddress();

}
