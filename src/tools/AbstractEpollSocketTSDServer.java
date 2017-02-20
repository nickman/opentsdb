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

import net.opentsdb.core.TSDB;

/**
 * <p>Title: AbstractEpollSocketTSDServer</p>
 * <p>Description: Base TSD server implementation for epoll based socket TSD servers</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tools.AbstractEpollSocketTSDServer</code></p>
 */

public class AbstractEpollSocketTSDServer extends AbstractSocketTSDServer {
	/** The channel event monitor for handling max connections, idle connections and event counts */
	protected final TSDServerEventMonitor eventMonitor;

	/**
	 * Creates a new AbstractEpollSocketTSDServer
	 * @param tsdb The parent TSDB instance
	 * @param protocol The protocol implemented by this server
	 */
	public AbstractEpollSocketTSDServer(final TSDB tsdb, final TSDProtocol protocol) {
		super(tsdb, protocol);
		eventMonitor = new TSDServerEventMonitor(channelGroup, maxConnections, maxIdleTime);
	}

}
