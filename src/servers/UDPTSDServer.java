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

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.UDPPacketHandler;

/**
 * <p>Title: UDPTSDServer</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tools.UDPTSDServer</code></p>
 */

public class UDPTSDServer extends AbstractSocketTSDServer {

	/**
	 * Creates a new UDPTSDServer
	 * @param tsdb The parent TSDB instance
	 */
	private UDPTSDServer(final TSDB tsdb) {
		super(tsdb, TSDProtocol.UDP);
	}
	
	@Override
	public void collectStats(final StatsCollector collector) {		
		super.collectStats(collector);
		UDPPacketHandler.collectStats(collector);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractTSDServer#start()
	 */
	@Override
	public void start() throws Exception {
		if(started.compareAndSet(false, true)) {
			log.info("Starting UDP TSD server....");
			try {
				serverChannel = bootstrap.bind(bindSocket).sync().channel();
				registerMBean();				
				log.info("Started [{}] UDP TSD server listening on [{}]", channelType.getSimpleName(), bindSocket);
				afterStart();
			} catch (Exception ex) {
				log.error("Failed to bind to [{}]", bindSocket, ex);
				started.set(false);
				throw ex;
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractTSDServer#stop()
	 */
	@Override
	public void stop() {
		if(started.compareAndSet(true, false)) {
			log.info("Stopping UDP TSD server....");
			unregisterMBean();
			try {
				serverChannel.close().sync();
				log.info("UDP TSD server Server Channel Closed");
			} catch (Exception x) {/* No Op */}
			try { workerGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
			log.info("UDP TSD server Shut Down");
		}
	}
	
	

}
