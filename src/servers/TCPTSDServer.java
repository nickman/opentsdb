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

/**
 * <p>Title: TCPTSDServer</p>
 * <p>Description: The TCP TSD server implementation</p> 
 * <p><code>net.opentsdb.tools.TCPTSDServer</code></p>
 */

public class TCPTSDServer extends AbstractSocketTSDServer implements TCPTSDServerMBean {

	private TCPTSDServer(TSDB tsdb) {
		super(tsdb, TSDProtocol.TCP);		
	}

	@Override
	public void start() throws Exception {
		if(started.compareAndSet(false, true)) {
			log.info("Starting TCP TSD server....");
			try {
				serverChannel = bootstrap.bind(bindSocket).sync().channel();
				registerMBean();				
				log.info("Started [{}] TCP TSD server listening on [{}]", channelType.getSimpleName(), bindSocket);
				afterStart();
			} catch (Exception ex) {
				log.error("Failed to bind to [{}]", bindSocket, ex);
				started.set(false);
				throw ex;
			}
		}
	}

	@Override
	public void stop() {
		if(started.compareAndSet(true, false)) {
			log.info("Stopping TCP TSD server....");
			unregisterMBean();
			try {
				serverChannel.close().sync();
				log.info("TCP TSD server Server Channel Closed");
			} catch (Exception x) {/* No Op */}
			try { bossGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
			try { workerGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
			try { channelGroupExecutor.shutdownGracefully(); } catch (Exception x) {/* No Op */}						
			log.info("TCP TSD server Shut Down");
		}
	}


}
