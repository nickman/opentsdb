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

import io.netty.channel.unix.DomainSocketAddress;
import net.opentsdb.core.TSDB;

/**
 * <p>Title: UNIXTSDServer</p>
 * <p>Description: Unix Domain Socket TSD server implementation</p> 
 * <p><code>net.opentsdb.servers.UNIXTSDServer</code></p>
 */

public class UNIXTSDServer extends AbstractSocketTSDServer implements UNIXTSDServerMBean {
	/** The unix socket file path */
	final String path;
	
	private UNIXTSDServer(TSDB tsdb) {
		super(tsdb, TSDProtocol.UNIX);
		path = ((DomainSocketAddress)bindSocket).path();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractTSDServer#start()
	 */
	@Override
	public void start() throws Exception {
		if(started.compareAndSet(false, true)) {
			log.info("Starting Unix Domain Socket TSD server....");
			try {
				serverChannel = bootstrap.bind(bindSocket).sync().channel();
				registerMBean();
				log.info("Started Unix Domain Socket TSD server on [{}]", path);
				afterStart();
			} catch (Exception ex) {
				log.error("Failed to start Unix Domain Socket TSD server on [{}]", path, ex);
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
			log.info("Stopping Unix Domain Socket TSD server...");	
			unregisterMBean();
			try {
				if(serverChannel!=null) {
					serverChannel.close().sync();				
				}
			} catch (Exception ex) {
				log.warn("Failed to stop Unix Domain Socket TSD server on [{}]", path, ex);
			}				
			try { bossGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
			try { workerGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
			try { channelGroupExecutor.shutdownGracefully(); } catch (Exception x) {/* No Op */}
			log.info("UnixDomainSocketServer shut down");
		}
	}
	
	

}
