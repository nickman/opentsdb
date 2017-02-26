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

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

import io.netty.buffer.ByteBuf;
import io.netty.channel.unix.DomainSocketAddress;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: UNIXTSDServer</p>
 * <p>Description: Unix Domain Socket TSD server implementation</p> 
 * <p><code>net.opentsdb.servers.UNIXTSDServer</code></p>
 */

public class UNIXTSDServer extends AbstractSocketTSDServer implements UNIXTSDServerMBean {
	/** The unix socket file path */
	final String path;
	/** The modified access mask on the socket file */
	String socketAccessMask = null;
	
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
				setAccessMask();
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
	
	/**
	 * Sets the configured access mask on the unix socket file
	 */
	protected void setAccessMask() {
		final Config config = tsdb.getConfig();
		String setMask = null;
		if(config.hasProperty("tsd.network.unixsocket.openaccess")) {
			final String accessMask = config.getString("tsd.network.unixsocket.openaccess");
			if(!accessMask.isEmpty()) {
				Process p = null;
				final int rv;
				try {
					p = new ProcessBuilder("chmod", accessMask, path).start();
					rv = p.waitFor();
					if (rv != 0) {
						log.error("Failed to set access [{}] on Unix Server Socket [{}]. RV:[{}]", accessMask, path, rv);
					} else {
						setMask = accessMask;
						log.info("Set access [{}] on Unix Server Socket [{}]", accessMask, path);
					}
				} catch (Exception ex) {
					log.error("Failed to set access on Unix Server Socket [{}]", path, ex);
				} finally {
					if(p!=null) try { p.destroy(); } catch (Exception x) {/* No Op */}
				}
			}
		}
		socketAccessMask = setMask;
	}
	
	/**
	 * Returns the successfully set access mask on the unix socket file
	 * @return the unix socket file access mask 
	 */
	public String getAccessMask() {
		return socketAccessMask;
	}
	
	

}
