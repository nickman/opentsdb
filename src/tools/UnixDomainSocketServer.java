// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
import java.lang.management.ManagementFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.PipelineFactory;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Threads;

/**
 * <p>Title: UnixDomainSocketServer</p>
 * <p>Description: A unix socket put listener</p> 
 * <p><code>net.opentsdb.tools.UnixDomainSocketServer</code></p>
 * Trace like this:  echo "put foo.bar 1460070776 5 dc=dc5 host=host5" | netcat -U /tmp/tsdb.sock
 */

public class UnixDomainSocketServer  {
	/** The boss event loop group */
	final  EventLoopGroup bossGroup;
	/** The worker event loop group */
	final  EventLoopGroup workerGroup;
	/** The server bootstrap */
	final  ServerBootstrap b = new ServerBootstrap();
	/** The channel initializer */
	final PipelineFactory pipelineFactory;
	/** The server channel */
	Channel serverChannel;
	/** The unix socket address the server will listen on */ 
	final DomainSocketAddress socketAddress;
	/** The path of the unix socket */
	final String path;
	/** Instance logger */
	final Logger log = LoggerFactory.getLogger(getClass());
	
	/** The number of core available to this JVM */
	public static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
	
	
	/**
	 * Creates a new UnixDomainSocketServer
	 * @param tsdb The parent TSDB instance
	 */
	public UnixDomainSocketServer(final TSDB tsdb) {
		final Config config = tsdb.getConfig();
		path = config.getString("tsd.network.unixsocket.path");
		final int workerThreads = config.getInt("tsd.network.unixsocket.workers", CORES*2);
		bossGroup = new EpollEventLoopGroup(1, Threads.newThreadFactory("UnixSocketBoss"));
		workerGroup = new EpollEventLoopGroup(workerThreads, Threads.newThreadFactory("UnixSocketWorker#%d"));		
		pipelineFactory = new PipelineFactory(tsdb);
		b.group(bossGroup, workerGroup)
			.channel(EpollServerDomainSocketChannel.class)
			.childHandler(pipelineFactory);
		if(config.hasProperty("tsd.network.unixsocket.log")) {
			final String level = config.getString("tsd.network.unixsocket.log");
			LogLevel lev = null;
			try { 
				lev = LogLevel.valueOf(level.trim().toUpperCase()); 
			} catch(Exception x) {
				log.warn("Invalid LogLevel [{}]. Defaulted to DEBUG");
				lev = LogLevel.DEBUG;
			}
			b.handler(new LoggingHandler(lev));
		}
		socketAddress = new DomainSocketAddress(path);		  
	}
	
	/**
	 * Starts this unix domain socket server
	 * @throws Exception thrown on any error
	 */
	public void start() throws Exception {
		log.info("Starting UnixDomainSocketServer...");
		try {
			serverChannel = b.bind(socketAddress).sync().channel();
			log.error("Started UnixDomainSocketServer on [{}]", path);
		} catch (Exception ex) {
			log.error("Failed to start UnixDomainSocketServer on [{}]", path, ex);
			throw ex;
		}		
	}

	/**
	 * Stops this unix domain socket server
	 */
	public void stop() {
		log.info("Stopping UnixDomainSocketServer...");		
		try {
			if(serverChannel!=null) {
				serverChannel.close().sync();				
			}
		} catch (Exception ex) {
			log.error("Failed to stop UnixDomainSocketServer on [{}]", path, ex);
		}				
		try { bossGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
		try { workerGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
	}
	

}
