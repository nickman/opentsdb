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

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.oio.OioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.PipelineFactory;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Threads;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: TSDServer</p>
 * <p>Description: Configuration and bootstrap for the TCP OpenTSDB listening server</p> 
 * <p><code>net.opentsdb.tools.TSDTCPServer</code></p>
 * TODO:
 * Configs:
 * 	Watermarks
 * 	Message Size Estimators
 *  Socket Performance Preferences ??
 *  
 */

/**
 * <p>Title: TSDServer</p>
 * <p>Description: The main TSD network server</p> 
 * <p><code>net.opentsdb.tools.TSDServer</code></p>
 */
public class TSDServer {

	/** Indicates if we're on linux in which case, async will use epoll */
	public static final boolean IS_LINUX = System.getProperty("os.name").toLowerCase().contains("linux");
	/** The number of core available to this JVM */
	public static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
	
	/** The instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** The port to listen on */
	protected final int port;
	/** The nic interface to bind to */
	protected final String bindInterface;
	/** The socket address that the listener will be bound to */
	protected final InetSocketAddress bindSocket;	
	/** Indicates if we're using asynchronous net io */
	protected final boolean async;
	/** Indicates if epoll has been disabled even if we're on linux and using asynchronous net io */
	protected final boolean disableEpoll;
	
	/** The netty server bootstrap */
	protected final ServerBootstrap serverBootstrap = new ServerBootstrap();
	/** The configured number of worker threads */
	protected final int workerThreads;
	
	/** The channel type this server will create */
	protected final Class<? extends ServerChannel> channelType;
	
	/** The netty boss event loop group */
	protected final EventLoopGroup bossGroup;
	/** The netty boss event loop group's executor and thread factory */
	protected final Executor bossExecutorThreadFactory;
	
	
	/** The netty worker event loop group */
	protected final EventLoopGroup workerGroup;
	/** The netty worker event loop group's executor and thread factory */
	protected final Executor workerExecutorThreadFactory;
	
	/** The Netty ByteBuf manager */
	final BufferManager bufferManager;
	
	/** The server channel created on socket bind */
	protected Channel serverChannel = null;
	/** The Unix Socket Server */
	protected final UnixDomainSocketServer unixDomainSocketServer;
	
	// =============================================
	// Channel Configs
	// =============================================
	/** The size of the server socket's backlog queue */
	protected final int backlog;
	/** Indicates if reuse address should be enabled */
	protected final boolean reuseAddress;
	/** The server's connect timeout in ms */
	protected final int connectTimeout;
	
	
	// =============================================
	// Child Channel Configs
	// =============================================
	/** Indicates if tcp no delay should be enabled */
	protected final boolean tcpNoDelay;
	/** Indicates if tcp keep alive should be enabled */
	protected final boolean keepAlive;
	/** The write spin count */
	protected final int writeSpins;
	/** The size of a channel's receive buffer in bytes */
	protected final int recvBuffer;
	/** The size of a channel's send buffer in bytes */
	protected final int sendBuffer;
	
	/** The server URI */
	public final URI serverURI;

	/**
	 * Creates a new TSDServer
	 * @param config The final config, used to configure this server
	 * @param pipelineFactory The channel pipeline initializer
	 */
	public TSDServer(final TSDB tsdb, final PipelineFactory pipelineFactory) {
		final Config config = tsdb.getConfig();
		bufferManager = BufferManager.getInstance(config);
		port = config.getInt("tsd.network.port");
		bindInterface = config.getString("tsd.network.bind", "0.0.0.0");
		bindSocket = new InetSocketAddress(bindInterface, port);
		workerThreads = config.getInt("tsd.network.worker_threads", CORES * 2);
		connectTimeout = config.getInt("tsd.network.sotimeout", 0);
		backlog = config.getInt("tsd.network.backlog", 3072);
		writeSpins = config.getInt("tsd.network.writespins", 16);
		recvBuffer = config.getInt("tsd.network.recbuffer", 43690);
		sendBuffer = config.getInt("tsd.network.sendbuffer", 8192);
		disableEpoll = config.getBoolean("tsd.network.epoll.disable", false);
		async = config.getBoolean("tsd.network.async_io", true);
		tcpNoDelay = config.getBoolean("tsd.network.tcp_no_delay", true);
		keepAlive = config.getBoolean("tsd.network.keep_alive", true);
		reuseAddress = config.getBoolean("tsd.network.reuse_address", true);
		serverBootstrap.handler(new LoggingHandler(getClass(), LogLevel.INFO));		
		serverBootstrap.childHandler(pipelineFactory);
		
		// Set the child options
		serverBootstrap.childOption(ChannelOption.ALLOCATOR, bufferManager);
		serverBootstrap.childOption(ChannelOption.TCP_NODELAY, tcpNoDelay);
		serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, keepAlive);
		serverBootstrap.childOption(ChannelOption.SO_RCVBUF, recvBuffer);
		serverBootstrap.childOption(ChannelOption.SO_SNDBUF, sendBuffer);
		serverBootstrap.childOption(ChannelOption.WRITE_SPIN_COUNT, writeSpins);
		// Set the server options
		serverBootstrap.option(ChannelOption.ALLOCATOR, bufferManager);
		serverBootstrap.option(ChannelOption.SO_BACKLOG, backlog);
		serverBootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);
		serverBootstrap.option(ChannelOption.SO_RCVBUF, recvBuffer);
		serverBootstrap.option(ChannelOption.SO_TIMEOUT, connectTimeout);
		final StringBuilder uri = new StringBuilder("tcp");
		if(async) {
			if(IS_LINUX && !disableEpoll) {
				bossExecutorThreadFactory = new ExecutorThreadFactory("EpollServerBoss", true);
				bossGroup = new EpollEventLoopGroup(1, (ThreadFactory)bossExecutorThreadFactory);
				workerExecutorThreadFactory = new ExecutorThreadFactory("EpollServerWorker", true);
				workerGroup = new EpollEventLoopGroup(workerThreads, (ThreadFactory)workerExecutorThreadFactory);
				channelType = EpollServerSocketChannel.class;
				uri.append("epoll");
			} else {
				bossExecutorThreadFactory = new ExecutorThreadFactory("NioServerBoss", true);
				bossGroup = new NioEventLoopGroup(1, bossExecutorThreadFactory);
				workerExecutorThreadFactory = new ExecutorThreadFactory("NioServerWorker", true);
				workerGroup = new NioEventLoopGroup(workerThreads, workerExecutorThreadFactory);
				channelType = NioServerSocketChannel.class;
				uri.append("nio");
			}
			serverBootstrap.channel(channelType).group(bossGroup, workerGroup);
		} else {
			bossExecutorThreadFactory = null;
			bossGroup = null;
			workerExecutorThreadFactory = new ExecutorThreadFactory("OioServer", true);
			workerGroup = new OioEventLoopGroup(workerThreads, workerExecutorThreadFactory); // workerThreads == maxChannels. see ThreadPerChannelEventLoopGroup
			channelType = OioServerSocketChannel.class;
			serverBootstrap.channel(channelType).group(workerGroup);
			uri.append("oio");
		}
		uri.append("://").append(bindInterface).append(":").append(port);
		URI u = null;
		try {
			u = new URI(uri.toString());
		} catch (URISyntaxException e) {
			log.warn("Failed server URI const: [{}]. Programmer Error", uri, e);
		}
		serverURI = u;
		if(IS_LINUX && config.hasProperty("tsd.network.unixsocket.path")) {
			unixDomainSocketServer = new UnixDomainSocketServer(tsdb);
		} else {
			unixDomainSocketServer = null;
		}
 
	}
	
	/**
	 * Starts the tcp server
	 * @throws Exception thrown if the server fails to bind to the requested port
	 */
	public void start() throws Exception {
		try {
			serverChannel = serverBootstrap.bind(bindSocket).sync().channel();
			log.info("Started [{}] TCP server listening on [{}]", channelType.getSimpleName(), bindSocket);
		} catch (Exception ex) {
			log.error("Failed to bind to [{}]", bindSocket, ex);
			throw ex;
		}
	}
	
	public void stop() {
		log.info("Stopping TSDServer....");
		try {
			serverChannel.close().sync();
			log.info("TSDServer Server Channel Closed");
		} catch (Exception x) {/* No Op */}
		try { bossGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
		try { workerGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
		if(unixDomainSocketServer!=null) unixDomainSocketServer.stop();
		log.info("TSDServer Shut Down");
	}
	

	/**
	 * <p>Title: ExecutorThreadFactory</p>
	 * <p>Description: Combines an executor and thread factory</p> 
	 * <p><code>net.opentsdb.tools.TSDTCPServer.ExecutorThreadFactory</code></p>
	 */
	public static class ExecutorThreadFactory implements Executor, ThreadFactory {
		final Executor executor;
		final ThreadFactory threadFactory;
		final String name;
		final AtomicInteger serial = new AtomicInteger();
		
		ExecutorThreadFactory(final String name, final boolean daemon) {
			this.name = name;
			threadFactory = Threads.newThreadFactory(name, daemon, Thread.NORM_PRIORITY);
			executor = Executors.newCachedThreadPool(threadFactory);
		}

		/**
		 * Executes the passed runnable in the executor
		 * @param command The runnable to execute
		 * @see java.util.concurrent.Executor#execute(java.lang.Runnable)
		 */
		@Override
		public void execute(final Runnable command) {
			executor.execute(command);
		}
		
		/**
		 * Creates a new thread
		 * {@inheritDoc}
		 * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
		 */
		@Override
		public Thread newThread(final Runnable r) {
			return threadFactory.newThread(r);
		}
	}
	

}
