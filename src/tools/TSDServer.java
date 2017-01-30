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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.oio.OioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.PipelineFactory;
import net.opentsdb.tsd.UnixDomainSocketServer;
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

public class TSDServer implements TSDServerMBean {
	/** The singleton instance */
	private static volatile TSDServer instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** Indicates if we're on linux in which case, async will use epoll */
	public static final boolean EPOLL = Epoll.isAvailable();
	/** The number of core available to this JVM */
	public static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
	
	/** The instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** Atomic flag indicating if the TSDServer is started */
	protected final AtomicBoolean started = new AtomicBoolean(false);
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
	/** The TSDServer JMX ObjectName */
	public final ObjectName objectName;
	/** The channel group to track all connections */
	protected final ChannelGroup channelGroup;
	/** The channel group event executor */
	protected final EventExecutor channelGroupExecutor;
	/** The epoll monitor */
	protected final EPollMonitor epollMonitor;
	

	
	/**
	 * Creates and initializes the TSDServer
	 * @param tsdb The parent TSDB instance
	 * @param pipelineFactory The channel pipeline initializer
	 * @return the initialized TSDServer
	 */
	static TSDServer getInstance(final TSDB tsdb, final PipelineFactory pipelineFactory) {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new TSDServer(tsdb, pipelineFactory);
				}
			}
		}
		return instance;
	}
	
	/**
	 * Acquires the already initialized TSDServer instance
	 * @return
	 */
	public static TSDServer getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					throw new IllegalStateException("The TSDServer has not been initialized");
				}
			}
		}
		return instance;
	}
	
	class GroupTracker extends ChannelInitializer<Channel> {
		final PipelineFactory delegate;
		GroupTracker(final PipelineFactory delegate) {
			this.delegate = delegate;
		}
		@Override
		protected void initChannel(final Channel ch) throws Exception {
			delegate.initChannel(ch);
			channelGroup.add(ch);			
		}		
	}

	/**
	 * Creates a new TSDServer
	 * @param tsdb The parent TSDB instance
	 * @param pipelineFactory The channel pipeline initializer
	 */
	private TSDServer(final TSDB tsdb, final PipelineFactory pipelineFactory) {
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
		final LogLevel logLevel = config.getLogLevel("tsd.network.loglevel");
		if(logLevel!=null) {
			serverBootstrap.handler(new LoggingHandler(getClass(), logLevel));
		}
		final GroupTracker gp = new GroupTracker(pipelineFactory);
		serverBootstrap.childHandler(gp);
		
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
		serverBootstrap.option(ChannelOption.SO_TIMEOUT, connectTimeout);
		
		// Fire up the channel group 
		channelGroupExecutor = new DefaultEventExecutor(Threads.newThreadFactory("TSDServerChannelGroup-%d", true, Thread.NORM_PRIORITY));
		channelGroup = new DefaultChannelGroup("TSDServerSocketConnections", channelGroupExecutor);
		//serverBootstrap.config().attrs()
		final StringBuilder uri = new StringBuilder("tcp");
		if(async) {
			if(EPOLL && !disableEpoll) {
				bossExecutorThreadFactory = new ExecutorThreadFactory("EpollServerBoss#%d", true);
				bossGroup = new EpollEventLoopGroup(1, (ThreadFactory)bossExecutorThreadFactory);
				workerExecutorThreadFactory = new ExecutorThreadFactory("EpollServerWorker#%d", true);
				workerGroup = new EpollEventLoopGroup(workerThreads, (ThreadFactory)workerExecutorThreadFactory);
				channelType = EpollServerSocketChannel.class;
				uri.append("epoll");
				epollMonitor = new EPollMonitor("tcp", channelGroup);
			} else {
				bossExecutorThreadFactory = new ExecutorThreadFactory("NioServerBoss#%d", true);
				bossGroup = new NioEventLoopGroup(1, bossExecutorThreadFactory);
				workerExecutorThreadFactory = new ExecutorThreadFactory("NioServerWorker#%d", true);
				workerGroup = new NioEventLoopGroup(workerThreads, workerExecutorThreadFactory);
				channelType = NioServerSocketChannel.class;
				uri.append("nio");
				epollMonitor = null;
			}
			serverBootstrap.channel(channelType).group(bossGroup, workerGroup);
			
		} else {
			epollMonitor = null;
			bossExecutorThreadFactory = null;
			bossGroup = null;
			workerExecutorThreadFactory = new ExecutorThreadFactory("OioServerWorker#%d", true);
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
		ObjectName tmp = null;
		try {
			tmp = new ObjectName(OBJECT_NAME);
			ManagementFactory.getPlatformMBeanServer().registerMBean(this, tmp);
		} catch (Exception e) {
			tmp = null;
			log.warn("Failed to register TSDServer management interface", e);
		}
		objectName = tmp;
		serverURI = u;
		if(EPOLL && config.hasProperty("tsd.network.unixsocket.path")) {
			unixDomainSocketServer = new UnixDomainSocketServer(tsdb, pipelineFactory);
			
		} else {
			unixDomainSocketServer = null;
		}
 
	}
	
	/**
	 * Starts the tcp server
	 * @throws Exception thrown if the server fails to bind to the requested port
	 */
	public void start() throws Exception {
		if(started.compareAndSet(false, true)) {
			try {
				serverChannel = serverBootstrap.bind(bindSocket).sync().channel();
				log.info("Started [{}] TCP server listening on [{}]", channelType.getSimpleName(), bindSocket);
				if(unixDomainSocketServer!=null) {
					unixDomainSocketServer.start();
				}
			} catch (Exception ex) {
				log.error("Failed to bind to [{}]", bindSocket, ex);
				throw ex;
			} finally {
				started.set(false);
			}
		}
	}
	
	/**
	 * Stops the TSDServer
	 */
	public void stop() {
		if(started.compareAndSet(true, false)) {
			log.info("Stopping TSDServer....");
			
			try {
				serverChannel.close().sync();
				log.info("TSDServer Server Channel Closed");
			} catch (Exception x) {/* No Op */}
			try { bossGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
			try { workerGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
			try { channelGroupExecutor.shutdownGracefully(); } catch (Exception x) {/* No Op */}			
			if(unixDomainSocketServer!=null) unixDomainSocketServer.stop();
			log.info("TSDServer Shut Down");
		}
	}
	
	  /**
	   * Collects the stats and metrics tracked by this instance.
	   * @param collector The collector to use.
	   */
	  public void collectStats(final StatsCollector collector) {
		  collector.record("tsdserver.connections", channelGroup.size());
		  if(epollMonitor!=null) {
			  epollMonitor.collectStats(collector);
		  }
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

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#isStarted()
	 */
	@Override
	public boolean isStarted() {		
		return started.get();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#getPort()
	 */
	public int getPort() {
		return port;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#getBindInterface()
	 */
	public String getBindInterface() {
		return bindInterface;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#getBindSocket()
	 */
	public String getBindSocket() {
		return bindSocket==null ? null : bindSocket.toString();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#isAsync()
	 */
	public boolean isAsync() {
		return async;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#isDisableEpoll()
	 */
	public boolean isDisableEpoll() {
		return disableEpoll;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#isEpollSupported()
	 */
	@Override
	public boolean isEpollSupported() {		
		return EPOLL;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#getWorkerThreads()
	 */
	public int getWorkerThreads() {
		return workerThreads;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#getChannelType()
	 */
	public String getChannelType() {
		return channelType==null ? null : channelType.getName();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#getBacklog()
	 */
	public int getBacklog() {
		return backlog;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#getConnectTimeout()
	 */
	public int getConnectTimeout() {
		return connectTimeout;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#isTcpNoDelay()
	 */
	public boolean isTcpNoDelay() {
		return tcpNoDelay;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#isKeepAlive()
	 */
	public boolean isKeepAlive() {
		return keepAlive;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#getWriteSpins()
	 */
	public int getWriteSpins() {
		return writeSpins;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#getRecvBuffer()
	 */
	public int getRecvBuffer() {
		return recvBuffer;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#getSendBuffer()
	 */
	public int getSendBuffer() {
		return sendBuffer;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tools.TSDServerMBean#getServerURI()
	 */
	public String getServerURI() {
		return serverURI==null ? null : serverURI.toString();
	}


}
