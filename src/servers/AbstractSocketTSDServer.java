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

import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.unix.DomainSocketReadMode;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.UDPPacketHandler;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: AbstractSocketTSDServer</p>
 * <p>Description: Base class for socket based TSD servers</p> 
 * <p><code>net.opentsdb.tools.AbstractSocketTSDServer</code></p>
 */

public abstract class AbstractSocketTSDServer extends AbstractTSDServer implements AbstractSocketTSDServerMBean {
	/** The socket address that the listener will be bound to */
	protected final SocketAddress bindSocket;	
	/** Indicates if we're using asynchronous net io */
	protected final boolean async;
	/** Indicates if epoll has been disabled even if we're on linux and using asynchronous net io */
	protected final boolean disableEpoll;
	/** The maximum number of connections; Zero means unlimited */
	protected int maxConnections;
	/** The maximum idle time in seconds; Zero means unlimited */
	protected  long maxIdleTime;
	
	/** The channel group to track all connections */
	protected final ChannelGroup channelGroup;
	/** The channel group event executor */
	protected final EventExecutor channelGroupExecutor;
	
	// =============================================
	// Channel Configs
	// =============================================
	/** The size of the server socket's backlog queue */
	protected final int backlog;
	/** Indicates if reuse address should be enabled */
	protected final boolean reuseAddress;
	/** The server's connect timeout in ms */
	protected final int connectTimeout;
	/** The channel config, set after start */
	protected ChannelConfig cfg = null;
	
	
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
	
	/** The netty bootstrap */
	@SuppressWarnings("rawtypes")
	protected final AbstractBootstrap bootstrap;
	/** The configured number of worker threads */
	protected final int workerThreads;
	
	/** The channel type this server will create */
	protected final Class<? extends Channel> channelType;
	
	/** The netty boss event loop group */
	protected final EventLoopGroup bossGroup;
	/** The netty worker event loop group */
	protected final EventLoopGroup workerGroup;
	
	/** The channel event monitor for handling max connections, idle connections and event counts */
	protected final TSDServerConnectionMonitor eventMonitor;	
	/** An epoll tcp monitor, instantiated if protocol is TCP and epoll is true */
	protected final EPollMonitor epollMonitor;
	/** The server channel created on socket bind */
	protected Channel serverChannel = null;
	
	/** If we get this on the unixsocket server, we shutdown */
	public final ByteBuf STOP_CODE;
	/** The number of bytes in the stop code */
	public final int STOP_CODE_SIZE;
	
	/** The charset for telnet decoding */
	public static final Charset ISO8859 = Charset.forName("ISO-8859-1");
	
	@ChannelHandler.Sharable
	class StopCodeListener extends ChannelDuplexHandler {
		
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object payload) throws Exception {
			if(payload!=null && (payload instanceof ByteBuf)) {
				final ByteBuf msg = (ByteBuf)payload;
				if(msg.readableBytes()>=STOP_CODE_SIZE) {
					if(msg.slice(0, STOP_CODE_SIZE).equals(STOP_CODE)) {
						try {
							log.info("\n\t==========================\n\tReceived StopCode. Stopping OpenTSDB...\n\t==========================\n");
							tsdb.shutdown();
							return;
						} catch (Exception ex) {
							log.error("Failed to call for TSDServer Stop", ex);
							return;
						}
					}
				}
				
			}
			super.channelRead(ctx, payload);
		}
	}

	/**
	 * Creates a new AbstractSocketTSDServer
	 * @param tsdb The parent TSDB instance
	 * @param protocol The protocol implemented by this server
	 */
	@SuppressWarnings("unchecked")
	protected AbstractSocketTSDServer(final TSDB tsdb, final TSDProtocol protocol) {
		super(tsdb, protocol);		
		bootstrap = protocol.buildBootstrap(config.getLogLevel("tsd.network.loglevel"));		
		bindSocket = protocol.socketAddress(config);
		disableEpoll = config.getBoolean("tsd.network.epoll.disable", true); 
		async = config.getBoolean("tsd.network.async_io", true);
		maxConnections = config.getInt("tsd.core.connections.limit", 0);
		maxIdleTime = config.getLong("tsd.core.socket.timeout", 0L);
		tcpNoDelay = config.getBoolean("tsd.network.tcp_no_delay", true);
		keepAlive = config.getBoolean("tsd.network.keep_alive", true);
		writeSpins = config.getInt("tsd.network.writespins", 16);
		recvBuffer = config.getInt("tsd.network.recbuffer", 43690);
		sendBuffer = config.getInt("tsd.network.sendbuffer", 8192);
		workerThreads = config.getInt("tsd.network.worker_threads", CORES * 2);		
		connectTimeout = config.getInt("tsd.network.sotimeout", 0);
		backlog = config.getInt("tsd.network.backlog", 3072);
		reuseAddress = config.getBoolean("tsd.network.reuse_address", true);
		if(config.hasProperty("tsd.network.unixsocket.stopcode")) {			
			STOP_CODE = BufferManager.getInstance().wrap(config.getString("tsd.network.unixsocket.stopcode").trim(), ISO8859).asReadOnly();
			STOP_CODE_SIZE = STOP_CODE.readableBytes();			
		} else {
			STOP_CODE = null;
			STOP_CODE_SIZE = -1;
		}
		
		
		final boolean epoll = !disableEpoll && EPOLL;
		
		bossGroup = protocol.buildBossGroup(async, epoll, config);
		workerGroup = protocol.buildWorkerGroup(async, epoll, config);
		channelType = protocol.channelClass(async, epoll, config);
		bootstrap.channel(channelType);
		
		
		bootstrap.option(ChannelOption.ALLOCATOR, bufferManager);			
		bootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);
		if(protocol.connectionServer) {
			bootstrap.option(ChannelOption.SO_BACKLOG, backlog);
			bootstrap.option(ChannelOption.SO_TIMEOUT, connectTimeout);
		}
		if(protocol.connectionServer) {			
			final ServerBootstrap serverBootstrap = (ServerBootstrap)bootstrap;
			// Set the child options
			if(protocol!=TSDProtocol.UNIX) {
				serverBootstrap.childOption(ChannelOption.TCP_NODELAY, tcpNoDelay);
				serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, keepAlive);
				serverBootstrap.childOption(ChannelOption.SO_RCVBUF, recvBuffer);
				serverBootstrap.childOption(ChannelOption.SO_SNDBUF, sendBuffer);				
			} else {
				// TODO: configurable
				serverBootstrap.childOption(EpollChannelOption.DOMAIN_SOCKET_READ_MODE, DomainSocketReadMode.BYTES);
				serverBootstrap.childOption(EpollChannelOption.EPOLL_MODE, EpollMode.EDGE_TRIGGERED);
				/*
				 * Not sure why, but this causes
				 * [UNIX-EpollBoss#0] ServerBootstrap: Unknown channel option: SO_RCVBUF=43690
				 * [UNIX-EpollBoss#0] ServerBootstrap: Unknown channel option: SO_SNDBUF=8192
				 */
//				serverBootstrap.childOption(EpollChannelOption.SO_RCVBUF, recvBuffer);
//				serverBootstrap.childOption(EpollChannelOption.SO_SNDBUF, sendBuffer);
			}
			serverBootstrap.childOption(ChannelOption.ALLOCATOR, bufferManager);			
			serverBootstrap.childOption(ChannelOption.WRITE_SPIN_COUNT, writeSpins);			
			channelGroupExecutor = new DefaultEventExecutor((Executor)new ExecutorThreadFactory(protocol.name() + "-TSDServerChannelGroup-%d", true));
			channelGroup = new DefaultChannelGroup("TSDServerSocketConnections", channelGroupExecutor);
			eventMonitor = new TSDServerConnectionMonitor(channelGroup, maxConnections, maxIdleTime);
			if(async && bossGroup!=null) {
				serverBootstrap.group(bossGroup, workerGroup);
			} else {
				serverBootstrap.group(workerGroup);
			}
			final ChannelHandler[] handlers = new ChannelHandler[]{eventMonitor, null};
			if(STOP_CODE!=null) handlers[1] = new StopCodeListener();
			channelInitializer = protocol.initializer(tsdb, handlers, new ChannelHandler[]{exceptionMonitor});
			serverBootstrap.childHandler(channelInitializer);
			//serverBootstrap.handler(eventMonitor);
		} else {
			channelGroupExecutor = null;
			channelGroup = null;
			eventMonitor = null;
			bootstrap.group(workerGroup);
			channelInitializer = protocol.initializer(tsdb, null, new ChannelHandler[]{exceptionMonitor});
			bootstrap.handler(channelInitializer);
		}
		if(epoll && protocol==TSDProtocol.TCP) {
			epollMonitor = new EPollMonitor(channelGroup);
		} else {
			epollMonitor = null;
		}
	}
	
	/**
	 * Callback after the server has started
	 */
	protected void afterStart() {
		cfg = serverChannel.config();
		if(log.isDebugEnabled()) {			
			final Map<ChannelOption<?>,Object> options = cfg.getOptions();
			final StringBuilder b = new StringBuilder("====================" + protocol.name() + " Configuration Options ====================");
			for(Map.Entry<ChannelOption<?>,Object> entry: options.entrySet()) {
				b.append("\n\t").append(entry.getKey()).append(" : ").append(entry.getValue());
			}
			log.info(b.toString());
		}
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractTSDServer#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(final StatsCollector collector) {		
		super.collectStats(collector);	
		try {
			collector.addExtraTag("protocol", protocol.name().toLowerCase());
			if(protocol.connectionServer) {
				collector.record("server.connections.active", getActiveConnections(), null);
				collector.record("server.connections.closed", getIdleConnectionsClosed(), "cause=idle");
				collector.record("server.connections.closed", getCloseExceptions(), "cause=exception");
				collector.record("server.connections.closed", getRejectedConnections(), "cause=rejected");				
				collector.record("server.connections.established", getConnectionsEstablished(), null);
			}
		} finally {
			collector.clearExtraTag("protocol");
		}
	}
	
	
	private static final Map<String, String> EMPTY_MAP = Collections.emptyMap();
	private static volatile Method childOptionsMethod = null;
	private static volatile Method childAttrsMethod = null;
	
	private static void initChildMethods() {
		try {
			childOptionsMethod = ServerBootstrap.class.getDeclaredMethod("childOptions");
			childOptionsMethod.setAccessible(true);
			childAttrsMethod = ServerBootstrap.class.getDeclaredMethod("childAttrs");
			childAttrsMethod.setAccessible(true);			
		} catch (Exception ex) {
			childOptionsMethod = null;
			childAttrsMethod = null;
		}
	}
	
	/**
	 * Returns a map of the server child channel's configuiration options
	 * @return a map of the server child  channel's configuiration options
	 */
	@SuppressWarnings("unchecked")
	public Map<String, String> childChannelOptions() {
		final Map<ChannelOption<?>,Object> options;
		final Map<AttributeKey<?>, Object> attrs;
		if(protocol.connectionServer) {
			final ServerBootstrap sb = (ServerBootstrap)bootstrap;
			// Annoyingly, the ServerBootstrap child options and attrs
			// are protected so we have to use reflection.
			if(childOptionsMethod==null) {
				initChildMethods();
			}
			if(childOptionsMethod==null) return EMPTY_MAP;
			try {
				options = (Map<ChannelOption<?>,Object>)childOptionsMethod.invoke(sb);
				attrs = (Map<AttributeKey<?>,Object>)childAttrsMethod.invoke(sb);
			} catch (Exception ex) {
				return EMPTY_MAP;
			}
		} else {
			options = bootstrap.config().options();
			attrs = bootstrap.config().attrs();
		}
		final int size = (options==null||options.isEmpty() ? 0 : options.size()) + (attrs==null||attrs.isEmpty() ? 0 : attrs.size());
		if(size==0) return EMPTY_MAP;
		final Map<String, String> map = new LinkedHashMap<String, String>(size);
		if(attrs!=null) {
			for(Map.Entry<AttributeKey<?>,Object> entry: attrs.entrySet()) {
				final Object value = entry.getValue();			
				map.put(entry.getKey().toString(), value == null ? "<null>" : value.toString());
			}
		}
		if(options!=null) {
			for(Map.Entry<ChannelOption<?>,Object> entry: options.entrySet()) {
				final Object value = entry.getValue();			
				map.put(entry.getKey().toString(), value == null ? "<null>" : value.toString());
			}
		}
		
		return map;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#serverChannelOptions()
	 */
	@Override
	public Map<String, String> serverChannelOptions() {		
		if(cfg==null) return EMPTY_MAP;
		final Map<ChannelOption<?>,Object> options = cfg.getOptions();
		final Map<String, String> map = new LinkedHashMap<String, String>(options.size());
		for(Map.Entry<ChannelOption<?>,Object> entry: options.entrySet()) {
			final Object value = entry.getValue();			
			map.put(entry.getKey().toString(), value == null ? "<null>" : value.toString());
		}
		return map;
	}

	/**
	 * Returns the maximum number of connections or zero if unlimited
	 * @return the maximum number of connections
	 */
	public int getMaxConnections() {
		return maxConnections;
	}


	/**
	 * Returns the maximum connection idle time in ms or zero if unlimited
	 * @return the maximum connection idle time
	 */
	public long getMaxIdleTime() {
		return maxIdleTime;
	}

	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#getBindSocket()
	 */
	@Override
	public String getBindSocket() {
		return bindSocket==null ? null : bindSocket.toString();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#isAsync()
	 */
	@Override
	public boolean isAsync() {
		return async;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#isDisableEpoll()
	 */
	@Override
	public boolean isDisableEpoll() {
		return disableEpoll;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#getBacklog()
	 */
	@Override
	public int getBacklog() {
		return backlog;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#isReuseAddress()
	 */
	@Override
	public boolean isReuseAddress() {
		return reuseAddress;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#getConnectTimeout()
	 */
	@Override
	public int getConnectTimeout() {
		return connectTimeout;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#isTcpNoDelay()
	 */
	@Override
	public boolean isTcpNoDelay() {
		return tcpNoDelay;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#isKeepAlive()
	 */
	@Override
	public boolean isKeepAlive() {
		return keepAlive;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#getWriteSpins()
	 */
	@Override
	public int getWriteSpins() {
		return writeSpins;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#getRecvBuffer()
	 */
	@Override
	public int getRecvBuffer() {
		return recvBuffer;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#getSendBuffer()
	 */
	@Override
	public int getSendBuffer() {
		return sendBuffer;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#getWorkerThreads()
	 */
	@Override
	public int getWorkerThreads() {
		return workerThreads;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#getChannelType()
	 */
	@Override
	public String getChannelType() {
		return channelType.getName();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#isEpollSupported()
	 */
	@Override
	public boolean isEpollSupported() {
		return EPOLL;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractSocketTSDServerMBean#resetCounters()
	 */
	@Override
	public void resetCounters() {
		if(eventMonitor!=null) eventMonitor.resetCounters();
		
	}
	
	/**
	 * Returns the total monotonic count of established connections
	 * @return the established connections count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getConnectionsEstablished()
	 */
	public long getConnectionsEstablished() {		
		return eventMonitor==null ? -1 : eventMonitor.getConnectionsEstablished();
	}
	
	/**
	 * Returns the total number of closed idle connections
	 * @return the total number of closed idle connections
	 */
	public long getIdleConnectionsClosed() {
		return eventMonitor==null ? -1 : eventMonitor.getIdleConnectionsClosed();
	}

	/**
	 * Returns the total monotonic count of closed connections
	 * @return the closed connections count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getClosedConnections()
	 */
	public long getClosedConnections() {
		return eventMonitor==null ? -1 : eventMonitor.getClosedConnections();
	}

	/**
	 * Returns the total monotonic count of rejected connections
	 * @return the rejected connections count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getRejectedConnections()
	 */
	public long getRejectedConnections() {
		return eventMonitor==null ? -1 : eventMonitor.getRejectedConnections();
	}

	/**
	 * Returns the total monotonic count of unknown cause connection exceptions
	 * @return the unknown connection exceptions count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getUnknownExceptions()
	 */
	public long getUnknownExceptions() {
		return eventMonitor==null ? -1 : eventMonitor.getUnknownExceptions();
	}

	/**
	 * Returns the total monotonic count of connection close exceptions
	 * @return the connection close exceptions count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getCloseExceptions()
	 */
	public long getCloseExceptions() {
		return eventMonitor==null ? -1 : eventMonitor.getCloseExceptions();
	}

	/**
	 * Returns the total monotonic count of connection reset exceptions
	 * @return the connection reset exceptions count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getResetExceptions()
	 */
	public long getResetExceptions() {
		return eventMonitor==null ? -1 : eventMonitor.getResetExceptions();
	}

	/**
	 * Returns the total monotonic count of idle connection closes
	 * @return the idle connection closes count
	 * @see net.opentsdb.servers.TSDServerConnectionMonitor#getTimeoutExceptions()
	 */
	public long getTimeoutExceptions() {
		return eventMonitor==null ? -1 : eventMonitor.getTimeoutExceptions();
	}
	
	/**
	 * Returns the number of active connections
	 * @return the number of active connections
	 */
	public int getActiveConnections() {		
		return channelGroup==null ? -1 : channelGroup.size();
	}
	

}
