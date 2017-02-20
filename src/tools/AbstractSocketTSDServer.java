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

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Threads;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: AbstractSocketTSDServer</p>
 * <p>Description: Base class for socket based TSD servers</p> 
 * <p><code>net.opentsdb.tools.AbstractSocketTSDServer</code></p>
 */

public class AbstractSocketTSDServer extends AbstractTSDServer {
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
	/** The maximum number of connections; Zero means unlimited */
	protected int maxConnections;
	/** The maximum idle time in seconds; Zero means unlimited */
	protected  long maxIdleTime;
	
	/** The channel group to track all connections */
	protected final ChannelGroup channelGroup;
	/** The channel group event executor */
	protected final EventExecutor channelGroupExecutor;
	
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
	
	

	/**
	 * Creates a new AbstractSocketTSDServer
	 * @param tsdb The parent TSDB instance
	 * @param protocol The protocol implemented by this server
	 */
	public AbstractSocketTSDServer(final TSDB tsdb, final TSDProtocol protocol) {
		super(tsdb, protocol);
		final Config config = tsdb.getConfig();
		port = config.getInt("tsd.network.port");
		bindInterface = config.getString("tsd.network.bind", "0.0.0.0");
		bindSocket = new InetSocketAddress(bindInterface, port);
		disableEpoll = config.getBoolean("tsd.network.epoll.disable", true); 
		async = config.getBoolean("tsd.network.async_io", true);
		channelGroupExecutor = new DefaultEventExecutor(Threads.newThreadFactory("TSDServerChannelGroup-%d", true, Thread.NORM_PRIORITY));
		channelGroup = new DefaultChannelGroup("TSDServerSocketConnections", channelGroupExecutor);
		maxConnections = config.getInt("tsd.core.connections.limit", 0);
		maxIdleTime = config.getLong("tsd.core.socket.timeout", 0L);
		tcpNoDelay = config.getBoolean("tsd.network.tcp_no_delay", true);
		keepAlive = config.getBoolean("tsd.network.keep_alive", true);
		writeSpins = config.getInt("tsd.network.writespins", 16);
		recvBuffer = config.getInt("tsd.network.recbuffer", 43690);
		sendBuffer = config.getInt("tsd.network.sendbuffer", 8192);
	}

}
