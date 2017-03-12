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

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.EnumSet;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.oio.OioServerSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.tsd.PipelineFactory;
import net.opentsdb.tsd.UDPPacketHandler;
import net.opentsdb.utils.Config;

/**
 * <p>Title: TSDProtocol</p>
 * <p>Description: A functional enumeration of the supported protocols for TSD server implementations</p> 
 * <p><code>net.opentsdb.tools.TSDProtocol</code></p>
 */

public enum TSDProtocol implements InitializerFactory, TSDServerBuilder {
	/** The TCP TSD server protocol */
	TCP(true, TCPTSDServer.class, new StandardChannelInitializerFactory()),
	/** The Unix Domain Socket TSD server protocol */
	UNIX(true, UNIXTSDServer.class, new UnixChannelInitializerFactory()),
	/** The UDP TSD server protocol */
	UDP(false, UDPTSDServer.class, new UDPChannelInitializerFactory());
	
	// UDPMULTICAST, MQTT, STOMP, REDIS, KAFKA, AERON, JMS
	
	private TSDProtocol(final boolean connectionServer, final Class<? extends AbstractTSDServer> tsdClass, final InitializerFactory factory) {
		this.tsdClass = tsdClass;
		this.factory = factory; 
		this.connectionServer = connectionServer;
	}
	
	/** The TSD server implementation class for this protocol */
	public final Class<? extends AbstractTSDServer> tsdClass;
	/** Indicates if the TSD server is connection based (true) or connectionless (false) */
	public final boolean connectionServer;
	/** The channel initializer factory */
	private final InitializerFactory factory;
	/** The enum values */
	private static final TSDProtocol[] values = values();
	/** Legacy boot default */
	private static final TSDProtocol[] legacyDefault = new TSDProtocol[]{TCP};
	
	/** The instance logger */
	protected static final Logger log = LoggerFactory.getLogger(TSDProtocol.class);

	
	/**
	 * Accepts the passed configuration string as comma separated TSDProtocol names
	 * and returns an array of the TSDProtocols therein. If the value <code>ALL</code>
	 * is passed in, all  TSDProtocols are returned. If the value is null, only TCP
	 * is returned.
	 * @param config The configuration string to parse
	 * @return an array of protocols
	 */
	public static TSDProtocol[] enabledServers(final String config) {
		if(config==null || config.trim().isEmpty()) return legacyDefault;
		final String uconfig = config.replace(" ", "").toUpperCase();
		if("ALL".equals(uconfig)) return values;
		final String[] names = Tags.splitString(uconfig, ',');
		final EnumSet<TSDProtocol> set = EnumSet.noneOf(TSDProtocol.class);
		for(String name: names) {
			try {
				set.add(valueOf(name));
			} catch (Exception ex) {
				throw new IllegalArgumentException("Invalid TSDProtocol: [" + name + "]");
			}
		}
		return set.toArray(new TSDProtocol[set.size()]);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.InitializerFactory#initializer(net.opentsdb.core.TSDB, io.netty.channel.ChannelHandler, io.netty.channel.ChannelHandler)
	 */
	@Override
	public ChannelInitializer<Channel> initializer(TSDB tsdb, final ChannelHandler[] first, final ChannelHandler[] last) {
		return factory.initializer(tsdb, first, last);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.TSDServerBuilder#isSupported(boolean)
	 */
	@Override
	public boolean isSupported(final boolean epoll) {
		if(this==UNIX && !epoll) return false;
		return true;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.TSDServerBuilder#buildBossGroup(boolean, boolean, net.opentsdb.utils.Config)
	 */
	@Override
	public EventLoopGroup buildBossGroup(final boolean async, final boolean epoll, final Config config) {
		if(!async || this==UDP) return null;
		final ThreadPoolExecutor threadPool = TSDBThreadPoolExecutor.newBuilder(name() + (epoll ? "-EpollBoss#%d" : "-NioBoss#%d"))
				.corePoolSize(1)					
				.build();
		final EventLoopGroup eventLoopGroup;
		if(epoll) {
			eventLoopGroup = new EpollEventLoopGroup(1, threadPool);
		} else {
			eventLoopGroup = new NioEventLoopGroup(1, threadPool);
		}
		return eventLoopGroup;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.TSDServerBuilder#buildWorkerGroup(boolean, boolean, net.opentsdb.utils.Config)
	 */
	@Override
	public EventLoopGroup buildWorkerGroup(final boolean async, final boolean epoll, final Config config) {
		final int threadCount = config.getInt("tsd.network.worker_threads", 
				Runtime.getRuntime().availableProcessors() * 2);
		final ThreadPoolExecutor threadPool = TSDBThreadPoolExecutor.newBuilder(name() + 
				(async ? epoll ? "-EpollWorker#%d" : "-NioWorker#%d" : "-OioWorker#%d"))
				.corePoolSize(1)
				.maxPoolSize(threadCount)
				.build();
		final EventLoopGroup eventLoopGroup;
		if(async) {
			if(epoll) {
				eventLoopGroup = new EpollEventLoopGroup(1, threadPool);
			} else {
				eventLoopGroup = new NioEventLoopGroup(1, threadPool);
			}
		} else {
			eventLoopGroup = new OioEventLoopGroup(threadCount, threadPool);
		}
		return eventLoopGroup;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.TSDServerBuilder#channelClass(boolean, boolean, net.opentsdb.utils.Config)
	 */
	@Override
	public Class<? extends Channel> channelClass(final boolean async, final boolean epoll, final Config config) {
		if(this==UDP) return epoll ? EpollDatagramChannel.class : NioDatagramChannel.class;
		if(this==UNIX) return EpollServerDomainSocketChannel.class;
		if(async) {
			return epoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
		} else {
			return OioServerSocketChannel.class;
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.TSDServerBuilder#buildBootstrap()
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public AbstractBootstrap buildBootstrap(final LogLevel level) {		
		if(connectionServer) {
			final ServerBootstrap b = new ServerBootstrap();
			if(level!=null) {
				b.handler(new LoggingHandler(tsdClass, level));
			}
			return b;
		} else {
			final Bootstrap b = new Bootstrap();
			b.option(ChannelOption.SO_BROADCAST, true);
			return b;
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.TSDServerBuilder#socketAddress(net.opentsdb.utils.Config)
	 */
	@Override
	public SocketAddress socketAddress(final Config config) {
		switch(this) {
			case TCP:
			case UDP:
				final int port = config.getInt("tsd.network.port", 4242);
				final String bindInterface = config.getString("tsd.network.bind", "0.0.0.0");
				return new InetSocketAddress(bindInterface, port);
			case UNIX:
				final String path = config.getString("tsd.network.unixsocket.path");
				return new DomainSocketAddress(path);
			default:
				throw new UnsupportedOperationException("No socket address factory for [" + name() + "]. Programmer Error.");		
		}
		
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.TSDServerBuilder#buildServer(net.opentsdb.core.TSDB)
	 */
	@Override
	public AbstractTSDServer buildServer(final TSDB tsdb) {
		if(tsdb==null) throw new IllegalArgumentException("The passed TSDB was null");
		try {
			final Constructor<? extends AbstractTSDServer> ctor = tsdClass.getDeclaredConstructor(TSDB.class);
			ctor.setAccessible(true);
			return ctor.newInstance(tsdb);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to create an instance of [" + tsdClass.getName() + "]", ex);
		}
	}
	
	/**
	 * <p>Title: UDPChannelInitializerFactory</p>
	 * <p>Description: Initializer for UDP TSD servers</p> 
	 * <p><code>net.opentsdb.tools.TSDProtocol.UDPChannelInitializerFactory</code></p>
	 */
	static class UDPChannelInitializerFactory implements InitializerFactory {
		private static final AtomicReference<ChannelInitializer<Channel>> instance = new AtomicReference<ChannelInitializer<Channel>>(null);
		@Override
		public ChannelInitializer<Channel> initializer(final TSDB tsdb, final ChannelHandler[] first, final ChannelHandler[] last) {
			if(tsdb==null) throw new IllegalArgumentException("The passed TSDB was null");
			if(instance.get()==null) {
				synchronized(instance) {
					if(instance.get()==null) {
						final UDPPacketHandler handler = UDPPacketHandler.getInstance(tsdb);
						final ChannelInitializer<Channel> initializer = new ChannelInitializer<Channel>() {
							@Override
							protected void initChannel(final Channel channel) throws Exception {
								final ChannelPipeline p = channel.pipeline();								
								final LogLevel level = tsdb.getConfig().getLogLevel("tsd.network.loglevel");
								if(level!=null) {
									p.addLast(new LoggingHandler(UDP.tsdClass, level));
								}																
								p.addLast(UDPPacketHandler.class.getSimpleName(), handler);								
								if(first!=null) {
									for(ChannelHandler ch: first) {
										if(ch==null) continue;
										p.addFirst(ch.getClass().getSimpleName() + "First", ch);
									}
									
								}
								if(last!=null) {
									for(ChannelHandler ch: last) {
										if(ch==null) continue;
										p.addLast(ch.getClass().getSimpleName() + "Last", ch);
									}
								}
							}
						};
						instance.set(initializer);
					}
				}
			}
			return instance.get();
		}
	}
	
	/**
	 * <p>Title: StandardChannelInitializerFactory</p>
	 * <p>Description: ChannelInitializerFactory to create the standard {@link PipelineFactory}</p> 
	 * <p><code>net.opentsdb.tools.TSDProtocol.StandardChannelInitializerFactory</code></p>
	 */
	static class StandardChannelInitializerFactory implements InitializerFactory {
		private final AtomicReference<ChannelInitializer<Channel>> instance = new AtomicReference<ChannelInitializer<Channel>>(null);		
		private final AtomicReference<PipelineFactory> pFactory = new AtomicReference<PipelineFactory>(null);
		@Override
		public ChannelInitializer<Channel> initializer(final TSDB tsdb, final ChannelHandler[] first, final ChannelHandler[] last) {
			if(tsdb==null) throw new IllegalArgumentException("The passed TSDB was null");
			if(pFactory.get()==null) {
				synchronized(pFactory) {
					if(pFactory.get()==null) {
						pFactory.set(new PipelineFactory(tsdb));
					}
				}
			}			
			if(instance.get()==null) {
				synchronized(instance) {
					if(instance.get()==null) {
						final ChannelInitializer<Channel> initializer = new ChannelInitializer<Channel>() {
							@Override
							protected void initChannel(final Channel channel) throws Exception {
								final ChannelPipeline p = channel.pipeline();
								p.addLast("pipelineFactory", pFactory.get());
								if(first!=null) {
									for(ChannelHandler ch: first) {
										if(ch==null) continue;
										p.addFirst(ch.getClass().getSimpleName() + "First", ch);
									}
									
								}
								if(last!=null) {
									for(ChannelHandler ch: last) {
										if(ch==null) continue;
										p.addLast(ch.getClass().getSimpleName() + "Last", ch);
									}
								}
								log.info("Channel {}/{} initialized. Pipeline: {}", channel, channel.getClass().getSimpleName(), p.toMap().keySet());
							}
						};
						instance.set(initializer);
					}
				}
			}
			return instance.get();
		}
	}
	
	/**
	 * <p>Title: StandardChannelInitializerFactory</p>
	 * <p>Description: ChannelInitializerFactory to create the standard {@link PipelineFactory}</p> 
	 * <p><code>net.opentsdb.tools.TSDProtocol.StandardChannelInitializerFactory</code></p>
	 */
	static class UnixChannelInitializerFactory extends StandardChannelInitializerFactory {
		
	}
}
