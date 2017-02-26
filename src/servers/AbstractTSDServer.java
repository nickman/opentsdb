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

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.epoll.Epoll;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: AbstractTSDServer</p>
 * <p>Description: The base TSD server</p> 
 * <p><code>net.opentsdb.servers.AbstractTSDServer</code></p>
 */

public abstract class AbstractTSDServer implements AbstractTSDServerMBean {
	/** A map of TSD servers keyed by the protocol enum */
	private static final Map<TSDProtocol, AbstractTSDServer> instances = Collections.synchronizedMap(new EnumMap<TSDProtocol, AbstractTSDServer>(TSDProtocol.class));
	
	/** Indicates if we're on linux in which case, async will use epoll */
	public static final boolean EPOLL = Epoll.isAvailable();
	/** The number of core available to this JVM */
	public static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
	
	/** The template for TSD server JMX ObjectNames */
	public static final String OBJECT_NAME = "net.opentsdb.servers:service=TSDServer,protocol=%s";
	
	/** The config key for the protocols to enable */
	public static final String PROTOCOL_CONFIG = "tsd.network.protocols";
	
	/** The instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** Atomic flag indicating if the TSDServer is started */
	protected final AtomicBoolean started = new AtomicBoolean(false);
	/** The server exception monitor */
	protected final TSDServerExceptionMonitor exceptionMonitor = new TSDServerExceptionMonitor(getClass());
	
	/** The TSDB configuration */
	protected final Config config;
	
	/** The channel initializer for this TSD server */
	protected ChannelInitializer<Channel> channelInitializer = null;
	/** The TSDB instance this TSD server is fronting */
	protected final TSDB tsdb;
	/** The TSDProtocol for this instance */
	protected final TSDProtocol protocol;
	/** The Netty ByteBuf manager */
	protected final BufferManager bufferManager;
	
	
	/**
	 * Starts all configured TSD servers
	 * @param tsdb The parent TSDB instance
	 */
	public static void startTSDServers(final TSDB tsdb) {
		if(tsdb==null) throw new IllegalArgumentException("The passed TSDB was null");
		String protocols = null;
		final Config config = tsdb.getConfig();
		if(config.hasProperty(PROTOCOL_CONFIG)) {
			protocols = config.getString(PROTOCOL_CONFIG);
		}
		final boolean disableEpoll = config.getBoolean("tsd.network.epoll.disable", true);
		final boolean epollAvailable = !disableEpoll && EPOLL;
		final TSDProtocol[] tps = TSDProtocol.enabledServers(protocols);
		for(TSDProtocol tp: tps) {
			if(tp.isSupported(epollAvailable)) {
				try {
					getInstance(tsdb, tp).start();
				} catch (Exception ex) {
					stopTSDServers();
					throw new IllegalArgumentException(ex);
				}
			}
		}
	}
	
	/**
	 * Stops all running TSD servers
	 */
	public static Deferred<Object> stopTSDServers() {
		final Deferred<Object> def = new Deferred<Object>();
		final Thread stopThread = new Thread("TSDServerStopThread") {
			public void run() {
				for(AbstractTSDServer server: instances.values()) {
					server.stop();
				}
				instances.clear();
				def.callback(null);
			}
		};
		stopThread.setDaemon(true);
		stopThread.start();
		return def;
	}
	
	/**
	 * Collects stats on all TSD servers
	 * @param collector The stats collector
	 */
	public static void collectTSDServerStats(final StatsCollector collector) {
		for(AbstractTSDServer server: instances.values()) {
			server.collectStats(collector);
		}
	}
	
	/**
	 * Creates and initializes the TSDServer
	 * @param tsdb The parent TSDB instance
	 * @param protocol The TSD server protocol
	 * @return the initialized TSDServer
	 */
	public static AbstractTSDServer getInstance(final TSDB tsdb, final TSDProtocol protocol) {
		if(tsdb==null) throw new IllegalArgumentException("The passed TSDB was null");
		if(protocol==null) throw new IllegalArgumentException("The passed TSDProtocol was null");		
		AbstractTSDServer server = instances.get(protocol);
		if(server==null) {
			synchronized(instances) {
				server = instances.get(protocol);
				if(server==null) {
					server = protocol.buildServer(tsdb);
					instances.put(protocol, server);
				}
			}
		}
		return server;
	}
	
	/**
	 * Acquires the already initialized TSD server instance
	 * @param protocol The TSD server protocol
	 * @return the TSD server instance
	 */
	public static AbstractTSDServer getInstance(final TSDProtocol protocol) {
		if(protocol==null) throw new IllegalArgumentException("The passed TSDProtocol was null");
		final AbstractTSDServer server = instances.get(protocol);
		if(server==null) {
			throw new IllegalStateException("The [" + protocol.name() + "] TSDServer has not been initialized");
		}
		return server;
	}
	
	
	/**
	 * Creates a new AbstractTSDServer
	 * @param tsdb The TSDB instance this TSD server is fronting
	 * @param channelInitializer The channel initializer for this TSD server
	 */	
	protected AbstractTSDServer(final TSDB tsdb, final TSDProtocol protocol) {
		if(tsdb==null) throw new IllegalArgumentException("The passed TSDB was null");
		if(protocol==null) throw new IllegalArgumentException("The passed TSDProtocol was null");
		config = tsdb.getConfig();
		bufferManager = BufferManager.getInstance(config);
		this.protocol = protocol;
		this.tsdb = tsdb;		
	}
	
	protected void registerMBean() {
		try {
			final ObjectName on = new ObjectName(String.format(OBJECT_NAME, protocol.name()));
			ManagementFactory.getPlatformMBeanServer().registerMBean(this, on);
		} catch (Exception ex) {
			log.warn("Failed to register JMX MBean. Continuing without.", ex);
		}
	}
	
	protected void unregisterMBean() {
		try {
			final ObjectName on = new ObjectName(String.format(OBJECT_NAME, protocol.name()));
			ManagementFactory.getPlatformMBeanServer().unregisterMBean(on);
		} catch (Exception x) { /* No Op */}
	}
	

	  /**
	   * Collects the stats and metrics tracked by this instance.
	   * @param collector The collector to use.
	   */
	  public void collectStats(final StatsCollector collector) {
		  /* Empty */
	  }
	
	/**
	 * Starts the tcp server
	 * @throws Exception thrown if the server fails to bind to the requested port
	 */
	public abstract void start() throws Exception;
	
	/**
	 * Stops this TSDServer
	 */
	public abstract void stop();

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractTSDServerMBean#isStarted()
	 */
	@Override
	public boolean isStarted() {
		return started.get();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractTSDServerMBean#getChannelInitializer()
	 */
	@Override
	public String getChannelInitializer() {
		return channelInitializer.getClass().getName();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractTSDServerMBean#getProtocol()
	 */
	@Override
	public String getProtocol() {
		return protocol.name();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractTSDServerMBean#getTotalExceptionCount()
	 */
	@Override
	public long getTotalExceptionCount() {
		return exceptionMonitor.getTotalExceptionCount();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractTSDServerMBean#exceptionsByType()
	 */
	@Override
	public Map<String, Long> exceptionsByType() {
		return exceptionMonitor.getExceptionsByType();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.servers.AbstractTSDServerMBean#resetExceptionCounts()
	 */
	@Override
	public void resetExceptionCounts() {
		exceptionMonitor.resetCounts();
	}
	
	
	
	
	
	
	
}
