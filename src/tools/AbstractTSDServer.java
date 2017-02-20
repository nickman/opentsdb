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
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.epoll.Epoll;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: AbstractTSDServer</p>
 * <p>Description: The base TSD server</p> 
 * <p><code>net.opentsdb.tools.AbstractTSDServer</code></p>
 */

public abstract class AbstractTSDServer {
	/** A map of TSD servers keyed by the protocol enum */
	private static final Map<TSDProtocol, AbstractTSDServer> instances = Collections.synchronizedMap(new EnumMap<TSDProtocol, AbstractTSDServer>(TSDProtocol.class));
	
	/** Indicates if we're on linux in which case, async will use epoll */
	public static final boolean EPOLL = Epoll.isAvailable();
	/** The number of core available to this JVM */
	public static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
	
	/** The instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** Atomic flag indicating if the TSDServer is started */
	protected final AtomicBoolean started = new AtomicBoolean(false);
	
	/** The channel initializer for this TSD server */
	protected final ChannelInitializer<Channel> channelInitializer;
	/** The TSDB instance this TSD server is fronting */
	protected final TSDB tsdb;
	/** The TSDProtocol for this instance */
	protected final TSDProtocol protocol;
	/** The Netty ByteBuf manager */
	final BufferManager bufferManager;
	
	
	/**
	 * Creates and initializes the TSDServer
	 * @param tsdb The parent TSDB instance
	 * @param protocol The TSD server protocol
	 * @return the initialized TSDServer
	 */
	static AbstractTSDServer getInstance(final TSDB tsdb, final TSDProtocol protocol) {
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
	@SuppressWarnings("unchecked")
	protected AbstractTSDServer(final TSDB tsdb, final TSDProtocol protocol) {
		if(tsdb==null) throw new IllegalArgumentException("The passed TSDB was null");
		if(protocol==null) throw new IllegalArgumentException("The passed TSDProtocol was null");
		final Config config = tsdb.getConfig();
		bufferManager = BufferManager.getInstance(config);
		this.protocol = protocol;
		this.tsdb = tsdb;
		channelInitializer = (ChannelInitializer<Channel>)protocol.initializer(tsdb);
	}
	
	
	
	
	
	
	
}
