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

import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.ChannelInitializerFactory;
import net.opentsdb.tsd.PipelineFactory;
import net.opentsdb.tsd.TSDServerBuilder;

/**
 * <p>Title: TSDProtocol</p>
 * <p>Description: A functional enumeration of the supported protocols for TSD server implementations</p> 
 * <p><code>net.opentsdb.tools.TSDProtocol</code></p>
 */

public enum TSDProtocol implements ChannelInitializerFactory, TSDServerBuilder {
	/** The TCP TSD server protocol */
	TCP(TCPTSDServer.class, new StandardChannelInitializerFactory()),
	/** The Unix Domain Socket TSD server protocol */
	UNIX(UNIXTSDServer.class, new StandardChannelInitializerFactory());
	
	// UDP, UDPMULTICAST, MQTT, STOMP, REDIS, KAFKA, AERON, JMS
	
	private TSDProtocol(final Class<? extends AbstractTSDServer> tsdClass, final ChannelInitializerFactory factory) {
		this.tsdClass = tsdClass;
		this.factory = factory; 
	}
	
	/** The TSD server implementation class for this protocol */
	public final Class<? extends AbstractTSDServer> tsdClass;
	private final ChannelInitializerFactory factory;
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.ChannelInitializerFactory#initializer(net.opentsdb.core.TSDB)
	 */
	@Override
	public ChannelInitializer<Channel> initializer(final TSDB tsdb) {
		return factory.initializer(tsdb);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.TSDServerBuilder#buildServer(net.opentsdb.core.TSDB)
	 */
	@Override
	public AbstractTSDServer buildServer(final TSDB tsdb) {
		if(tsdb==null) throw new IllegalArgumentException("The passed TSDB was null");
		try {
			final Constructor<? extends AbstractTSDServer> ctor = tsdClass.getConstructor(TSDB.class);
			ctor.setAccessible(true);
			return ctor.newInstance(tsdb);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to create an instance of [" + tsdClass.getName() + "]", ex);
		}
	}
	
	/**
	 * <p>Title: StandardChannelInitializerFactory</p>
	 * <p>Description: ChannelInitializerFactory to create the standard {@link PipelineFactory}</p> 
	 * <p><code>net.opentsdb.tools.TSDProtocol.StandardChannelInitializerFactory</code></p>
	 */
	static class StandardChannelInitializerFactory implements ChannelInitializerFactory {
		private static final AtomicReference<ChannelInitializer<Channel>> instance = new AtomicReference<ChannelInitializer<Channel>>(null); 
		@Override
		public ChannelInitializer<Channel> initializer(final TSDB tsdb) {
			if(tsdb==null) throw new IllegalArgumentException("The passed TSDB was null");
			if(instance.get()==null) {
				synchronized(instance) {
					if(instance.get()==null) {
						instance.set(new PipelineFactory(tsdb));
					}
				}
			}
			return instance.get();
		}
	}
}
