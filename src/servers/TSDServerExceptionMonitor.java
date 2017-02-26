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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hbase.async.jsr166e.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * <p>Title: TSDServerExceptionMonitor</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.servers.TSDServerExceptionMonitor</code></p>
 */
@ChannelHandler.Sharable
public class TSDServerExceptionMonitor extends ChannelInboundHandlerAdapter {
	/** The number of available processors */
	public static final int CORES = Runtime.getRuntime().availableProcessors();
	/** A long adder placeholder */
	private static final LongAdder PLACEHOLDER = new LongAdder();
	/** An empty map const */
	private static final Map<String, Long> EMPTY_MAP = Collections.unmodifiableMap(new HashMap<String, Long>(0));
	
	/** Instance logger */
	protected final Logger log;
	/** Exception counter */
	protected final LongAdder totalExceptionCount = new LongAdder();
	/** Exceptions by cause name */
	protected final ConcurrentHashMap<String, LongAdder> exceptionsByType = new ConcurrentHashMap<String, LongAdder>(128, 0.75f, CORES); 

	/**
	 * Creates a new TSDServerExceptionMonitor
	 */
	TSDServerExceptionMonitor(final Class<?> serverType) {
		log = LoggerFactory.getLogger(serverType);
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#exceptionCaught(io.netty.channel.ChannelHandlerContext, java.lang.Throwable)
	 */
	@Override
	public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
		totalExceptionCount.increment();
	    final String key = cause.getClass().getName();
	    LongAdder adder = exceptionsByType.putIfAbsent(key, PLACEHOLDER);
	    if(adder==null || adder==PLACEHOLDER) {
	    	adder = new LongAdder();
	    	exceptionsByType.replace(key, PLACEHOLDER, adder);
	    }
	    adder.increment();
	    log.error("Exception caught in pipeline on channel [{}]", ctx.channel(), cause);
	}
	
	long getTotalExceptionCount() {
		return totalExceptionCount.longValue();
	}
	
	Map<String, Long> getExceptionsByType() {
		if(exceptionsByType.isEmpty()) return EMPTY_MAP;
		final Map<String, Long> map = new HashMap<String, Long>(exceptionsByType.size());
		for(Map.Entry<String, LongAdder> entry: exceptionsByType.entrySet()) {
			map.put(entry.getKey(), entry.getValue().longValue());
		}
		return map;
	}
	
	void resetCounts() {
		totalExceptionCount.reset();
		for(LongAdder adder: exceptionsByType.values()) {
			adder.reset();
		}
	}

}
