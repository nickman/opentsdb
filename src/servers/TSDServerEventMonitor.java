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

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.CodecException;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import net.opentsdb.tools.ConnectionRefusedException;


/**
 * <p>Title: TSDServerEventMonitor</p>
 * <p>Description: Inbound and outbound handler to monitor and control events in client channel pipelines</p> 
 * <p><code>net.opentsdb.tools.TSDServerEventMonitor</code></p>
 */


/*
Inbound event propagation methods:
	ChannelHandlerContext.fireChannelRegistered()
	ChannelHandlerContext.fireChannelActive()
	ChannelHandlerContext.fireChannelRead(Object)
	ChannelHandlerContext.fireChannelReadComplete()
	ChannelHandlerContext.fireExceptionCaught(Throwable)
	ChannelHandlerContext.fireUserEventTriggered(Object)
	ChannelHandlerContext.fireChannelWritabilityChanged()
	ChannelHandlerContext.fireChannelInactive()
	ChannelHandlerContext.fireChannelUnregistered()
Outbound event propagation methods:
	ChannelHandlerContext.bind(SocketAddress, ChannelPromise)
	ChannelHandlerContext.connect(SocketAddress, SocketAddress, ChannelPromise)
	ChannelHandlerContext.write(Object, ChannelPromise)
	ChannelHandlerContext.flush()
	ChannelHandlerContext.read()
	ChannelHandlerContext.disconnect(ChannelPromise)
	ChannelHandlerContext.close(ChannelPromise)
	ChannelHandlerContext.deregister(ChannelPromise)
 */

@ChannelHandler.Sharable
public class TSDServerEventMonitor extends ChannelDuplexHandler implements ChannelFutureListener {
	/** The maximum number of connections allowed, or zero for unlimited */
	protected final int maxConnections;
	/** The max idle time in seconds */
	protected final long allIdleTime;
	/** The channel group we're tracking */
	protected final ChannelGroup channelGroup;
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());	
	
	/** The monotinic counter of the total number of successful connection events */
	protected final AtomicLong connections_established = new AtomicLong();
	/** The monotinic counter of the total number of connection closed events */
	protected final AtomicLong connections_closed = new AtomicLong();	
	/** The monotinic counter of the total number of rejected connection events */
	protected final AtomicLong connections_rejected = new AtomicLong();
	/** The monotinic counter of the total number of unknown connection (channel) exceptions */
	protected final AtomicLong exceptions_unknown = new AtomicLong();
	/** The monotinic counter of the total number of connection closed events */
	protected final AtomicLong exceptions_closed = new AtomicLong();
	/** The monotinic counter of the total number of successful reset events */
	protected final AtomicLong exceptions_reset = new AtomicLong();
	/** The monotinic counter of the total number of connection timeout events */
	protected final AtomicLong exceptions_timeout = new AtomicLong();
	
	
	/**
	 * Creates a new TSDServerEventMonitor
	 * @param channelGroup The channel group containing the channels being monitored
	 * @param maxConnections The maximum number of active connections
	 * @param allIdleTime The idle time after which a channel is closed
	 */
	public TSDServerEventMonitor(final ChannelGroup channelGroup, final int maxConnections, final long allIdleTime) {
		this.maxConnections = maxConnections;
		this.channelGroup = channelGroup;
		this.allIdleTime = allIdleTime;
	}
	
	/**
	 * Resets the connection and exception counters
	 */
	public void resetCounters() {
		connections_established.set(0L);
		connections_closed.set(0L);	
		connections_rejected.set(0L);
		exceptions_unknown.set(0L);
		exceptions_closed.set(0L);
		exceptions_reset.set(0L);
		exceptions_timeout.set(0L);
	}
	
	
	
	@Override
	public void operationComplete(final ChannelFuture future) throws Exception {		
		connections_closed.incrementAndGet();	
//		new Exception().printStackTrace(System.err);
	}
	
	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		if(channelGroup.add(ctx.channel())) {
			if(allIdleTime > 0) {
				ctx.pipeline().addFirst("IdleConnectionHandler", new IdleStateHandler(0, 0, (int)allIdleTime));
			}
			
			final int connectionCount = channelGroup.size();
			if(connectionCount == maxConnections) {
				connections_rejected.incrementAndGet();
				log.warn("Connection rejected due to max connection count at {}", connectionCount);
				ctx.channel().close();
				return;
			}
			connections_established.incrementAndGet();
			ctx.channel().closeFuture().addListener(this);
		}
		super.channelActive(ctx);
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelDuplexHandler#connect(io.netty.channel.ChannelHandlerContext, java.net.SocketAddress, java.net.SocketAddress, io.netty.channel.ChannelPromise)
	 */
	@Override
	public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress, final SocketAddress localAddress,
			final ChannelPromise promise) throws Exception {
		if(allIdleTime > 0) {
			ctx.pipeline().addFirst("IdleConnectionHandler", new IdleStateHandler(0, 0, (int)allIdleTime));
		}
		
		final int connectionCount = channelGroup.size();
		if(connectionCount == maxConnections) {
			connections_rejected.incrementAndGet();
			log.warn("Connection rejected due to max connection count at {}", connectionCount);
			ctx.channel().close();
			return;
		}
		connections_established.incrementAndGet();
		channelGroup.add(ctx.channel());
		super.connect(ctx, remoteAddress, localAddress, promise);
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelDuplexHandler#close(io.netty.channel.ChannelHandlerContext, io.netty.channel.ChannelPromise)
	 */
	@Override
	public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) throws Exception {
//		log.info("Read EM Close: {}", System.identityHashCode(this));
//		connections_closed.incrementAndGet();
		super.close(ctx, promise);
	}
	

	
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#userEventTriggered(io.netty.channel.ChannelHandlerContext, java.lang.Object)
	 */
	@Override
	public void userEventTriggered(final ChannelHandlerContext ctx, final Object event) throws Exception {
        if (event instanceof IdleStateEvent) {
            final IdleStateEvent evt = (IdleStateEvent) event;
    		if (evt.state() == IdleState.ALL_IDLE) {
    			final String channel_info = ctx.channel().toString();
    			log.debug("Closing idle socket: [{}]", channel_info);
    			ctx.channel().close();
    			exceptions_timeout.incrementAndGet();
    			log.info("Closed idle socket: [{}]", channel_info);			
    		}
        }
        super.userEventTriggered(ctx, event);
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#exceptionCaught(io.netty.channel.ChannelHandlerContext, java.lang.Throwable)
	 */
	@Override
	public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
	    final Channel chan = ctx.channel();
	    if (cause instanceof ClosedChannelException) {
	      exceptions_closed.incrementAndGet();
	      log.warn("Attempt to write to closed channel " + chan);
	      return;
	    }
	    if (cause instanceof IOException) {
	      final String message = cause.getMessage();
	      if ("Connection reset by peer".equals(message)) {
	        exceptions_reset.incrementAndGet();
	        return;
	      } else if ("Connection timed out".equals(message)) {
	        exceptions_timeout.incrementAndGet();
	        // Do nothing.  A client disconnecting isn't really our problem.  Oh,
	        // and I'm not kidding you, there's no better way to detect ECONNRESET
	        // in Java.  Like, people have been bitching about errno for years,
	        // and Java managed to do something *far* worse.  That's quite a feat.
	        return;
	      } else if (cause instanceof ConnectionRefusedException) {
	        connections_rejected.incrementAndGet();
//	        if (log.isDebugEnabled()) {
//	        	log.debug("Refusing connection from " + chan, cause);
//	        }
	        log.warn("Refusing connection from " + chan);
	        try { chan.close(); } catch (Exception x) {/* No Op */}
	        return;
	      }
	    }
	    if (cause instanceof CodecException) {
	    	// payload was not compressed as it was announced to be
	    	log.warn("Http codec error : " + cause.getMessage());
	    	ctx.channel().close();
	    	return;
	    }
	    exceptions_unknown.incrementAndGet();
	    log.error("Unexpected exception from downstream for " + chan, cause);
	    ctx.channel().close();		

	}
	
	
	/**
	 * Returns the total number of established connections
	 * @return the connections established
	 */
	public long getConnectionsEstablished() {
		return connections_established.get();
	}

	/**
	 * Returns the total number of closed connections
	 * @return the closed connections
	 */
	public long getClosedConnections() {
		return connections_closed.get();
	}

	/**
	 * Returns the total number of rejected connections
	 * @return the rejected connections
	 */
	public long getRejectedConnections() {
		return connections_rejected.get();
	}

	/**
	 * Returns the total number of unknown exceptions
	 * @return the unknown exceptions
	 */
	public long getUnknownExceptions() {
		return exceptions_unknown.get();
	}

	/**
	 * Returns the total number of connection closed exceptions
	 * @return the connection closed exceptions
	 */
	public long getCloseExceptions() {
		return exceptions_closed.get();
	}

	/**
	 * Returns the total number of reset connections
	 * @return the reset connections
	 */
	public long getResetExceptions() {
		return exceptions_reset.get();
	}

	/**
	 * Returns the total number of timeout exceptions
	 * @return the timeout exceptions
	 */
	public long getTimeoutExceptions() {
		return exceptions_timeout.get();
	}

}
