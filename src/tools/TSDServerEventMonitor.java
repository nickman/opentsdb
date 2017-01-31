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

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.CodecException;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * <p>Title: TSDServerEventMonitor</p>
 * <p>Description: Inbound and outbound handler to monitor and control events in client channel pipelines</p> 
 * <p><code>net.opentsdb.tools.TSDServerEventMonitor</code></p>
 */

public class TSDServerEventMonitor extends IdleStateHandler {
	/** The maximum number of connections allowed, or zero for unlimited */
	protected final int maxConnections;
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
		super(0L, 0L, allIdleTime, TimeUnit.SECONDS);
		this.maxConnections = maxConnections;
	}

	
	
	/**
	 * Closes idle channels.
	 * {@inheritDoc}
	 * @see io.netty.handler.timeout.IdleStateHandler#channelIdle(io.netty.channel.ChannelHandlerContext, io.netty.handler.timeout.IdleStateEvent)
	 */
	@Override
	protected void channelIdle(final ChannelHandlerContext ctx, final IdleStateEvent evt) throws Exception {
		if (evt.state() == IdleState.ALL_IDLE) {
			final String channel_info = ctx.channel().toString();
			log.debug("Closing idle socket: [{}]", channel_info);
			ctx.channel().close();
			log.info("Closed idle socket: [{}]", channel_info);			
		}
		super.channelIdle(ctx, evt);
	}
	
	/**
	 * Counts channel closed events
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelOutboundHandler#close(io.netty.channel.ChannelHandlerContext, io.netty.channel.ChannelPromise)
	 */
	@Override
	public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) throws Exception {
		connections_closed.incrementAndGet();
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelOutboundHandler#connect(io.netty.channel.ChannelHandlerContext, java.net.SocketAddress, java.net.SocketAddress, io.netty.channel.ChannelPromise)
	 */
	@Override
	public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress, final SocketAddress localAddress,
			final ChannelPromise promise) throws Exception {
		connections_established.incrementAndGet();
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelHandler#exceptionCaught(io.netty.channel.ChannelHandlerContext, java.lang.Throwable)
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
	        if (log.isDebugEnabled()) {
	        	log.debug("Refusing connection from " + chan, cause);
	        }
	        chan.close();
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
	
	  /** Simple exception for refusing a connection. */
	  private static class ConnectionRefusedException extends ChannelException {
	    
		/**  */
		private static final long serialVersionUID = -8713880417174327967L;

		/**
	     * Default ctor with a message.
	     * @param message A descriptive message for the exception.
	     */
	    public ConnectionRefusedException(final String message) {
	      super(message);
	    }

	    
	  }
	

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandler#channelRegistered(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandler#channelUnregistered(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelUnregistered(final ChannelHandlerContext ctx) throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandler#channelActive(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandler#channelInactive(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandler#channelRead(io.netty.channel.ChannelHandlerContext, java.lang.Object)
	 */
	@Override
	public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandler#channelReadComplete(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandler#userEventTriggered(io.netty.channel.ChannelHandlerContext, java.lang.Object)
	 */
	@Override
	public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandler#channelWritabilityChanged(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelOutboundHandler#bind(io.netty.channel.ChannelHandlerContext, java.net.SocketAddress, io.netty.channel.ChannelPromise)
	 */
	@Override
	public void bind(final ChannelHandlerContext ctx, final SocketAddress localAddress, final ChannelPromise promise) throws Exception {
		/* No Op */
	}


	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelOutboundHandler#disconnect(io.netty.channel.ChannelHandlerContext, io.netty.channel.ChannelPromise)
	 */
	@Override
	public void disconnect(final ChannelHandlerContext ctx, final ChannelPromise promise) throws Exception {
		/* No Op */
	}


	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelOutboundHandler#deregister(io.netty.channel.ChannelHandlerContext, io.netty.channel.ChannelPromise)
	 */
	@Override
	public void deregister(final ChannelHandlerContext ctx, final ChannelPromise promise) throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelOutboundHandler#read(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void read(final ChannelHandlerContext ctx) throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelOutboundHandler#write(io.netty.channel.ChannelHandlerContext, java.lang.Object, io.netty.channel.ChannelPromise)
	 */
	@Override
	public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelOutboundHandler#flush(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void flush(final ChannelHandlerContext ctx) throws Exception {
		/* No Op */
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelHandler#handlerAdded(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelHandler#handlerRemoved(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
		/* No Op */
	}



	public AtomicLong getConnections_established() {
		return connections_established;
	}



	public AtomicLong getConnections_closed() {
		return connections_closed;
	}



	public AtomicLong getConnections_rejected() {
		return connections_rejected;
	}



	public AtomicLong getExceptions_unknown() {
		return exceptions_unknown;
	}



	public AtomicLong getExceptions_closed() {
		return exceptions_closed;
	}



	public AtomicLong getExceptions_reset() {
		return exceptions_reset;
	}



	public AtomicLong getExceptions_timeout() {
		return exceptions_timeout;
	}

	

}
