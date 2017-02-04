/**
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 */
package net.opentsdb;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * <p>Title: FakeChannel</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.FakeChannel</code></p>
 */

public class FakeChannel extends EmbeddedChannel {
	/** Counter for invocations on <b><code>alloc</code></b> */
	final AtomicInteger allocCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>bind</code></b> */
	final AtomicInteger bindCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>bytesBeforeUnwritable</code></b> */
	final AtomicInteger bytesBeforeUnwritableCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>bytesBeforeWritable</code></b> */
	final AtomicInteger bytesBeforeWritableCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>close</code></b> */
	final AtomicInteger closeCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>closeFuture</code></b> */
	final AtomicInteger closeFutureCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>config</code></b> */
	final AtomicInteger configCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>connect</code></b> */
	final AtomicInteger connectCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>deregister</code></b> */
	final AtomicInteger deregisterCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>disconnect</code></b> */
	final AtomicInteger disconnectCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>eventLoop</code></b> */
	final AtomicInteger eventLoopCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>flush</code></b> */
	final AtomicInteger flushCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>id</code></b> */
	final AtomicInteger idCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>isActive</code></b> */
	final AtomicInteger isActiveCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>isOpen</code></b> */
	final AtomicInteger isOpenCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>isRegistered</code></b> */
	final AtomicInteger isRegisteredCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>isWritable</code></b> */
	final AtomicInteger isWritableCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>localAddress</code></b> */
	final AtomicInteger localAddressCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>metadata</code></b> */
	final AtomicInteger metadataCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>newFailedFuture</code></b> */
	final AtomicInteger newFailedFutureCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>newProgressivePromise</code></b> */
	final AtomicInteger newProgressivePromiseCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>newPromise</code></b> */
	final AtomicInteger newPromiseCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>newSucceededFuture</code></b> */
	final AtomicInteger newSucceededFutureCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>parent</code></b> */
	final AtomicInteger parentCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>pipeline</code></b> */
	final AtomicInteger pipelineCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>read</code></b> */
	final AtomicInteger readCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>remoteAddress</code></b> */
	final AtomicInteger remoteAddressCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>unsafe</code></b> */
	final AtomicInteger unsafeCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>voidPromise</code></b> */
	final AtomicInteger voidPromiseCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>write</code></b> */
	final AtomicInteger writeCounter = new AtomicInteger(0);
	/** Counter for invocations on <b><code>writeAndFlush</code></b> */
	final AtomicInteger writeAndFlushCounter = new AtomicInteger(0);

	/**
	 * Creates a new FakeChannel
	 */
	public FakeChannel() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * Creates a new FakeChannel
	 * @param channelId
	 */
	public FakeChannel(ChannelId channelId) {
		super(channelId);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Creates a new FakeChannel
	 * @param handlers
	 */
	public FakeChannel(ChannelHandler... handlers) {
		super(handlers);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Creates a new FakeChannel
	 * @param hasDisconnect
	 * @param handlers
	 */
	public FakeChannel(boolean hasDisconnect, ChannelHandler... handlers) {
		super(hasDisconnect, handlers);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Creates a new FakeChannel
	 * @param channelId
	 * @param handlers
	 */
	public FakeChannel(ChannelId channelId, ChannelHandler... handlers) {
		super(channelId, handlers);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Creates a new FakeChannel
	 * @param channelId
	 * @param hasDisconnect
	 * @param handlers
	 */
	public FakeChannel(ChannelId channelId, boolean hasDisconnect, ChannelHandler... handlers) {
		super(channelId, hasDisconnect, handlers);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Creates a new FakeChannel
	 * @param channelId
	 * @param hasDisconnect
	 * @param config
	 * @param handlers
	 */
	public FakeChannel(ChannelId channelId, boolean hasDisconnect, ChannelConfig config, ChannelHandler... handlers) {
		super(channelId, hasDisconnect, config, handlers);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Resets the invocation counters
	 */
	public void resetCounters() {
		allocCounter.set(0); bindCounter.set(0); bytesBeforeUnwritableCounter.set(0); 
		bytesBeforeWritableCounter.set(0); closeCounter.set(0); closeFutureCounter.set(0); 
		configCounter.set(0); connectCounter.set(0); deregisterCounter.set(0); 
		disconnectCounter.set(0); eventLoopCounter.set(0); flushCounter.set(0); 
		idCounter.set(0); isActiveCounter.set(0); isOpenCounter.set(0); 
		isRegisteredCounter.set(0); isWritableCounter.set(0); localAddressCounter.set(0); 
		metadataCounter.set(0); newFailedFutureCounter.set(0); newProgressivePromiseCounter.set(0); 
		newPromiseCounter.set(0); newSucceededFutureCounter.set(0); parentCounter.set(0); 
		pipelineCounter.set(0); readCounter.set(0); remoteAddressCounter.set(0); 
		unsafeCounter.set(0); voidPromiseCounter.set(0); writeCounter.set(0); 
		writeAndFlushCounter.set(0); 		
	}
	
	/**
	 * Returns a formatted string reporting the counters
	 * @return the counters print
	 */
	public String printCounters() {
		return new StringBuilder(28 * 25)
				.append("Alloc Count:").append(allocCounter.get())
				.append("Bind Count:").append(bindCounter.get())
				.append("BytesBeforeUnwritable Count:").append(bytesBeforeUnwritableCounter.get())
				.append("BytesBeforeWritable Count:").append(bytesBeforeWritableCounter.get())
				.append("Close Count:").append(closeCounter.get())
				.append("CloseFuture Count:").append(closeFutureCounter.get())
				.append("Config Count:").append(configCounter.get())
				.append("Connect Count:").append(connectCounter.get())
				.append("Deregister Count:").append(deregisterCounter.get())
				.append("Disconnect Count:").append(disconnectCounter.get())
				.append("EventLoop Count:").append(eventLoopCounter.get())
				.append("Flush Count:").append(flushCounter.get())
				.append("Id Count:").append(idCounter.get())
				.append("IsActive Count:").append(isActiveCounter.get())
				.append("IsOpen Count:").append(isOpenCounter.get())
				.append("IsRegistered Count:").append(isRegisteredCounter.get())
				.append("IsWritable Count:").append(isWritableCounter.get())
				.append("LocalAddress Count:").append(localAddressCounter.get())
				.append("Metadata Count:").append(metadataCounter.get())
				.append("NewFailedFuture Count:").append(newFailedFutureCounter.get())
				.append("NewProgressivePromise Count:").append(newProgressivePromiseCounter.get())
				.append("NewPromise Count:").append(newPromiseCounter.get())
				.append("NewSucceededFuture Count:").append(newSucceededFutureCounter.get())
				.append("Parent Count:").append(parentCounter.get())
				.append("Pipeline Count:").append(pipelineCounter.get())
				.append("Read Count:").append(readCounter.get())
				.append("RemoteAddress Count:").append(remoteAddressCounter.get())
				.append("Unsafe Count:").append(unsafeCounter.get())
				.append("VoidPromise Count:").append(voidPromiseCounter.get())
				.append("Write Count:").append(writeCounter.get())
				.append("WriteAndFlush Count:").append(writeAndFlushCounter.get())
				.toString();
	}
	
	public SocketAddress remoteAddress() {
		remoteAddressCounter.incrementAndGet();
		return super.remoteAddress();
	}

	public long bytesBeforeUnwritable() {
		bytesBeforeUnwritableCounter.incrementAndGet();
		return super.bytesBeforeUnwritable();
	}

	public long bytesBeforeWritable() {
		bytesBeforeWritableCounter.incrementAndGet();
		return super.bytesBeforeWritable();
	}

	public ChannelFuture closeFuture() {
		closeFutureCounter.incrementAndGet();
		return super.closeFuture();
	}

	public SocketAddress localAddress() {
		localAddressCounter.incrementAndGet();
		return super.localAddress();
	}

	public EventLoop eventLoop() {
		eventLoopCounter.incrementAndGet();
		return super.eventLoop();
	}

	public ByteBufAllocator alloc() {
		allocCounter.incrementAndGet();
		return super.alloc();
	}

	public boolean isWritable() {
		isWritableCounter.incrementAndGet();
		return super.isWritable();
	}

	public boolean isActive() {
		isActiveCounter.incrementAndGet();
		return super.isActive();
	}

	public ChannelConfig config() {
		configCounter.incrementAndGet();
		return super.config();
	}

	public ChannelMetadata metadata() {
		metadataCounter.incrementAndGet();
		return super.metadata();
	}

	public ChannelPipeline pipeline() {
		pipelineCounter.incrementAndGet();
		return super.pipeline();
	}

	public Channel parent() {
		parentCounter.incrementAndGet();
		return super.parent();
	}

	public boolean isRegistered() {
		isRegisteredCounter.incrementAndGet();
		return super.isRegistered();
	}

//	public Channel read() {
//		readCounter.incrementAndGet();
//		return super.read();
//	}
//
//	public Channel flush() {
//		flushCounter.incrementAndGet();
//		return super.flush();
//	}

	public Unsafe unsafe() {
		unsafeCounter.incrementAndGet();
		return super.unsafe();
	}

	public boolean isOpen() {
		isOpenCounter.incrementAndGet();
		return super.isOpen();
	}

//	public ChannelId id() {
//		idCounter.incrementAndGet();
//		return super.id();
//	}

	public ChannelFuture writeAndFlush(final Object object, final ChannelPromise channelPromise) {
		writeAndFlushCounter.incrementAndGet();
		return super.writeAndFlush(object, channelPromise);
	}

	public ChannelFuture writeAndFlush(final Object object) {
		writeAndFlushCounter.incrementAndGet();
		return super.writeAndFlush(object);
	}

	public ChannelFuture deregister() {
		deregisterCounter.incrementAndGet();
		return super.deregister();
	}

	public ChannelFuture deregister(final ChannelPromise channelPromise) {
		deregisterCounter.incrementAndGet();
		return super.deregister(channelPromise);
	}

	public ChannelProgressivePromise newProgressivePromise() {
		newProgressivePromiseCounter.incrementAndGet();
		return super.newProgressivePromise();
	}

	public ChannelFuture newFailedFuture(final Throwable throwable) {
		newFailedFutureCounter.incrementAndGet();
		return super.newFailedFuture(throwable);
	}

//	public ChannelPromise voidPromise() {
//		voidPromiseCounter.incrementAndGet();
//		return super.voidPromise();
//	}

	public ChannelPromise newPromise() {
		newPromiseCounter.incrementAndGet();
		return super.newPromise();
	}

	public ChannelFuture newSucceededFuture() {
		newSucceededFutureCounter.incrementAndGet();
		return super.newSucceededFuture();
	}

	public ChannelFuture bind(final SocketAddress socketAddress, final ChannelPromise channelPromise) {
		bindCounter.incrementAndGet();
		return super.bind(socketAddress, channelPromise);
	}

	public ChannelFuture bind(final SocketAddress socketAddress) {
		bindCounter.incrementAndGet();
		return super.bind(socketAddress);
	}

//	public ChannelFuture disconnect() {
//		disconnectCounter.incrementAndGet();
//		return super.disconnect();
//	}

//	public ChannelFuture disconnect(final ChannelPromise channelPromise) {
//		disconnectCounter.incrementAndGet();
//		return super.disconnect(channelPromise);
//	}

	public ChannelFuture write(final Object object, final ChannelPromise channelPromise) {
		writeCounter.incrementAndGet();
		return super.write(object, channelPromise);
	}

	public ChannelFuture write(final Object object) {
		writeCounter.incrementAndGet();
		return super.write(object);
	}

//	public ChannelOutboundInvoker read() {
//		readCounter.incrementAndGet();
//		return super.read();
//	}

	public ChannelFuture connect(final SocketAddress socketAddress) {
		connectCounter.incrementAndGet();
		return super.connect(socketAddress);
	}

	public ChannelFuture connect(final SocketAddress socketAddress1, final SocketAddress socketAddress2) {
		connectCounter.incrementAndGet();
		return super.connect(socketAddress1, socketAddress2);
	}

	public ChannelFuture connect(final SocketAddress socketAddress1, final SocketAddress socketAddress2, final ChannelPromise channelPromise) {
		connectCounter.incrementAndGet();
		return super.connect(socketAddress1, socketAddress2, channelPromise);
	}

	public ChannelFuture connect(final SocketAddress socketAddress, final ChannelPromise channelPromise) {
		connectCounter.incrementAndGet();
		return super.connect(socketAddress, channelPromise);
	}

//	public ChannelOutboundInvoker flush() {
//		flushCounter.incrementAndGet();
//		return super.flush();
//	}
//
//	public ChannelFuture close() {
//		closeCounter.incrementAndGet();
//		return super.close();
//	}

//	public ChannelFuture close(final ChannelPromise channelPromise) {
//		closeCounter.incrementAndGet();
//		return super.close(channelPromise);
//	}

	
	
}
