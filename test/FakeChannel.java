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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
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
	
	protected final AtomicBoolean writable = new AtomicBoolean(true);
	
	private static final ConcurrentHashMap<String, AtomicInteger> PLACEHOLDER_MAP = new ConcurrentHashMap<String, AtomicInteger>(0);
	
	
	private static final ConcurrentHashMap<FakeChannel, ConcurrentHashMap<String, AtomicInteger>> COUNTERS = 
		new ConcurrentHashMap<FakeChannel, ConcurrentHashMap<String, AtomicInteger>>();
	
	public int count(final String...names) {		
		int total = 0;
		for(String name: names) {
			try {
				total += COUNTERS.get(this).get(name).get();
			} catch (Exception ex) {
				throw new RuntimeException("No counter named [" + name + "]");
			}				
		}
		return total;
	}
	
	private static ConcurrentHashMap<String, AtomicInteger> countersFor(final FakeChannel fc) {
		ConcurrentHashMap<String, AtomicInteger> counters = COUNTERS.putIfAbsent(fc, PLACEHOLDER_MAP);
		if(counters==null || counters==PLACEHOLDER_MAP) {
			counters = new ConcurrentHashMap<String, AtomicInteger>();
			counters.put("closeFuture()Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("bytesBeforeUnwritable()J", new AtomicInteger(0));
			counters.put("bytesBeforeWritable()J", new AtomicInteger(0));
			counters.put("pipeline()Lio/netty/channel/ChannelPipeline;", new AtomicInteger(0));
			counters.put("alloc()Lio/netty/buffer/ByteBufAllocator;", new AtomicInteger(0));
			counters.put("remoteAddress()Ljava/net/SocketAddress;", new AtomicInteger(0));
			counters.put("localAddress()Ljava/net/SocketAddress;", new AtomicInteger(0));
			counters.put("eventLoop()Lio/netty/channel/EventLoop;", new AtomicInteger(0));
			counters.put("isActive()Z", new AtomicInteger(0));
			counters.put("isWritable()Z", new AtomicInteger(0));
			counters.put("config()Lio/netty/channel/ChannelConfig;", new AtomicInteger(0));
			counters.put("metadata()Lio/netty/channel/ChannelMetadata;", new AtomicInteger(0));
			counters.put("parent()Lio/netty/channel/Channel;", new AtomicInteger(0));
			counters.put("isRegistered()Z", new AtomicInteger(0));
			counters.put("read()Lio/netty/channel/Channel;", new AtomicInteger(0));
			counters.put("flush()Lio/netty/channel/Channel;", new AtomicInteger(0));
			counters.put("unsafe()Lio/netty/channel/Channel$Unsafe;", new AtomicInteger(0));
			counters.put("isOpen()Z", new AtomicInteger(0));
			counters.put("newSucceededFuture()Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("newProgressivePromise()Lio/netty/channel/ChannelProgressivePromise;", new AtomicInteger(0));
			counters.put("writeAndFlush(Ljava/lang/Object;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("writeAndFlush(Ljava/lang/Object;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("deregister()Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("deregister(Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("newPromise()Lio/netty/channel/ChannelPromise;", new AtomicInteger(0));
			counters.put("newFailedFuture(Ljava/lang/Throwable;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("bind(Ljava/net/SocketAddress;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("bind(Ljava/net/SocketAddress;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("disconnect()Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("disconnect(Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("write(Ljava/lang/Object;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("write(Ljava/lang/Object;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("read()Lio/netty/channel/ChannelOutboundInvoker;", new AtomicInteger(0));
			counters.put("connect(Ljava/net/SocketAddress;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("connect(Ljava/net/SocketAddress;Ljava/net/SocketAddress;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("connect(Ljava/net/SocketAddress;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("connect(Ljava/net/SocketAddress;Ljava/net/SocketAddress;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;", new AtomicInteger(0));
			counters.put("flush()Lio/netty/channel/ChannelOutboundInvoker;", new AtomicInteger(0));
			COUNTERS.replace(fc, PLACEHOLDER_MAP, counters);
		}
		return counters;
	}
	
	private static void increment(final FakeChannel fc, final String name) {
		final ConcurrentHashMap<String, AtomicInteger> counters = countersFor(fc);
		counters.get(name).incrementAndGet();
	}
	
	public int isOpenCount() {
		return count("isOpen()Z");
	}
	
	public int isWritableCount() {
		return count("isWritable()Z");
	}
	
	public int anyWriteCount() {
		return count("write(Ljava/lang/Object;)Lio/netty/channel/ChannelFuture;", "write(Ljava/lang/Object;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;");
	}
	
	public int anyWriteAndFlushCount() {
		return count("writeAndFlush(Ljava/lang/Object;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;", "writeAndFlush(Ljava/lang/Object;)Lio/netty/channel/ChannelFuture;");
	}
	
	public int anyWriteOrWriteAndFlush() {
		return anyWriteCount() + anyWriteAndFlushCount();
	}

	
	/**
	 * Creates a new FakeChannel
	 */
	public FakeChannel() {
		super();
	}

	/**
	 * Creates a new FakeChannel
	 * @param channelId
	 */
	public FakeChannel(ChannelId channelId) {
		super(channelId);
	}

	/**
	 * Creates a new FakeChannel
	 * @param handlers
	 */
	public FakeChannel(ChannelHandler... handlers) {
		super(handlers);
	}

	/**
	 * Creates a new FakeChannel
	 * @param hasDisconnect
	 * @param handlers
	 */
	public FakeChannel(boolean hasDisconnect, ChannelHandler... handlers) {
		super(hasDisconnect, handlers);
	}

	/**
	 * Creates a new FakeChannel
	 * @param channelId
	 * @param handlers
	 */
	public FakeChannel(ChannelId channelId, ChannelHandler... handlers) {
		super(channelId, handlers);
	}

	/**
	 * Creates a new FakeChannel
	 * @param channelId
	 * @param hasDisconnect
	 * @param handlers
	 */
	public FakeChannel(ChannelId channelId, boolean hasDisconnect, ChannelHandler... handlers) {
		super(channelId, hasDisconnect, handlers);
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
	}

	/**
	 * Resets the invocation counters
	 */
	public void resetCounters() {
		COUNTERS.remove(this);
		countersFor(this);
	}
	
	/**
	 * Returns a formatted string reporting the counters
	 * @return the counters print
	 */
	public String printCounters() {
		return new StringBuilder(28 * 25)
				.append("CloseFuture Count:").append(COUNTERS.get(this).get("closeFuture()Lio/netty/channel/ChannelFuture;").get())
				.append("BytesBeforeUnwritable Count:").append(COUNTERS.get(this).get("bytesBeforeUnwritable()J").get())
				.append("BytesBeforeWritable Count:").append(COUNTERS.get(this).get("bytesBeforeWritable()J").get())
				.append("Pipeline Count:").append(COUNTERS.get(this).get("pipeline()Lio/netty/channel/ChannelPipeline;").get())
				.append("Alloc Count:").append(COUNTERS.get(this).get("alloc()Lio/netty/buffer/ByteBufAllocator;").get())
				.append("RemoteAddress Count:").append(COUNTERS.get(this).get("remoteAddress()Ljava/net/SocketAddress;").get())
				.append("LocalAddress Count:").append(COUNTERS.get(this).get("localAddress()Ljava/net/SocketAddress;").get())
				.append("EventLoop Count:").append(COUNTERS.get(this).get("eventLoop()Lio/netty/channel/EventLoop;").get())
				.append("IsActive Count:").append(COUNTERS.get(this).get("isActive()Z").get())
				.append("IsWritable Count:").append(COUNTERS.get(this).get("isWritable()Z").get())
				.append("Config Count:").append(COUNTERS.get(this).get("config()Lio/netty/channel/ChannelConfig;").get())
				.append("Metadata Count:").append(COUNTERS.get(this).get("metadata()Lio/netty/channel/ChannelMetadata;").get())
				.append("Parent Count:").append(COUNTERS.get(this).get("parent()Lio/netty/channel/Channel;").get())
				.append("IsRegistered Count:").append(COUNTERS.get(this).get("isRegistered()Z").get())
				.append("Read Count:").append(COUNTERS.get(this).get("read()Lio/netty/channel/Channel;").get())
				.append("Flush Count:").append(COUNTERS.get(this).get("flush()Lio/netty/channel/Channel;").get())
				.append("Unsafe Count:").append(COUNTERS.get(this).get("unsafe()Lio/netty/channel/Channel$Unsafe;").get())
				.append("IsOpen Count:").append(COUNTERS.get(this).get("isOpen()Z").get())
				.append("NewSucceededFuture Count:").append(COUNTERS.get(this).get("newSucceededFuture()Lio/netty/channel/ChannelFuture;").get())
				.append("NewProgressivePromise Count:").append(COUNTERS.get(this).get("newProgressivePromise()Lio/netty/channel/ChannelProgressivePromise;").get())
				.append("WriteAndFlush Count:").append(COUNTERS.get(this).get("writeAndFlush(Ljava/lang/Object;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;").get())
				.append("WriteAndFlush Count:").append(COUNTERS.get(this).get("writeAndFlush(Ljava/lang/Object;)Lio/netty/channel/ChannelFuture;").get())
				.append("Deregister Count:").append(COUNTERS.get(this).get("deregister()Lio/netty/channel/ChannelFuture;").get())
				.append("DeregisterPromise Count:").append(COUNTERS.get(this).get("deregister(Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;").get())
				.append("NewPromise Count:").append(COUNTERS.get(this).get("newPromise()Lio/netty/channel/ChannelPromise;").get())
				.append("NewFailedFuture Count:").append(COUNTERS.get(this).get("newFailedFuture(Ljava/lang/Throwable;)Lio/netty/channel/ChannelFuture;").get())
				.append("BindWPromise Count:").append(COUNTERS.get(this).get("bind(Ljava/net/SocketAddress;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;").get())
				.append("Bind Count:").append(COUNTERS.get(this).get("bind(Ljava/net/SocketAddress;)Lio/netty/channel/ChannelFuture;").get())
				.append("Disconnect Count:").append(COUNTERS.get(this).get("disconnect()Lio/netty/channel/ChannelFuture;").get())
				.append("DisconnectWPromise Count:").append(COUNTERS.get(this).get("disconnect(Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;").get())
				.append("Write Count:").append(COUNTERS.get(this).get("write(Ljava/lang/Object;)Lio/netty/channel/ChannelFuture;").get())
				.append("WriteWPromise Count:").append(COUNTERS.get(this).get("write(Ljava/lang/Object;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;").get())
				.append("Read Count:").append(COUNTERS.get(this).get("read()Lio/netty/channel/ChannelOutboundInvoker;").get())
				.append("Connect Count:").append(COUNTERS.get(this).get("connect(Ljava/net/SocketAddress;)Lio/netty/channel/ChannelFuture;").get())
				.append("ConnectWLocal Count:").append(COUNTERS.get(this).get("connect(Ljava/net/SocketAddress;Ljava/net/SocketAddress;)Lio/netty/channel/ChannelFuture;").get())
				.append("ConnectWPromise Count:").append(COUNTERS.get(this).get("connect(Ljava/net/SocketAddress;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;").get())
				.append("ConnectWPromise&Local Count:").append(COUNTERS.get(this).get("connect(Ljava/net/SocketAddress;Ljava/net/SocketAddress;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;").get())
				.append("Flush Count:").append(COUNTERS.get(this).get("flush()Lio/netty/channel/ChannelOutboundInvoker;").get())
				.toString();
	}
	
	public ChannelFuture closeFuture() {
		increment(this, "closeFuture()Lio/netty/channel/ChannelFuture;");
		return super.closeFuture();
	}

	public long bytesBeforeUnwritable() {
		increment(this, "bytesBeforeUnwritable()J");
		return super.bytesBeforeUnwritable();
	}

	public long bytesBeforeWritable() {
		increment(this, "bytesBeforeWritable()J");
		return super.bytesBeforeWritable();
	}

	public ChannelPipeline pipeline() {
		increment(this, "pipeline()Lio/netty/channel/ChannelPipeline;");
		return super.pipeline();
	}

	public ByteBufAllocator alloc() {
		increment(this, "alloc()Lio/netty/buffer/ByteBufAllocator;");
		return super.alloc();
	}

	public SocketAddress remoteAddress() {
		increment(this, "remoteAddress()Ljava/net/SocketAddress;");
		return super.remoteAddress();
	}

	public SocketAddress localAddress() {
		increment(this, "localAddress()Ljava/net/SocketAddress;");
		return super.localAddress();
	}

	public EventLoop eventLoop() {
		increment(this, "eventLoop()Lio/netty/channel/EventLoop;");
		return super.eventLoop();
	}

	public boolean isActive() {
		increment(this, "isActive()Z");
		return super.isActive();
	}

	public boolean isWritable() {
		increment(this, "isWritable()Z");
		if(this.writable.get()) {
			return super.isWritable();
		}
		return false;
	}
	
	public FakeChannel setWritable(final boolean writable) {
		this.writable.set(writable);
		return this;
	}

	public ChannelConfig config() {
		increment(this, "config()Lio/netty/channel/ChannelConfig;");
		return super.config();
	}

	public ChannelMetadata metadata() {
		increment(this, "metadata()Lio/netty/channel/ChannelMetadata;");
		return super.metadata();
	}

	public Channel parent() {
		increment(this, "parent()Lio/netty/channel/Channel;");
		return super.parent();
	}

	public boolean isRegistered() {
		increment(this, "isRegistered()Z");
		return super.isRegistered();
	}

	public Channel read() {
		increment(this, "read()Lio/netty/channel/Channel;");
		return super.read();
	}

	public Channel flush() {
		increment(this, "flush()Lio/netty/channel/Channel;");
		return super.flush();
	}

	public Unsafe unsafe() {
		increment(this, "unsafe()Lio/netty/channel/Channel$Unsafe;");
		return super.unsafe();
	}

	public boolean isOpen() {
		increment(this, "isOpen()Z");
		return super.isOpen();
	}

//	public ChannelId id() {
//		increment(this, "id()Lio/netty/channel/ChannelId;");
//		return super.id();
//	}

	public ChannelFuture newSucceededFuture() {
		increment(this, "newSucceededFuture()Lio/netty/channel/ChannelFuture;");
		return super.newSucceededFuture();
	}

	public ChannelProgressivePromise newProgressivePromise() {
		increment(this, "newProgressivePromise()Lio/netty/channel/ChannelProgressivePromise;");
		return super.newProgressivePromise();
	}

	public ChannelFuture writeAndFlush(final Object object, final ChannelPromise channelPromise) {
		increment(this, "writeAndFlush(Ljava/lang/Object;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;");
		return super.writeAndFlush(object, channelPromise);
	}
		

	public ChannelFuture writeAndFlush(final Object object) {
		increment(this, "writeAndFlush(Ljava/lang/Object;)Lio/netty/channel/ChannelFuture;");
		return super.writeAndFlush(object);
	}

	public ChannelFuture deregister() {
		increment(this, "deregister()Lio/netty/channel/ChannelFuture;");
		return super.deregister();
	}

	public ChannelFuture deregister(final ChannelPromise channelPromise) {
		increment(this, "deregister(Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;");
		return super.deregister(channelPromise);
	}

	public ChannelPromise newPromise() {
		increment(this, "newPromise()Lio/netty/channel/ChannelPromise;");
		return super.newPromise();
	}

	public ChannelFuture newFailedFuture(final Throwable throwable) {
		increment(this, "newFailedFuture(Ljava/lang/Throwable;)Lio/netty/channel/ChannelFuture;");
		return super.newFailedFuture(throwable);
	}

//	public ChannelPromise voidPromise() {
//		increment(this, "voidPromise()Lio/netty/channel/ChannelPromise;");
//		return super.voidPromise();
//	}

	public ChannelFuture bind(final SocketAddress socketAddress, final ChannelPromise channelPromise) {
		increment(this, "bind(Ljava/net/SocketAddress;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;");
		return super.bind(socketAddress, channelPromise);
	}

	public ChannelFuture bind(final SocketAddress socketAddress) {
		increment(this, "bind(Ljava/net/SocketAddress;)Lio/netty/channel/ChannelFuture;");
		return super.bind(socketAddress);
	}

//	public ChannelFuture disconnect() {
//		increment(this, "disconnect()Lio/netty/channel/ChannelFuture;");
//		return super.disconnect();
//	}

//	public ChannelFuture disconnect(final ChannelPromise channelPromise) {
//		increment(this, "disconnect(Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;");
//		return super.disconnect(channelPromise);
//	}

	public ChannelFuture write(final Object object) {
		increment(this, "write(Ljava/lang/Object;)Lio/netty/channel/ChannelFuture;");
		return super.write(object);
	}

	public ChannelFuture write(final Object object, final ChannelPromise channelPromise) {
		increment(this, "write(Ljava/lang/Object;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;");
		return super.write(object, channelPromise);
	}


	public ChannelFuture connect(final SocketAddress socketAddress) {
		increment(this, "connect(Ljava/net/SocketAddress;)Lio/netty/channel/ChannelFuture;");
		return super.connect(socketAddress);
	}

	public ChannelFuture connect(final SocketAddress remoteAddress, final SocketAddress localAddress) {
		increment(this, "connect(Ljava/net/SocketAddress;Ljava/net/SocketAddress;)Lio/netty/channel/ChannelFuture;");
		return super.connect(remoteAddress, localAddress);
	}

	public ChannelFuture connect(final SocketAddress socketAddress, final ChannelPromise channelPromise) {
		increment(this, "connect(Ljava/net/SocketAddress;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;");
		return super.connect(socketAddress, channelPromise);
	}

	public ChannelFuture connect(final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise channelPromise) {
		increment(this, "connect(Ljava/net/SocketAddress;Ljava/net/SocketAddress;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;");
		return super.connect(remoteAddress, localAddress, channelPromise);
	}


//	public ChannelFuture close() {
//		increment(this, "close()Lio/netty/channel/ChannelFuture;");
//		return super.close();
//	}
//
//	public ChannelFuture close(final ChannelPromise channelPromise) {
//		increment(this, "close(Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;");
//		return super.close(channelPromise);
//	}


	
	
}
