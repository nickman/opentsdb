// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.tsd;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import net.opentsdb.core.TSDB;
import net.opentsdb.tools.EPollMonitor;
import net.opentsdb.tools.TSDServer;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Threads;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: UnixDomainSocketServer</p>
 * <p>Description: A unix socket put listener</p> 
 * <p><code>net.opentsdb.tsd.UnixDomainSocketServer</code></p>
 * Trace like this:  echo "put foo.bar 1460070776 5 dc=dc5 host=host5" | netcat -U /tmp/tsdb.sock
 * TODO: fine socket config
 */
@Sharable
public class UnixDomainSocketServer extends SimpleChannelInboundHandler<ByteBuf> {
	/** The boss event loop group */
	final  EventLoopGroup bossGroup;
	/** The worker event loop group */
	final  EventLoopGroup workerGroup;
	/** The server bootstrap */
	final  ServerBootstrap b = new ServerBootstrap();
	/** The client bootstrap */
	final  Bootstrap cb = new Bootstrap();
	/** Channel group for connected remote clients */
	final ChannelGroup remoteConnections;
	/** Channel group for connected local clients */
	final ChannelGroup localConnections;
	/** Event executor for the channel groups */
	final EventExecutor channelGroupExecutor;
	/** The channel initializer */
	final PipelineFactory pipelineFactory;
	/** The server channel */
	Channel serverChannel;
	/** The unix socket address the server will listen on */ 
	final DomainSocketAddress socketAddress;
	/** The path of the unix socket */
	final String path;
	/** Instance logger */
	final Logger log = LoggerFactory.getLogger(getClass());
	/** Started flag */
	final AtomicBoolean started = new AtomicBoolean(false);
	/** The logging handler to add to the server pipeline */
	volatile LoggingHandler loggingHandler = null; 
	/** The epoll monitor */
	final EPollMonitor monitor;
	
	/** The number of core available to this JVM */
	public static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
	/** The charset for telnet decoding */
	public static final Charset ISO8859 = Charset.forName("ISO-8859-1");
	// Those are entirely stateless and thus a single instance is needed.
	/** The string decoder */
	private static final StringEncoder ENCODER = new StringEncoder(ISO8859);
	/** If we get this on the unixsocket server, we shutdown */
	public final ByteBuf STOP_CODE;
	/** The number of bytes in the stop code */
	public final int STOP_CODE_SIZE;
	
	
	/**
	 * Creates a new UnixDomainSocketServer
	 * @param tsdb The parent TSDB instance
	 * @param pipelineFactory The server pipeline factory
	 */
	public UnixDomainSocketServer(final TSDB tsdb, final PipelineFactory pipelineFactory) {
		final Config config = tsdb.getConfig();
		if(config.hasProperty("tsd.network.unixsocket.stopcode")) {			
			STOP_CODE = BufferManager.getInstance().wrap(config.getString("tsd.network.unixsocket.stopcode").trim(), ISO8859).asReadOnly();
			STOP_CODE_SIZE = STOP_CODE.readableBytes();			
		} else {
			STOP_CODE = null;
			STOP_CODE_SIZE = -1;
		}
		path = config.getString("tsd.network.unixsocket.path");
		final int workerThreads = config.getInt("tsd.network.unixsocket.worker_threads", CORES*2);
		bossGroup = new EpollEventLoopGroup(1, Threads.newThreadFactory("UnixSocketBoss"));
		workerGroup = new EpollEventLoopGroup(workerThreads, Threads.newThreadFactory("UnixSocketWorker#%d"));
		channelGroupExecutor = new DefaultEventExecutor(Threads.newThreadFactory("UnixSocketGroup-%d", true, Thread.NORM_PRIORITY));
		remoteConnections = new DefaultChannelGroup("RemoteUnixSocketConnections", channelGroupExecutor);
		localConnections = new DefaultChannelGroup("LocalUnixSocketConnections", channelGroupExecutor);
		this.pipelineFactory = pipelineFactory; 
		b.group(bossGroup, workerGroup)
			.channel(EpollServerDomainSocketChannel.class)
			.option(ChannelOption.ALLOCATOR, BufferManager.getInstance())
			.childOption(ChannelOption.ALLOCATOR, BufferManager.getInstance())
			.childHandler(new ChannelInitializer<Channel>() {
				@Override
				protected void initChannel(final Channel ch) throws Exception {
					if(STOP_CODE!=null) {
						ch.pipeline().addLast("StopListener", UnixDomainSocketServer.this);
					}
					pipelineFactory.switchToTelnet(ch);
					installLoggingHandler(config, ch.pipeline());
					remoteConnections.add(ch);					
				}
			});
		cb.group(workerGroup)
			.channel(EpollDomainSocketChannel.class)
			.option(ChannelOption.ALLOCATOR, BufferManager.getInstance())
			.handler(new ChannelInitializer<Channel>() {
				@Override
				protected void initChannel(final Channel ch) throws Exception {
					final ChannelPipeline pipeline = ch.pipeline();
					pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8));
					pipeline.addLast("squeue", new StringQueue());
					pipeline.addLast("stringEncoder", ENCODER);
					pipeline.addLast("frameDecoder", new LineBasedFrameDecoder(2048));
					localConnections.add(ch);
				}
			});
		socketAddress = new DomainSocketAddress(path);	
		monitor = new EPollMonitor("unix", remoteConnections);
	}
	
	/**
	 * Conditionally installs a logging handler into the server's pipeline
	 * @param config The TSDB config
	 * @param pipeline The pipeline to insert into
	 */
	protected void installLoggingHandler(final Config config, final ChannelPipeline pipeline) {
		final LogLevel logLevel = config.getLogLevel("tsd.network.unixsocket.loglevel");
		if(logLevel!=null && pipeline.get("encoder")!=null) {
			loggingHandler = new LoggingHandler("UnixSocketConnection", logLevel);
			pipeline.addAfter("encoder", "logging", loggingHandler);
		}
	}
	
	/**
	 * Starts this unix domain socket server
	 * @throws Exception thrown on any error
	 */
	public void start() throws Exception {
		if(started.compareAndSet(false, true)) {
			log.info("Starting UnixDomainSocketServer...");
			try {
				serverChannel = b.bind(socketAddress).sync().channel();
				log.info("Started UnixDomainSocketServer on [{}]", path);
			} catch (Exception ex) {
				log.error("Failed to start UnixDomainSocketServer on [{}]", path, ex);
				started.set(false);
				throw ex;
			}
		}
	}

	/**
	 * Stops this unix domain socket server
	 */
	public void stop() {
		if(started.compareAndSet(true, false)) {
			log.info("Stopping UnixDomainSocketServer...");		
			localConnections.close();
			remoteConnections.close();
			try {
				if(serverChannel!=null) {
					serverChannel.close().sync();				
				}
			} catch (Exception ex) {
				log.warn("Failed to stop UnixDomainSocketServer on [{}]", path, ex);
			}				
			try { bossGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
			try { workerGroup.shutdownGracefully(); } catch (Exception x) {/* No Op */}
			try { channelGroupExecutor.shutdownGracefully(); } catch (Exception x) {/* No Op */}
			log.info("UnixDomainSocketServer shut down");
		}
	}
	
	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
		if(msg!=null && msg.readableBytes()>=STOP_CODE_SIZE) {
			if(msg.slice(0, STOP_CODE_SIZE).equals(STOP_CODE)) {
				try {
					TSDServer.getInstance().stop();
				} catch (Exception ex) {
					log.error("Failed to call for TSDServer Stop", ex);
					stop();
				}
			}
		}
		
	}
	
	
	public class StringQueue extends ChannelInboundHandlerAdapter {
		final BlockingQueue<String> q = new SynchronousQueue<String>(false);
		@Override
		public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
			q.put(msg.toString());
		}
		public String read() {
			try { 
				return q.take();
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		public String read(final long timeout, final TimeUnit unit) {
			try { 
				return q.poll(timeout, unit);
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		
	}
	
	/**
	 * Creates a client connection to this server
	 * @return the future of the connection
	 */
	public ChannelFuture client() {
		if(!started.get()) throw new IllegalStateException("The server has not been started");
		final ChannelFuture cf = cb.connect(socketAddress);
		return cf;
	}
	
	public static StringQueue stringQueue(final Channel channel) {
		return (StringQueue)channel.pipeline().get("squeue");
	}

	
	/**
	 * Indicates if the server is started
	 * @return true if started, false otherwise
	 */
	public boolean isStarted() {
		return started.get();
	}

}
