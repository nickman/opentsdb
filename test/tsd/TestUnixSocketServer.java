// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.RowLock;
import org.hbase.async.Scanner;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.tsd.UnixDomainSocketServer.StringQueue;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: TestUnixSocketServer</p>
 * <p>Description: Tests for Unix socket server</p> 
 * <p><code>net.opentsdb.tsd.TestUnixSocketServer</code></p>
 */

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
	  "ch.qos.*", "org.slf4j.*",
	  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, TSMeta.class, UIDMeta.class, 
	  HBaseClient.class, RowLock.class, UniqueIdRpc.class, KeyValue.class, 
	  GetRequest.class, Scanner.class, UniqueId.class, HttpQuery.class, PipelineFactory.class})
public class TestUnixSocketServer {
	/** The socket name for the current test */
	File currentSocket = null;
	/** The current instance of the server */
	UnixDomainSocketServer server = null; //UnixDomainSocketServer(final TSDB tsdb, final PipelineFactory pipelineFactory)
	/** The ByteBuf allocator */
	final BufferManager bufferManager = BufferManager.newInstance();
	/** The unix socket server client */
	Channel clientChannel = null;
	
	/**
	 * Creates a new socket file
	 * @throws Exception
	 */
	@Before
	public void resetSocket() throws Exception {
		Assume.assumeTrue(System.getProperty("os.name").toLowerCase().contains("linux"));
		currentSocket = File.createTempFile("UnixSocket", ".sock");
		currentSocket.delete();
	}
	
	/**
	 * Stops the server and deletes the socket file
	 */
	@After
	public void close() {
		if(server!=null) {
			server.stop();
		}
		if(currentSocket!=null) currentSocket.delete();
	}
	
	/**
	 * Creates a mockito answer that overrides the initialization of a pipeline
	 * @param handlers The handlers to add to the pipeline
	 * @return the answer
	 */
	Answer<Void> initPipeline(final ChannelHandler...handlers) {
		return new Answer<Void>() {
			@Override
			public Void answer(final InvocationOnMock invocation) throws Throwable {
				final Channel channel = (Channel)invocation.getArguments()[0];
				final ChannelPipeline pipeline = channel.pipeline();
				pipeline.addLast("frameDecoder", new LineBasedFrameDecoder(2048));
				pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8));
				pipeline.addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8));
				pipeline.addLast(handlers);
				System.err.println("Adding Handlers: " + Arrays.toString(handlers));
				return null;
			}
		};
	}
	
	@ChannelHandler.Sharable
	class StringEchoHandler extends ChannelDuplexHandler {
		@Override
		public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {		
			ctx.writeAndFlush(new StringBuilder(msg.toString()).reverse().toString());
		}
		public String toString() {
			return "testSimpleStartup-ChannelDuplexHandler";
		}		
	}
	
	/**
	 * Starts the UnixDomainSocketServer and tests it.
	 * The {@link PipelineFactory} is powermocked to install a string codec 
	 * and a string reverse handler into the server pipeline. The client
	 * then sends a random string and verifies that the response is the
	 * sent string reversed.
	 * @throws Exception
	 */
	@Test
	public void testSimpleStartup() throws Exception {
		try {
			TSDB tsdb = NettyMocks.getMockedHTTPTSDB();
			tsdb.getConfig().overrideConfig("tsd.network.unixsocket.path", currentSocket.getAbsolutePath());
			PipelineFactory pipelineFactory = PowerMockito.spy(new PipelineFactory(tsdb));
			
			final ChannelDuplexHandler handler = new StringEchoHandler();
			final Method initer = Whitebox.getMethod(PipelineFactory.class, "switchToTelnet", Channel.class);
			assertNotNull(initer);
			PowerMockito
				.doAnswer(
						initPipeline(handler)
				)
				.when(pipelineFactory).switchToTelnet(Mockito.any(Channel.class));
			server = new UnixDomainSocketServer(tsdb, pipelineFactory);
			server.start();
			assertTrue(server.isStarted());
			clientChannel = server.client().sync().channel();
			assertTrue(clientChannel.isOpen());
			final String message = UUID.randomUUID().toString();
			final String expectedResponse = new StringBuilder(message).reverse().toString();
			clientChannel.writeAndFlush(message + "\n");
			final StringQueue sq = UnixDomainSocketServer.stringQueue(clientChannel);
			final String actualResponse = sq.read(3000, TimeUnit.MILLISECONDS);
			assertEquals(expectedResponse, actualResponse);
			System.err.println("Result: [" + actualResponse + "]");
		} finally {
			try { clientChannel.close(); } catch (Exception x) {/* No Op */}
			clientChannel = null;
		}
	}
	
	
	
	
}
