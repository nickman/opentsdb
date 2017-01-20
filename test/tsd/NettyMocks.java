// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.net.SocketAddress;

import org.junit.Ignore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPipeline;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * Helper class that provides mockups for testing any OpenTSDB processes that
 * deal with Netty.
 */
/**
 * <p>Title: NettyMocks</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.NettyMocks</code></p>
 */
@Ignore
public final class NettyMocks {

  static final BufferManager bufferManager = BufferManager.newInstance();
  /**
   * Sets up a TSDB object for HTTP RPC tests that has a Config object
   * @return A TSDB mock
   */
  public static TSDB getMockedHTTPTSDB() throws Exception {
    final TSDB tsdb = mock(TSDB.class);
    final Config config = new Config(false);
    config.overrideConfig("tsd.http.show_stack_trace", "true");
    when(tsdb.getConfig()).thenReturn(config);
    return tsdb;
  }
  
  /**
   * Returns a mocked Channel object that simply sets the name to
   * [fake channel]
   * @return A Channel mock
   */
  public static Channel fakeChannel() {
    final Channel chan = mock(Channel.class);
    when(chan.toString()).thenReturn("[fake channel]");
    when(chan.isOpen()).thenReturn(true);
    when(chan.isWritable()).thenReturn(true);
    
    final SocketAddress socket = mock(SocketAddress.class);
    when(socket.toString()).thenReturn("192.168.1.1:4243");
    when(chan.remoteAddress()).thenReturn(socket);
    
    return chan;
  }
  
  /**
   * Returns an HttpQuery object with the given URI and the following parameters:
   * Method = GET
   * Content = null
   * Content-Type = null
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @return an HttpQuery object
   */
  public static HttpQuery getQuery(final TSDB tsdb, final String uri) {
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, uri);
    return new HttpQuery(tsdb, req, channelMock);
  }
  
  /**
   * Returns an HttpQuery object with the given uri, content and type
   * Method = POST
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @return an HttpQuery object
   */
  public static HttpQuery postQuery(final TSDB tsdb, final String uri, 
      final String content) {
    return postQuery(tsdb, uri, content, "application/json; charset=UTF-8");
  }
  
  /**
   * Returns an HttpQuery object with the given uri, content and type
   * Method = POST
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @param type Content-Type value
   * @return an HttpQuery object
   */
  public static HttpQuery postQuery(final TSDB tsdb, final String uri, 
      final String content, final String type) {
    return contentQuery(tsdb, uri, content, type, HttpMethod.POST);
  }
  
  /**
   * Returns an HttpQuery object with the given uri, content and type
   * Method = PUT
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @return an HttpQuery object
   */
  public static HttpQuery putQuery(final TSDB tsdb, final String uri, 
      final String content) {
    return putQuery(tsdb, uri, content, "application/json; charset=UTF-8");
  }
  
  /**
   * Returns an HttpQuery object with the given uri, content and type
   * Method = PUT
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @param type Content-Type value
   * @return an HttpQuery object
   */
  public static HttpQuery putQuery(final TSDB tsdb, final String uri, 
      final String content, final String type) {
    return contentQuery(tsdb, uri, content, type, HttpMethod.PUT);
  }
  
  /**
   * Returns an HttpQuery object with the given uri, content and type
   * Method = DELETE
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @return an HttpQuery object
   */
  public static HttpQuery deleteQuery(final TSDB tsdb, final String uri, 
      final String content) {
    return deleteQuery(tsdb, uri, content, "application/json; charset=UTF-8");
  }
  
  /**
   * Returns an HttpQuery object with the given uri, content and type
   * Method = DELETE
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @param type Content-Type value
   * @return an HttpQuery object
   */
  public static HttpQuery deleteQuery(final TSDB tsdb, final String uri, 
      final String content, final String type) {
    return contentQuery(tsdb, uri, content, type, HttpMethod.DELETE);
  }
  
  /**
   * Returns an HttpQuery object with the given settings
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @param type Content-Type value
   * @param method The HTTP method to use, GET, POST, etc.
   * @return an HttpQuery object
   */
  public static HttpQuery contentQuery(final TSDB tsdb, final String uri, 
      final String content, final String type, final HttpMethod method) {
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        method, uri);
    if (content != null) {
    	req.content().writeBytes(bufferManager.wrap(content));
    }
    req.headers().set("Content-Type", type);
    return new HttpQuery(tsdb, req, channelMock);
  }

  /** @param the query to mock a future callback for */
  public static void mockChannelFuture(final HttpQuery query) {
    final DefaultChannelPromise future = new DefaultChannelPromise(query.channel());
    when(query.channel().write(any(ByteBuf.class))).thenReturn(future);
    future.setSuccess();
  }
  
// /**
//  * Creates an embedded channel with the passed handler, writes an object to it and returns the response 
//  * @param writeObject The object to write the channel
//  * @param handlers The handlers to attach to the embedded channel 
//  * @return the object read from the embedded channel
//  */
//  @SuppressWarnings("unchecked")
//  public static <T> T writeReadEmbeddedChannel(final Object writeObject, final ChannelHandler...handlers) {
//	  final EmbeddedChannel ec = new EmbeddedChannel(handlers);
//	  ec.writeInbound(writeObject);
//	  ec.runPendingTasks();
//	  return (T)ec.readOutbound();
//  }
//  
// /**
//  * Writes the passed object to an instance of the {@link RpcHandler} handler in an embedded channel 
//  * and returns the value read back from the channel
//  * @param writeObject The object to write to the handler
//  * @param tsdb The TSDB instance to create the RpcHandler with
//  * @return The object read back from the channel
//  */
//  @SuppressWarnings("unchecked")
//  public static <T> T writeThenReadFromRpcHandler(final Object writeObject, final TSDB tsdb) {
//	  final EmbeddedChannel ec = rpcHandlerChannel(tsdb);
//	  ec.writeInbound(writeObject);
//	  ec.runPendingTasks();
//	  final Object response = ec.readOutbound();
//	  return (T)response;	  
//  }
//  
//  /**
//   * Writes the passed object to an embedded channel pipeline of handlers  
//   * and returns the value read back from the channel
//   * @param writeObject The object to write to the handler
//   * @param handlers The channel handlers to install into the embedded channel
//   * @return The object read back from the channel
//   */
//   @SuppressWarnings("unchecked")
//   public static <T> T writeThenReadFromHandlers(final Object writeObject, final ChannelHandler...handlers) {
// 	  final EmbeddedChannel ec = new EmbeddedChannel(handlers);
// 	  ec.writeInbound(writeObject);
// 	  ec.runPendingTasks();
// 	  return (T)ec.readOutbound();	  
//   }
  
	/**
	 * Creates a new EmbeddedChannel containing the specified HttpRpc
	 * @param tsdb The TSDB to test against
	 * @param httpRpc The HttpRpc instance to invoke
	 * @return The EmbeddedChannel, ready to test
	 */
	public static EmbeddedChannel testChannel(final TSDB tsdb, final HttpRpc httpRpc) {
		final ChannelDuplexHandler rpcWrapper = new ChannelDuplexHandler() {
			@Override
			public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
				final FullHttpRequest request = (FullHttpRequest)msg;
				final HttpQuery query = new HttpQuery(tsdb, request, ctx);
				httpRpc.execute(tsdb, query);
			}
		};
		return new EmbeddedChannel(rpcWrapper);
	}
	
	/**
	 * Creates a new EmbeddedChannel containing the specified HttpRpc, 
	 * writes the passed inbound objects into it, and returns the response.
	 * @param tsdb The TSDB to test against
	 * @param httpRpc The HttpRpc instance to invoke
	 * @param inbound The inbound objects to write
	 * @return the HttpRpc response
	 */
	@SuppressWarnings("unchecked")
	public static <T> T writeThenReadFromChannel(final TSDB tsdb, final HttpRpc httpRpc, final Object...inbound) {
		final EmbeddedChannel ec = testChannel(tsdb, httpRpc);
		try {
			ec.writeInbound(inbound);
			ec.runPendingTasks();
			T t = ec.readOutbound();
			return t;
		} catch (Exception ex) {
			return (T)handleException(ex);
		}
	}
	
	/**
	 * Handles an exception thrown from the direct invocation of the target HttpRpc
	 * @param ex The thrown exception
	 * @return the HttpResponse representing the thrown exception
	 */
	public static DefaultFullHttpResponse handleException(final Exception ex) {
		try {
			throw ex;
		} catch (BadRequestException brex) {			
			final DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, brex.getStatus());
			ByteBufUtil.writeUtf8(response.content(), brex.getMessage());
			final String details = brex.getDetails();
			if(details!=null && !details.trim().isEmpty()) {
				ByteBufUtil.writeUtf8(response.content(), "|");
				ByteBufUtil.writeUtf8(response.content(), details.trim());
			}
			return response;
		} catch (Exception exx) {
			final DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
			ByteBufUtil.writeUtf8(response.content(), exx.getMessage());
			return response;
		}
	}
	
	/**
	 * Creates a new EmbeddedChannel containing the specified HttpRpc, 
	 * writes the passed inbound objects into it, and returns the response.
	 * @param tsdb The TSDB to test against
	 * @param httpRpc The HttpRpc instance to invoke
	 * @param inbound The inbound objects to write
	 * @return The EmbeddedChannel, ready to be read from
	 */
	public static EmbeddedChannel writeToChannel(final TSDB tsdb, final HttpRpc httpRpc, final Object...inbound) {
		final EmbeddedChannel ec = testChannel(tsdb, httpRpc);
		try {			
			ec.writeInbound(inbound);
			ec.runPendingTasks();			
		} catch (Exception ex) {
			final DefaultFullHttpResponse response = handleException(ex);
			ec.writeOutbound(response);
		}
		return ec;
	}
  
  
  
 /**
  * Creates a Netty EmbeddedChannel that routes passed messages to an instance of a {@link RpcHandler} handler.
  * @param tsdb The [mocked] TSDB instance
  * @return The embedded channel, ready for writing to and reading from
  */
  public static EmbeddedChannel rpcHandlerChannel(final TSDB tsdb) {
	  final RpcManager rpcManager = RpcManager.instance(tsdb);
	  final RpcHandler rpcHandler = new RpcHandler(tsdb, rpcManager);
	  return new EmbeddedChannel(rpcHandler);
  }
  
  
  static class CtorDefaultChannelPipeline extends DefaultChannelPipeline {

	protected CtorDefaultChannelPipeline(final Channel channel) {
		super(channel);
	}
	  
  }
  
}
