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

//import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.Ignore;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
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
@Ignore
public final class NettyMocks {
   /** ByteBuf allocator */
   public static final BufferManager bufferManager = BufferManager.newInstance();
   /** The UTF8 Character set */
   public static final Charset UTF8 = Charset.forName("UTF8");
   
   private static final Splitter WEBPATH_SPLITTER = Splitter.on('/')
		      .trimResults()
		      .omitEmptyStrings();
   
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
  
  public static HttpQuery returnUpdatingQuery(final TSDB tsdb, final FullHttpRequest request) {
	  final EmbeddedChannel chan = new EmbeddedChannel();
	  final HttpQuery q = new HttpQuery(tsdb, request, chan);
	  final Answer<Void> sendBufferIntercept = new Answer<Void>(){
		  @Override
		public Void answer(final InvocationOnMock invocation) throws Throwable {
			final HttpQuery q = (HttpQuery)invocation.getMock();
			Whitebox.setInternalState(q.serializer().query, "api_version", q.apiVersion());
			final Object[] args = invocation.getArguments();
			final FullHttpResponse response = q.response();
			response.content().writeBytes((ByteBuf)args[1]);
			response.setStatus((HttpResponseStatus)args[0]);
			invocation.callRealMethod();
			return null;
		}
	  };
	  final Answer<Void> sendBufferUpdateApiVersion = new Answer<Void>(){
		  @Override
		public Void answer(final InvocationOnMock invocation) throws Throwable {
			final HttpQuery q = (HttpQuery)invocation.getMock();
			Whitebox.setInternalState(q.serializer().query, "api_version", q.apiVersion());
			invocation.callRealMethod();
			return null;
		}
	  };
	  
	  final HttpQuery query = PowerMockito.spy(q);
	  PowerMockito.doAnswer(sendBufferIntercept)
	  	.when(query).sendBuffer(Mockito.any(HttpResponseStatus.class), Mockito.any(ByteBuf.class), Mockito.anyString());
	  when(query.response()).thenCallRealMethod();
	  PowerMockito.doAnswer(sendBufferUpdateApiVersion)
	  	.when(query).sendBuffer(Mockito.any(HttpResponseStatus.class), Mockito.any(ByteBuf.class));
	  when(query.response()).thenCallRealMethod();
	  
//	  try {
////		  PowerMockito.doCallRealMethod().when(query.method());
////		  PowerMockito.when(query.apiVersion()).thenCallRealMethod();
////		  PowerMockito.when(query.channel()).thenCallRealMethod();
////		  PowerMockito.when(query.explodeAPIPath()).thenCallRealMethod();
////		  PowerMockito.when(query.explodePath()).thenCallRealMethod();
////		  PowerMockito.when(query.request()).thenCallRealMethod();
////		  PowerMockito.when(query.response()).thenCallRealMethod();
////		  PowerMockito.when(query.ctx()).thenCallRealMethod();
////		  PowerMockito.when(query.method()).thenCallRealMethod();
//		  PowerMockito.doAnswer(sendBufferUpdateApiVersion)
//		  	//.when(query, PowerMockito.method(HttpQuery.class, "sendBuffer", HttpResponseStatus.class, ByteBuf.class));
//		  .when(query.sendBuffer(Mockito.any(HttpResponseStatus.class), Mockito.any(ByteBuf.class)));
//		  PowerMockito.doCallRealMethod().when(query.method());
//	  } catch (Exception ex) {
//		  throw new RuntimeException("Failed to mock method sendBuffer(status, method)", ex);
//	  }
	  return query;
  }
  
  public static FullHttpResponse writeReqestReadResponse(final FullHttpRequest request, final ChannelHandler...handlers) {
	  final EmbeddedChannel ec = new EmbeddedChannel(handlers);
	  ec.writeInbound(request);
	  ec.runPendingTasks();
	  return ec.readOutbound();	  
  }
  
  public static FullHttpResponse writeReqestReadResponse(final TSDB tsdb, final HttpQuery request, final HttpRpc rpc) {
	  final ChannelDuplexHandler handler = new ChannelDuplexHandler() {
		@Override
		public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
			rpc.execute(tsdb, (HttpQuery)msg);			
		}  
	  };
	  final EmbeddedChannel ec = new EmbeddedChannel(handler);
	  ec.writeInbound(request);
	  ec.runPendingTasks();
	  return ec.readOutbound();	  	  
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
				final HttpQuery query = (HttpQuery)createQueryInstance(tsdb, request, ctx);
				query.getQueryBaseRoute();  // Set API Version
				httpRpc.execute(tsdb, query);
			}
		};
		return new EmbeddedChannel(rpcWrapper);
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
	
	static boolean isHttpRpcPluginPath(final String uri) {
	    if (Strings.isNullOrEmpty(uri) || uri.length() <= RpcManager.PLUGIN_BASE_WEBPATH.length()) {
	      return false;
	    } else {
	      // Don't consider the query portion, if any.
	      int qmark = uri.indexOf('?');
	      String path = uri;
	      if (qmark != -1) {
	        path = uri.substring(0, qmark);
	      }

	      final List<String> parts = WEBPATH_SPLITTER.splitToList(path);
	      return (parts.size() > 1 && parts.get(0).equals(RpcManager.PLUGIN_BASE_WEBPATH));
	    }
	  }	
	
	 /**
	   * Using the request URI, creates a query instance capable of handling 
	   * the given request.
	   * @param tsdb the TSDB instance we are running within
	   * @param request the incoming HTTP request
	   * @param chan the {@link Channel} the request came in on.
	   * @return a subclass of {@link AbstractHttpQuery}
	   * @throws BadRequestException if the request is invalid in a way that
	   * can be detected early, here.
	   */
	  public static AbstractHttpQuery createQueryInstance(final TSDB tsdb,
	        final FullHttpRequest request,
	        final ChannelHandlerContext ctx) 
	            throws BadRequestException {
	    final String uri = request.uri();
	    if (Strings.isNullOrEmpty(uri)) {
	      throw new BadRequestException("Request URI is empty");
	    } else if (uri.charAt(0) != '/') {
	      throw new BadRequestException("Request URI doesn't start with a slash");
	    } else if (isHttpRpcPluginPath(uri)) {
	      return new HttpRpcPluginQuery(tsdb, request, ctx.channel());
	    } else {
	      HttpQuery builtinQuery = new HttpQuery(tsdb, request, ctx);
	      return builtinQuery;
	    }
	  }	
  
  
  /**
   * Returns a mocked Channel object that simply sets the name to
   * [fake channel]
   * @return A Channel mock
   */
  public static Channel fakeChannel() {
    final EmbeddedChannel chan = mock(EmbeddedChannel.class);
    when(chan.toString()).thenReturn("[fake channel]");
    when(chan.isOpen()).thenReturn(true);
    when(chan.isWritable()).thenReturn(true);
    
    final SocketAddress socket = mock(SocketAddress.class);
    when(socket.toString()).thenReturn("192.168.1.1:4243");
    when(chan.remoteAddress()).thenReturn(socket);
    return chan;
  }
  
  
  
  
  public static HttpRpcPluginQuery pluginQuery(final TSDB tsdb, final FullHttpRequest req) {
	  final EmbeddedChannel chan = new EmbeddedChannel();
	  return new HttpRpcPluginQuery(tsdb, req, chan);
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
	  final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
		        HttpMethod.GET, uri);
	  return returnUpdatingQuery(tsdb, req);	  
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
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri);
    if (content != null) {
    	req.content().writeBytes(BufferManager.getInstance().wrap(content));
    }
    if(type!=null) {
    	req.headers().set("Content-Type", type);
    }
	return returnUpdatingQuery(tsdb, req);
  }
  
  /**
   * Creates a new TRACE HttpQuery
   * @param tsdb The mocked TSDB to associate with
   * @param uri A UIR to use
   * @return an HttpQuery object
   */
 public static HttpQuery traceQuery(final TSDB tsdb, final String uri) {
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.TRACE, uri);
	return returnUpdatingQuery(tsdb, req);
  }
  

//  /** @param the query to mock a future callback for */
//  public static void mockChannelFuture(final HttpQuery query) {
//    final ChannelFuture future = new DefaultChannelPromise(query.channel(), false);
//    when(query.channel().write(any(ByteBuf.class))).thenReturn(future);
//    future.setSuccess();
//  }
  
}
