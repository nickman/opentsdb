// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Method;

import org.hbase.async.HBaseClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.net.HttpHeaders;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, Config.class, HBaseClient.class, RpcHandler.class,
  HttpQuery.class, DefaultHttpResponse.class, 
  ChannelHandlerContext.class })
public final class TestRpcHandler {
  private TSDB tsdb = null;
  private RpcManager rpc_manager;
  private ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
  private HBaseClient client = mock(HBaseClient.class);
  
  
  @Before
  public void before() throws Exception {
    final Config config = new Config(false);
    tsdb = new TSDB(client, config);
    rpc_manager = RpcManager.instance(tsdb);
  }
  
  @After
  public void after() {
    rpc_manager.shutdown();
  }
  
  @Test
  public void ctorDefaults() {
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    assertNotNull(rpc);
  }
  
  @Test
  public void ctorCORSPublic() {
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", "*");
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    assertNotNull(rpc);
  }
  
  @Test
  public void ctorCORSSeparated() {
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", 
        "aurther.com,dent.net,beeblebrox.org");
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    assertNotNull(rpc);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorCORSPublicAndDomains() {
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", 
        "*,aurther.com,dent.net,beeblebrox.org");
    new RpcHandler(tsdb, rpc_manager);
  }
  
  @Test
  public void httpCORSIgnored() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.status());
          assertNull(response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    rpc.channelRead(ctx, req); 
  }
  
  //FullHttpResponse writeReqestReadResponse(final FullHttpRequest request, final ChannelHandler...handlers) {

  @Test
  public void httpCORSPublicSimple() throws Exception {	
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", "*");
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    final HttpQuery q = NettyMocks.getQuery(tsdb, "/api/v1/version");
    q.request().headers().add(HttpHeaders.ORIGIN, "42.com");
	final FullHttpResponse httpResponse = NettyMocks.writeReqestReadResponse(
			q.request(),
			rpc);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertEquals("42.com", 
    		httpResponse.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
  }
  
  
//  @Test
//  public void httpCORSPublicSimple() throws Exception {	
//    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
//        HttpMethod.GET, "/api/v1/version");
//    req.headers().add(HttpHeaders.ORIGIN, "42.com");
//
//    handleHttpRpc(req,
//      new Answer<ChannelFuture>() {
//        public ChannelFuture answer(final InvocationOnMock args) 
//          throws Throwable {
//          DefaultHttpResponse response = 
//            (DefaultHttpResponse)args.getArguments()[0];
//          assertEquals(HttpResponseStatus.OK, response.status());
//          assertEquals("42.com", 
//              response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
//          return null;
//        }        
//      }
//    );
//    
//    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", "*");
//    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
//    rpc.channelRead(ctx, req);
//  }
  
  @Test
  public void httpCORSSpecificSimple() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.status());
          assertEquals("42.com", 
              response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", 
        "aurther.com,dent.net,42.com,beeblebrox.org");
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    rpc.channelRead(ctx, req);
  }
  
  @Test
  public void httpCORSNotAllowedSimple() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.status());
          assertNull(response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", 
        "aurther.com,dent.net,beeblebrox.org");
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    rpc.channelRead(ctx, req);
  }
  
  @Test
  public void httpOptionsNoCORS() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED, response.status());
          assertNull(response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    rpc.channelRead(ctx, req);
  }
  
  @Test
  public void httpOptionsCORSNotConfigured() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");
    
    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED, response.status());
          assertNull(response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    rpc.channelRead(ctx, req);
  }
  
  @Test
  public void httpOptionsCORSPublic() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");
    
    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.status());
          assertEquals("42.com", 
              response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", "*");
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    rpc.channelRead(ctx, req);
  }
  
  @Test
  public void httpOptionsCORSSpecific() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");
    
    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.status());
          assertEquals("42.com", 
              response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", 
      "aurther.com,dent.net,42.com,beeblebrox.org");
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    rpc.channelRead(ctx, req);
  }
  
  @Test
  public void httpOptionsCORSNotAllowed() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");
    
    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.status());
          assertNull(response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", 
      "aurther.com,dent.net,beeblebrox.org");
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    rpc.channelRead(ctx, req);
  }
  
  @Test
  public void createQueryInstanceForBuiltin() throws Exception {
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    final Channel mockChan = NettyMocks.fakeChannel();
    final Method meth = Whitebox.getMethod(RpcHandler.class, "createQueryInstance", 
        TSDB.class, HttpRequest.class, Channel.class);
    AbstractHttpQuery query = (AbstractHttpQuery) meth.invoke(
        rpc, tsdb, 
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/api/v1/version"), 
        mockChan);
    assertTrue(query instanceof HttpQuery);
    
    query = (AbstractHttpQuery) meth.invoke(
        rpc, tsdb, 
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/api/version"), 
        mockChan);
    assertTrue(query instanceof HttpQuery);
    
    query = (AbstractHttpQuery) meth.invoke(
        rpc, tsdb, 
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/q"), 
        mockChan);
    assertTrue(query instanceof HttpQuery);
    
    query = (AbstractHttpQuery) meth.invoke(
        rpc, tsdb, 
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/"), 
        mockChan);
    assertTrue(query instanceof HttpQuery);
  }
  
  @Test(expected=BadRequestException.class)
  public void createQueryInstanceEmptyRequestInvalid() throws Exception {
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    final Channel mockChan = NettyMocks.fakeChannel();
    final Method meth = Whitebox.getMethod(RpcHandler.class, "createQueryInstance", 
        TSDB.class, HttpRequest.class, Channel.class);
    meth.invoke(
        rpc, tsdb, 
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, ""), 
        mockChan);
  }
  
  @Test
  public void emptyPathIsBadRequest() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "");
    
    final Channel mockChan = handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
              (DefaultHttpResponse)args.getArguments()[0];
            assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
            return null; // EventExecutor.newSucceededFuture((Channel) args.getMock());
            //return new SucceededChannelFuture();
        }        
      }
    );
    
    final RpcHandler rpc = new RpcHandler(tsdb, rpc_manager);
    Whitebox.invokeMethod(rpc, "handleHttpQuery", tsdb, mockChan, req);
  }
  
  private Channel handleHttpRpc(final HttpRequest req, final Answer<?> answer) {
    final Channel channel = NettyMocks.fakeChannel();
//    when(message.getMessage()).thenReturn(req);
//    when(message.getChannel()).thenReturn(channel);
    when(channel.write((DefaultHttpResponse)any())).thenAnswer(answer);
    return channel;
  }
}