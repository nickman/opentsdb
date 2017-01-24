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
package net.opentsdb.tsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.nio.charset.Charset;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import net.opentsdb.core.BaseTsdbTest;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: Netty4MocksTest</p>
 * <p>Description: Testing Replacement for {@link NettyMocks} for Netty 4</p> 
 * <p><code>net.opentsdb.tsd.Netty4MocksTest</code></p>
 */

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
    "ch.qos.*", "org.slf4j.*",
    "com.sum.*", "org.xml.*", "com.fasterxml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, HttpQuery.class})
public class Netty4MocksTest extends BaseTestPutRpc {
	  
	  final BufferManager bufferManager = BufferManager.newInstance();
	  final static Method m = MemberMatcher.method(HttpQuery.class,
              "sendBuffer",
              HttpResponseStatus.class, ByteBuf.class, String.class);
	  public static final Charset UTF8 = Charset.forName("UTF8");
	  
	
	  
	  
	  
	  public static HttpQuery returnSelfQuery(final TSDB tsdb, final FullHttpRequest request) {
		  final EmbeddedChannel chan = new EmbeddedChannel();
		  final HttpQuery q = new HttpQuery(tsdb, request, chan);
		  final Answer<Void> answer = new Answer<Void>(){
			  @Override
			public Void answer(final InvocationOnMock invocation) throws Throwable {
				final HttpQuery q = (HttpQuery)invocation.getMock();
				final Object[] args = invocation.getArguments();
				final FullHttpResponse response = q.response();
				response.content().writeBytes((ByteBuf)args[1]);
				response.setStatus((HttpResponseStatus)args[0]);
				invocation.callRealMethod();
				return null;
			}
		  };
		  
		  try {
			final HttpQuery query = PowerMockito.spy(q);
			PowerMockito.doAnswer(answer).when(query).sendBuffer(Mockito.any(HttpResponseStatus.class), Mockito.any(ByteBuf.class), Mockito.anyString());
			return query;
		} catch (Exception e) {
			e.printStackTrace(System.err);
			throw new RuntimeException("Failed to create HttpQuery", e);
		}
	  }
	  
	  public static HttpQuery getQuery(final TSDB tsdb, final String uri) {
		    
		    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
		        HttpMethod.GET, uri);
		    return returnSelfQuery(tsdb, req);
		  }
	  
	  public static HttpQuery contentQuery(final TSDB tsdb, final String uri, 
		      final String content, final String type, final HttpMethod method) {
		    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
		        method, uri);
		    if (content != null) {
		    	req.content().writeBytes(BufferManager.getInstance().wrap(content));
		    }
		    req.headers().set("Content-Type", type);
		    return returnSelfQuery(tsdb, req);
		  }	  
	  
	  public static HttpQuery postQuery(final TSDB tsdb, final String uri, 
		      final String content) {
		    return postQuery(tsdb, uri, content, "application/json; charset=UTF-8");
		  }
		  
		  public static HttpQuery postQuery(final TSDB tsdb, final String uri, 
		      final String content, final String type) {
		    return contentQuery(tsdb, uri, content, type, HttpMethod.POST);
		  }
	  

	  @Test
	  public void sendReplyStatusCB() throws Exception {
	    HttpQuery query = getQuery(tsdb, "/");
	    ByteBuf cb = bufferManager.wrap("Hello World", UTF8);
	    
	    query.sendReply(HttpResponseStatus.CREATED, cb);	    
	    assertEquals(HttpResponseStatus.CREATED, query.response().status());
	    assertEquals("Hello World", query.response().content().toString(UTF8));
	  }
	  
	  @Test
	  public void putSingleDetails() throws Exception {
		final HttpRpc rpc = new PutDataPointRpc(tsdb.getConfig());
	    HttpQuery query = postQuery(tsdb, "/api/put?details", 
	            "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
	                +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
	    rpc.execute(tsdb, query);
	    assertEquals(HttpResponseStatus.OK, query.response().status());
	    final String response = 
	    		query.response().content().toString(NettyMocks.UTF8);
	    assertTrue(response.contains("\"failed\":0"));
	    assertTrue(response.contains("\"success\":1"));
	    assertTrue(response.contains("\"errors\":[]"));
	  }
	  
	
	
}
