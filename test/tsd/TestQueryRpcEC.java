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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.query.expression.ExpressionTree;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

/**
 * Reimplemented to use EmbeddedChannels.
 * Unit tests for the Query RPC class that handles parsing user queries for
 * timeseries data and returning that data
 * <b>Note:</b> Testing query validation and such should be done in the 
 * core.TestTSQuery and TestTSSubQuery classes
 */
@PowerMockIgnore({"javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, Config.class, HttpQuery.class, Query.class, 
  Deferred.class, TSQuery.class, DateTime.class, DeferredGroupException.class })
public class TestQueryRpcEC {
	  private TSDB tsdb = null;
	  private QueryRpc rpc;
	  private Query empty_query = mock(Query.class);
	  private Query query_result;
	  private List<ExpressionTree> expressions;
	  
	  private static final Method parseQuery;
	  static {
	    try {
	      parseQuery = QueryRpc.class.getDeclaredMethod("parseQuery", 
	          TSDB.class, HttpQuery.class, List.class);
	      parseQuery.setAccessible(true);
	    } catch (Exception e) {
	      throw new RuntimeException("Failed in static initializer", e);
	    }
	  }
	  
	  @Before
	  public void before() throws Exception {
	    tsdb = NettyMocks.getMockedHTTPTSDB();
	    HttpQuery.initializeSerializerMaps(tsdb);
	    empty_query = mock(Query.class);
	    query_result = mock(Query.class);
	    rpc = new QueryRpc();
	    expressions = null;
	    
	    when(tsdb.newQuery()).thenReturn(query_result);
	    when(empty_query.run()).thenReturn(new DataPoints[0]);
	    when(query_result.configureFromQuery((TSQuery)any(), anyInt()))
	      .thenReturn(Deferred.fromResult(null));
	    when(query_result.runAsync())
	      .thenReturn(Deferred.fromResult(new DataPoints[0]));
	  }
	  
//	  @Test
//	  public void responseStatus() throws Exception {
//		  org.jboss.netty.handler.codec.http.HttpResponseStatus status = new org.jboss.netty.handler.codec.http.HttpResponseStatus(200, "OK");
//		  System.out.println(JSON.serializeToString(status));
//	  }
	  
	  public static final Charset UTF8 = Charset.forName("UTF8");

	  @Test
	  public void postQuerySimplePassEC() throws Exception {
		    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query",
		            "{\"start\":1425440315306,\"queries\":" +
		              "[{\"metric\":\"somemetric\",\"aggregator\":\"sum\",\"rate\":true," +
		              "\"rateOptions\":{\"counter\":false}}]}");
		  final FullHttpResponse response = NettyMocks.writeThenReadFromRpcHandler(query.request(), tsdb);
		  assertEquals(HttpResponseStatus.OK, response.status());
		  
	  }
	  
	  
}
