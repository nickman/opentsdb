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
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import net.opentsdb.core.TSDB;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
	  "ch.qos.*", "org.slf4j.*",
	  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class})
public final class TestSuggestRpc {
  private TSDB tsdb = null;
  private SuggestRpc rpc = null;
  
  public static final Charset UTF8 = Charset.forName("UTF-8");
  
  @Before
  public void before() throws Exception {
    rpc = new SuggestRpc();
    tsdb = NettyMocks.getMockedHTTPTSDB();
    final List<String> metrics = new ArrayList<String>();
    metrics.add("sys.cpu.0.system"); 
    metrics.add("sys.mem.free");
    when(tsdb.suggestMetrics("s")).thenReturn(metrics);
    final List<String> metrics_one = new ArrayList<String>();
    metrics_one.add("sys.cpu.0.system"); 
    when(tsdb.suggestMetrics("s", 1)).thenReturn(metrics_one);
    final List<String> tagks = new ArrayList<String>();
    tagks.add("host");
    when(tsdb.suggestTagNames("h")).thenReturn(tagks);
    final List<String> tagvs = new ArrayList<String>();
    tagvs.add("web01.mysite.com");
    when(tsdb.suggestTagValues("w")).thenReturn(tagvs);
  }
  
  @Test
  public void metricsQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=metrics&q=s");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertEquals("[\"sys.cpu.0.system\",\"sys.mem.free\"]", 
        httpResponse.content().toString(UTF8));
  }
  
  @Test
  public void metricsPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"type\":\"metrics\",\"q\":\"s\"}", "application/json");
        final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertEquals("[\"sys.cpu.0.system\",\"sys.mem.free\"]", 
        httpResponse.content().toString(UTF8));
  }

  @Test
  public void metricQSMax() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=metrics&q=s&max=1");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertEquals("[\"sys.cpu.0.system\"]", 
        httpResponse.content().toString(UTF8));
  }
  
  @Test
  public void metricsPOSTMax() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"type\":\"metrics\",\"q\":\"s\",\"max\":1}", "application/json");
        final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertEquals("[\"sys.cpu.0.system\"]", 
        httpResponse.content().toString(UTF8));
  }
  
  @Test
  public void tagkQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=tagk&q=h");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertEquals("[\"host\"]", 
        httpResponse.content().toString(UTF8));
  }
  
  @Test
  public void tagkPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"type\":\"tagk\",\"q\":\"h\"}", "application/json");
        final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertEquals("[\"host\"]", 
        httpResponse.content().toString(UTF8));
  }
  
  @Test
  public void tagvQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=tagv&q=w");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertEquals("[\"web01.mysite.com\"]", 
        httpResponse.content().toString(UTF8));
  }
  
  @Test
  public void tagvPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"type\":\"tagv\",\"q\":\"w\"}", "application/json");
        final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertEquals("[\"web01.mysite.com\"]", 
        httpResponse.content().toString(UTF8));
  }
  
  @Test
  public void badMethod() throws Exception {
    final HttpQuery query = NettyMocks.putQuery(tsdb, "/api/suggest", "type=metrics&q=h");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED, httpResponse.status());
  }
  
  @Test
  public void missingType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?q=h");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
  }
  
  @Test
  public void missingContent() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "", "application/json");
        final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
  }
  
  @Test
  public void badType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=doesnotexist&q=h");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
  }
  
  @Test
  public void missingTypePOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"q\":\"w\"}", "application/json");
        final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
  }

  @Test
  public void badMaxQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=tagv&q=w&max=foo");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
  }
  
  @Test
  public void badMaxPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"type\":\"metrics\",\"q\":\"s\",\"max\":\"foo\"}", 
        "application/json");
        final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
  }
}
