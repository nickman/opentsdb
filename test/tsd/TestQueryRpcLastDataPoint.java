// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.Charset;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import net.opentsdb.core.BaseTsdbTest;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TestTSUIDQuery;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, HBaseClient.class, Config.class, HttpQuery.class, 
  Query.class, Deferred.class, UniqueId.class, DateTime.class, KeyValue.class,
  Scanner.class })
public class TestQueryRpcLastDataPoint extends BaseTsdbTest {
  private QueryRpc rpc;
  
  public static Charset UTF8 = Charset.forName("UTF8");
  
  @Before
  public void beforeLocal() throws Exception {
    Whitebox.setInternalState(config, "enable_tsuid_incrementing", true);
    Whitebox.setInternalState(config, "enable_realtime_ts", true);
    rpc = new QueryRpc();
    storage = new MockBase(tsdb, client, true, true, true, true);
    TestTSUIDQuery.setupStorage(tsdb, storage);
  }
  
  @Test
  public void qsMetricMeta() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricMetaScan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricMetaScanResolve() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user&resolve");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
    assertTrue(json.contains("\"tags\":{\"host\":\"web02\"}"));
  }
  
  @Test
  public void qsMetricMetaScanOneMissing() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertFalse(json.contains("\"value\":\"42\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricMetaScanNoResults() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals("[]", json);
  }
  
  @Test
  public void qsMetricMetaScanBackscanZero() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user&back_scan=0");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}&back_scan=1");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricBackscanResolved() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}"
        + "&back_scan=1&resolve=true");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
  }
  
  @Test
  public void qsMetricBackscanNoResult() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}&back_scan=1");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertEquals("[]", json);
  }
  
  @Test
  public void qsMetricTwoQueriesBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}"
            + "&timeseries=sys.cpu.user{host=web02}&back_scan=1");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricTwoQueriesBackscanResolve() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}"
            + "&timeseries=sys.cpu.user{host=web02}&back_scan=1&resolve");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
    assertTrue(json.contains("\"tags\":{\"host\":\"web02\"}"));
  }
  
  @Test
  public void qsMetricTwoQueriesOneMissingBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}"
            + "&timeseries=sys.cpu.user{host=web02}&back_scan=1");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertFalse(json.contains("\"value\":\"42\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricBackscanMissingTags() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user&back_scan=1");
    try {
      final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
      assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("Tags"));
    }
  }
  
  @Test
  public void qsMetricNSUNMetric() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.nice{host=web01}");
    try {
      final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
      assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("No such name"));
      assertTrue(e.getMessage().contains("metric"));
    }
  }
  
  @Test
  public void qsMetricNSUNTagk() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{dc=web01}");
    try {
      final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
      assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("No such name"));
      assertTrue(e.getMessage().contains("tagk"));
    }
  }
  
  @Test
  public void qsMetricNSUNTagv() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web03}");
    try {
      final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
      assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("No such name"));
      assertTrue(e.getMessage().contains("tagv"));
    }
  }
  
  @Test
  public void qsTSUIDMeta() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"value\":\"24\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDMetaCommaSeparated() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001,000001000001000002");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDMetaTwoQueries() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001&tsuids=000001000001000002");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDMetaNoResults() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals("[]", json);
  }
  
  @Test
  public void qsTSUIDBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001&back_scan=1");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDBackscanNoResult() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001&back_scan=1");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertEquals("[]", json);
  }
  
  @Test
  public void qsTSUIDCommaSeparatedBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001,000001000001000002&back_scan=1");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDCommaSeparatedOneMissingBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001,000001000001000002&back_scan=1");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"value\":\"24\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDTwoQueriesBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001"
        + "&tsuids=000001000001000002&back_scan=1");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDTwoQueriesOneMissingBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001"
        + "&tsuids=000001000001000002&back_scan=1");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"value\":\"24\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDNSUIMetric() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    storage.addColumn(MockBase.stringToBytes("00000350E22700000001000001"), 
        new byte[] { 0, 0 }, new byte[] { 0x2A });
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000003000001000001&back_scan=1&resolve");
    try {
      final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
      assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("No such unique ID"));
      assertTrue(e.getMessage().contains("metric"));
    }
  }
  
  @Test
  public void qsTSUIDNSUITagk() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    storage.addColumn(MockBase.stringToBytes("00000150E22700000004000001"), 
        new byte[] { 0, 0 }, new byte[] { 0x2A });
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000004000001&back_scan=1&resolve");
    try {
      final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
      assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("No such unique ID"));
      assertTrue(e.getMessage().contains("tagk"));
    }
  }
  
  @Test
  public void qsTSUIDNSUITagv() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    storage.addColumn(MockBase.stringToBytes("00000150E22700000001000003"), 
        new byte[] { 0, 0 }, new byte[] { 0x2A });
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000003&back_scan=1&resolve");
    try {
      final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
      assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("No such unique ID"));
      assertTrue(e.getMessage().contains("tagv"));
    }
  }
  
  @Test
  public void qsDualMeta() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}"
        + "&tsuids=000001000001000002");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsDualBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}"
        + "&tsuids=000001000001000002&back_scan=1");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsEmpty() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/api/query/last");
    try {
      final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
      assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
    } catch (BadRequestException e) { }
  }
  
  @Test
  public void postMetricMetaWithTags() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\",\"tags\":"
        + "{\"host\":\"web01\"}}]}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postMetricMetaWithoutTags() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\"}]}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postMetricMetaWithoutTagsResolve() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\"}],\"resolveNames\":true}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
    assertTrue(json.contains("\"tags\":{\"host\":\"web02\"}"));
  }
  
  @Test
  public void postMetricMetaTwoQueries() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\",\"tags\":"
        + "{\"host\":\"web01\"}},"
        + "{\"metric\":\"sys.cpu.user\",\"tags\":"
        + "{\"host\":\"web02\"}}]}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postMetricBackscanWithTags() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\",\"tags\":"
        + "{\"host\":\"web01\"}}],\"backScan\":1}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postTSUIDMeta() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"tsuids\":[\"000001000001000001\"]}]}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"value\":\"24\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postTSUIDMetaList() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"tsuids\":[\"000001000001000001\","
        + "\"000001000001000002\"]}]}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postTSUIDMetaResolve() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"tsuids\":[\"000001000001000001\","
        + "\"000001000001000002\"]}],\"resolveNames\":true}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
    assertTrue(json.contains("\"tags\":{\"host\":\"web02\"}"));
  }
  
  @Test
  public void postTSUIDMetaTwoQueries() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"tsuids\":[\"000001000001000001\"]},"
        + "{\"tsuids\":[\"000001000001000002\"]}]}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postTSUIDBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"tsuids\":[\"000001000001000001\"]}],\"backScan\":1}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"value\":\"24\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postDualMeta() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\",\"tags\":"
        + "{\"host\":\"web01\"}},"
        + "{\"tsuids\":[\"000001000001000002\"]}]}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postDualMetaResolve() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\",\"tags\":"
        + "{\"host\":\"web01\"}},"
        + "{\"tsuids\":[\"000001000001000002\"]}],"
        + "\"resolveNames\":true}");
    final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
    final String json = getContent(httpResponse);
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
    assertTrue(json.contains("\"tags\":{\"host\":\"web02\"}"));
  }
  
  @Test
  public void postEmpty() throws Exception {
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[]}");
    try {
      final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
      assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
      
    } catch (BadRequestException e) { }
  }
  
  @Test
  public void postEmptyList() throws Exception {
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{}");
    try {
      final FullHttpResponse httpResponse = NettyMocks.writeThenReadFromChannel(tsdb, rpc, query.request());
      assertEquals(HttpResponseStatus.BAD_REQUEST, httpResponse.status());
    } catch (BadRequestException e) { }
  }
  
  /**
   * Returns the content of the response buffer
   * @param query The query to parse
   * @return Some string if we were lucky
   */
  private String getContent(final HttpQuery query) {
    return query.response().content().toString(UTF8);
  }
  
  /**
   * Returns the content of the response buffer
   * @param response The http response to read from
   * @return Some string if we were lucky
   */
  private String getContent(final FullHttpResponse request) {
    return request.content().toString(UTF8);
  }
  
}