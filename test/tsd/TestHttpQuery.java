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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
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
import io.netty.util.CharsetUtil;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;
import net.opentsdb.utils.buffermgr.BufferManager;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, HttpQuery.class})
public final class TestHttpQuery {
  private TSDB tsdb = null;
  final BufferManager bufferManager = BufferManager.newInstance();
  public static final Charset UTF8 = Charset.forName("UTF8");
  final static private Method guessMimeTypeFromUri;
  static {
    try {
      guessMimeTypeFromUri = HttpQuery.class.getDeclaredMethod(
        "guessMimeTypeFromUri", String.class);
      guessMimeTypeFromUri.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  final static private Method guessMimeTypeFromContents;
  static {
    try {
      guessMimeTypeFromContents = HttpQuery.class.getDeclaredMethod(
        "guessMimeTypeFromContents", ByteBuf.class);
      guessMimeTypeFromContents.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  final static private Method sendBuffer;
  static {
    try {
      sendBuffer = HttpQuery.class.getDeclaredMethod(
        "sendBuffer", HttpResponseStatus.class, ByteBuf.class);
      sendBuffer.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  @Before
  public void before() throws Exception {
    tsdb = NettyMocks.getMockedHTTPTSDB();
  }
  
  @Test
  public void getQueryString() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    Map<String, List<String>> params = query.getQueryString();
    assertNotNull(params);
    assertEquals("value", params.get("param").get(0));
    assertEquals("value2", params.get("param2").get(0));
  }
  
  @Test
  public void getQueryStringEmpty() {
    Map<String, List<String>> params = 
      NettyMocks.getQuery(tsdb, "/api/v1/put").getQueryString();
    assertNotNull(params);
    assertEquals(0, params.size());
  }
  
  @Test
  public void getQueryStringMulti() {
    Map<String, List<String>> params = 
      NettyMocks.getQuery(tsdb, 
          "/api/v1/put?param=v1&param=v2&param=v3").getQueryString();
    assertNotNull(params);
    assertEquals(1, params.size());
    assertEquals(3, params.get("param").size());
  }
  
  @Test (expected = NullPointerException.class)
  public void getQueryStringNULL() {
    NettyMocks.getQuery(tsdb, null).getQueryString();
  }
  
  @Test
  public void getQueryStringParam() {
    assertEquals("value", 
        NettyMocks.getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2")
        .getQueryStringParam("param"));
  }
  
  @Test
  public void getQueryStringParamNull() {
    assertNull(NettyMocks.getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2").
        getQueryStringParam("nothere"));
  }
  
  @Test
  public void getRequiredQueryStringParam() {
    assertEquals("value", 
        NettyMocks.getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2").
        getRequiredQueryStringParam("param"));
  }
  
  @Test (expected = BadRequestException.class)
  public void getRequiredQueryStringParamMissing() {
    NettyMocks.getQuery(tsdb, "/api/v1/put?param=value&param2=value2").
      getRequiredQueryStringParam("nothere");
  }
  
  @Test
  public void hasQueryStringParam() {
    assertTrue(NettyMocks.getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2").
        hasQueryStringParam("param"));
  }
  
  @Test
  public void hasQueryStringMissing() {
    assertFalse(NettyMocks.getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2").
        hasQueryStringParam("nothere"));
  }
  
  @Test
  public void getQueryStringParams() {
    List<String> params = NettyMocks.getQuery(tsdb, 
        "/api/v1/put?param=v1&param=v2&param=v3").
      getQueryStringParams("param");
    assertNotNull(params);
    assertEquals(3, params.size());
  }
  
  @Test
  public void getQueryStringParamsNull() {
    List<String> params = NettyMocks.getQuery(tsdb, 
        "/api/v1/put?param=v1&param=v2&param=v3").
      getQueryStringParams("nothere");
    assertNull(params);
  }
  
  @Test
  public void getQueryPathA() {
    assertEquals("/api/v1/put", 
        NettyMocks.getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2").
        getQueryPath());
  }
  
  @Test
  public void getQueryPathB() {
    assertEquals("/", NettyMocks.getQuery(tsdb, "/").getQueryPath());
  }
  
  @Test (expected = NullPointerException.class)
  public void getQueryPathNull() {
    NettyMocks.getQuery(tsdb, null).getQueryPath();
  }
  
  @Test
  public void explodePath() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2");
    final String[] path = query.explodePath();
    assertNotNull(path);
    assertEquals(3, path.length);
    assertEquals("api", path[0]);
    assertEquals("v1", path[1]);
    assertEquals("put", path[2]);
  }
  
  @Test
  public void explodePathEmpty() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    final String[] path = query.explodePath();
    assertNotNull(path);
    assertEquals(1, path.length);
    assertEquals("", path[0]);
  }
  
  @Test (expected = NullPointerException.class)
  public void explodePathNull() {
    NettyMocks.getQuery(tsdb, null).explodePath();
  }
  
  @Test
  public void getQueryBaseRouteRoot() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    assertEquals("", query.getQueryBaseRoute());
    assertEquals(0, query.apiVersion());
  }
  
  @Test
  public void explodeAPIPath() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/v1/put?param=value&param2=value2");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertEquals("put", path[0]);
  }
  
  @Test
  public void explodeAPIPathNoVersion() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/put?param=value&param2=value2");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertEquals("put", path[0]);
  }
  
  @Test
  public void explodeAPIPathExtended() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/v1/uri/assign");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertEquals("uri", path[0]);
    assertEquals("assign", path[1]);
  }
  
  @Test
  public void explodeAPIPathExtendedNoVersion() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/uri/assign");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertEquals("uri", path[0]);
    assertEquals("assign", path[1]);
  }
  
  @Test
  public void explodeAPIPathCase() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/Api/Uri");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertEquals("Uri", path[0]);
  }
  
  @Test
  public void explodeAPIPathRoot() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertTrue(path[0].isEmpty());
  }
  
  @Test
  public void explodeAPIPathRootVersion() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/v1");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertTrue(path[0].isEmpty());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void explodeAPIPathNotAPI() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/q?hello=world");
    query.explodeAPIPath();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void explodeAPIPathHome() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/");
    query.explodeAPIPath();
  }
  
  @Test
  public void getQueryBaseRouteRootQS() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/?param=value");
    assertEquals("", query.getQueryBaseRoute());
    assertEquals(0, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteQ() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/q");
    assertEquals("q", query.getQueryBaseRoute());
    assertEquals(0, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteQSlash() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/q/");
    assertEquals("q", query.getQueryBaseRoute());
    assertEquals(0, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteLogs() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/logs");
    assertEquals("logs", query.getQueryBaseRoute());
    assertEquals(0, query.apiVersion());
  }
  
  @Test (expected = BadRequestException.class)
  public void getQueryBaseRouteAPIVNotImplemented() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/api/v3/put");
    assertEquals("api/put", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteAPICap() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/API/V1/PUT");
    assertEquals("api/put", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteAPIDefaultV() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/api/put");
    assertEquals("api/put", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteAPIQS() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/v1/put?metric=mine");
    assertEquals("api/put", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteAPINoEP() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/api");
    assertEquals("api", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteAPINoEPSlash() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/api/");
    assertEquals("api", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteFavicon() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/favicon.ico");
    assertEquals("favicon.ico", query.getQueryBaseRoute());
    assertEquals(0, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteVersion() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/api/version/query");
    assertEquals("api/version", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test (expected = BadRequestException.class)
  public void getQueryBaseRouteVBadNumber() {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/api/v2d/query");
    query.getQueryBaseRoute();
  }
  
  @Test (expected = NullPointerException.class)
  public void getQueryBaseRouteNull() {
    NettyMocks.getQuery(tsdb, null).getQueryBaseRoute();
  }
  
  @Test (expected = BadRequestException.class)
  public void getQueryBaseRouteBad() {
    NettyMocks.getQuery(tsdb, "notavalidquery").getQueryBaseRoute();
  }
  
  @Test (expected = BadRequestException.class)
  public void getQueryBaseRouteEmpty() {
    NettyMocks.getQuery(tsdb, "").getQueryBaseRoute();
  }
  
  @Test
  public void getCharsetDefault() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.headers().add("Content-Type", "text/plain");
    final HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    assertEquals(UTF8, query.getCharset());
  }
  
  @Test
  public void getCharsetDefaultNoHeader() {
    assertEquals(UTF8, 
        NettyMocks.getQuery(tsdb, "/").getCharset());
  }
  
  @Test
  public void getCharsetSupplied() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.headers().add("Content-Type", "text/plain; charset=UTF-16");
    final HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    assertEquals(Charset.forName("UTF-16"), query.getCharset());
  }
  
  @Test (expected = UnsupportedCharsetException.class)
  public void getCharsetInvalid() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.headers().add("Content-Type", "text/plain; charset=foobar");
    final HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    assertEquals(Charset.forName("UTF-16"), query.getCharset());
  }
  
  @Test
  public void hasContent() {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/", "Hello World", "");
    assertTrue(query.hasContent());
  }
  
  @Test
  public void hasContentFalse() {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/", null, "");
    assertFalse(query.hasContent());
  }
  
  @Test
  public void hasContentNotReadable() {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/", "", "");
    assertFalse(query.hasContent());
  }
  
  @Test
  public void getContentEncoding() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.headers().add("Content-Type", "text/plain; charset=UTF-16");
    final ByteBuf buf = bufferManager.wrap("S\u00ED Se\u00F1or", 
        CharsetUtil.UTF_16);
    req.content().writeBytes(buf);
    final HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    assertEquals("S\u00ED Se\u00F1or", query.getContentBuffer().toString(CharsetUtil.UTF_16));
  }
  
  @Test
  public void getContentDefault() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final ByteBuf buf = bufferManager.wrap("S\u00ED Se\u00F1or", 
        CharsetUtil.UTF_8);
    req.content().writeBytes(buf);
    final HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    assertEquals("S\u00ED Se\u00F1or", query.getContentBuffer().toString(CharsetUtil.UTF_8));
  }
  
  @Test
  public void getContentBadEncoding() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final ByteBuf buf = bufferManager.wrap("S\u00ED Se\u00F1or", 
            CharsetUtil.ISO_8859_1);
    req.content().writeBytes(buf);
    final HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    assertThat("S\u00ED Se\u00F1or", not(equalTo(query.getContent())));
  }
  
  
  @Test
  public void getContentEmpty() {
    assertTrue(NettyMocks.getQuery(tsdb, "/").getContentBuffer().readableBytes()==0);
  }
  
  @Test
  public void getAPIMethodGet() {
    assertEquals(HttpMethod.GET, 
        NettyMocks.getQuery(tsdb, "/").getAPIMethod());
  }
  
  @Test
  public void getAPIMethodPost() {
    assertEquals(HttpMethod.POST, 
        NettyMocks.postQuery(tsdb, "/", null).getAPIMethod());
  }
  
  @Test
  public void getAPIMethodPut() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.PUT, "/");
    HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    assertEquals(HttpMethod.PUT, query.getAPIMethod());
  }
  
  @Test
  public void getAPIMethodDelete() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.DELETE, "/");
    HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    assertEquals(HttpMethod.DELETE, query.getAPIMethod());
  }
  
  @Test
  public void getAPIMethodOverrideGet() {
    assertEquals(HttpMethod.GET, 
        NettyMocks.getQuery(tsdb, "/?method_override=get").getAPIMethod());
  }
  
  @Test
  public void getAPIMethodOverridePost() {
    assertEquals(HttpMethod.POST, 
        NettyMocks.getQuery(tsdb, "/?method_override=post").getAPIMethod());
  }
  
  @Test
  public void getAPIMethodOverridePut() {
    assertEquals(HttpMethod.PUT, 
        NettyMocks.getQuery(tsdb, "/?method_override=put").getAPIMethod());
  }
  
  @Test
  public void getAPIMethodOverrideDelete() {
    assertEquals(HttpMethod.DELETE, 
        NettyMocks.getQuery(tsdb, "/?method_override=delete").getAPIMethod());
  }
  
  @Test
  public void getAPIMethodOverrideDeleteCase() {
    assertEquals(HttpMethod.DELETE, 
        NettyMocks.getQuery(tsdb, "/?method_override=DeLeTe").getAPIMethod());
  }
  
  @Test (expected = BadRequestException.class)
  public void getAPIMethodOverrideMissingValue() {
    NettyMocks.getQuery(tsdb, "/?method_override").getAPIMethod();
  }
  
  @Test (expected = BadRequestException.class)
  public void getAPIMethodOverrideInvalidMEthod() {
    NettyMocks.getQuery(tsdb, "/?method_override=notaverb").getAPIMethod();
  }
  
  @Test
  public void guessMimeTypeFromUriPNG() throws Exception {
    assertEquals("image/png", 
        guessMimeTypeFromUri.invoke(null, "abcd.png"));
  }
  
  @Test
  public void guessMimeTypeFromUriHTML() throws Exception {
    assertEquals("text/html; charset=UTF-8", 
        guessMimeTypeFromUri.invoke(null, "abcd.html"));
  }
  
  @Test
  public void guessMimeTypeFromUriCSS() throws Exception {
    assertEquals("text/css", 
        guessMimeTypeFromUri.invoke(null, "abcd.css"));
  }
  
  @Test
  public void guessMimeTypeFromUriJS() throws Exception {
    assertEquals("text/javascript", 
        guessMimeTypeFromUri.invoke(null, "abcd.js"));
  }
  
  @Test
  public void guessMimeTypeFromUriGIF() throws Exception {
    assertEquals("image/gif", 
        guessMimeTypeFromUri.invoke(null, "abcd.gif"));
  }
  
  @Test
  public void guessMimeTypeFromUriICO() throws Exception {
    assertEquals("image/x-icon", 
        guessMimeTypeFromUri.invoke(null, "abcd.ico"));
  }
  
  @Test
  public void guessMimeTypeFromUriOther() throws Exception {
    assertNull(guessMimeTypeFromUri.invoke(null, "abcd.jpg"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void guessMimeTypeFromUriNull() throws Exception {
    guessMimeTypeFromUri.invoke(null, (Object[])null);
  }
  
  @Test 
  public void guessMimeTypeFromUriEmpty() throws Exception {
    assertNull(guessMimeTypeFromUri.invoke(null, ""));
  }

  @Test
  public void guessMimeTypeFromContentsHTML() throws Exception {
    assertEquals("text/html; charset=UTF-8", 
        guessMimeTypeFromContents.invoke(
            NettyMocks.getQuery(tsdb, ""),
            bufferManager.wrap(
                "<HTML>...", UTF8)));
  }
  
  @Test
  public void guessMimeTypeFromContentsJSONObj() throws Exception {
    assertEquals("application/json", 
        guessMimeTypeFromContents.invoke(
            NettyMocks.getQuery(tsdb, ""),
            bufferManager.wrap(
                "{\"hello\":\"world\"}", UTF8)));
  }
  
  @Test
  public void guessMimeTypeFromContentsJSONArray() throws Exception {
    assertEquals("application/json", 
        guessMimeTypeFromContents.invoke(
            NettyMocks.getQuery(tsdb, ""),
            bufferManager.wrap(
                "[\"hello\",\"world\"]", UTF8)));
  }
  
  @Test
  public void guessMimeTypeFromContentsPNG() throws Exception {
    assertEquals("image/png", 
        guessMimeTypeFromContents.invoke(
            NettyMocks.getQuery(tsdb, ""),
            bufferManager.wrap(
                new byte[] {(byte) 0x89, 0x00})));
  }
  
  @Test
  public void guessMimeTypeFromContentsText() throws Exception {
    assertEquals("text/plain", 
        guessMimeTypeFromContents.invoke(
            NettyMocks.getQuery(tsdb, ""),
            bufferManager.wrap(
                "Just plain text", UTF8)));
  }
  
  @Test 
  public void guessMimeTypeFromContentsEmpty() throws Exception {
    assertEquals("text/plain", 
        guessMimeTypeFromContents.invoke(
            NettyMocks.getQuery(tsdb, ""),
            bufferManager.wrap(
                "", UTF8)));
  }
  
  @Test (expected = NullPointerException.class)
  public void guessMimeTypeFromContentsNull() throws Exception {
    ByteBuf buf = null;
    guessMimeTypeFromContents.invoke(
        NettyMocks.getQuery(tsdb, ""), buf);
  }
  
  @Test
  public void initializeSerializerMaps() throws Exception {
    HttpQuery.initializeSerializerMaps(null);
  }
  
  @Test
  public void setSerializer() throws Exception {
    HttpQuery.initializeSerializerMaps(null);
    HttpQuery query = NettyMocks.getQuery(tsdb, "/aggregators");
    query.setSerializer();
    assertEquals(HttpJsonSerializer.class.getCanonicalName(), 
        query.serializer().getClass().getCanonicalName());
  }
  
  @Test
  public void setFormatterQS() throws Exception {
    HttpQuery.initializeSerializerMaps(null);
    HttpQuery query = NettyMocks.getQuery(tsdb, "/aggregators?formatter=json");
    query.setSerializer();
    assertEquals(HttpJsonSerializer.class.getCanonicalName(), 
        query.serializer().getClass().getCanonicalName());
  }
  
  @Test
  public void setSerializerDummyQS() throws Exception {
    PluginLoader.loadJAR("plugin_test.jar");
    HttpQuery.initializeSerializerMaps(null);
    HttpQuery query = NettyMocks.getQuery(tsdb, "/aggregators?serializer=dummy");
    query.setSerializer();
    assertEquals("net.opentsdb.tsd.DummyHttpSerializer", 
        query.serializer().getClass().getCanonicalName());
  }
  
  @Test
  public void setSerializerCT() throws Exception {
    HttpQuery.initializeSerializerMaps(null);
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.headers().add("Content-Type", "application/json");
    final HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    query.setSerializer();
    assertEquals(HttpJsonSerializer.class.getCanonicalName(), 
        query.serializer().getClass().getCanonicalName());
  }
  
  @Test
  public void setSerializerDummyCT() throws Exception {
    PluginLoader.loadJAR("plugin_test.jar");
    HttpQuery.initializeSerializerMaps(null);
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.headers().add("Content-Type", "application/tsdbdummy");
    final HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    query.setSerializer();
    assertEquals("net.opentsdb.tsd.DummyHttpSerializer", 
        query.serializer().getClass().getCanonicalName());
  }
  
  @Test
  public void setSerializerDefaultCT() throws Exception {
    HttpQuery.initializeSerializerMaps(null);
    final Channel channelMock = NettyMocks.fakeChannel();
    final FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.headers().add("Content-Type", "invalid/notfoundtype");
    final HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    query.setSerializer();
    assertEquals(HttpJsonSerializer.class.getCanonicalName(), 
        query.serializer().getClass().getCanonicalName());
  }
  
  @Test (expected = BadRequestException.class)
  public void setSerializerNotFound() throws Exception {
    HttpQuery.initializeSerializerMaps(null);
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?serializer=notfound");
    query.setSerializer();
  }
  
  @Test
  public void internalErrorDeprecated() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "", ec);
    try {
      throw new Exception("Internal Error");
    } catch (Exception e) {
      query.internalError(e);
    }
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
        response.status());
    assertEquals(
        "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">", 
        response.content().toString(UTF8)
        .substring(0, 63));
  }
  
  @Test
  public void internalErrorDeprecatedJSON() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/?json", ec);
    try {
      throw new Exception("Internal Error");
    } catch (Exception e) {
      query.internalError(e);
    }
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
        response.status());    
    assertEquals(
        "{\"err\":\"java.lang.Exception: Internal Error", 
        response.content().toString(UTF8)
        .substring(0, 43));
  }
  
  @Test
  public void internalErrorDefaultSerializer() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/error", ec);
    query.getQueryBaseRoute();
    try {
      throw new Exception("Internal Error");
    } catch (Exception e) {
      query.internalError(e);
    }
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
        response.status());    
    assertEquals(
        "{\"error\":{\"code\":500,\"message\":\"Internal Error\"", 
        response.content().toString(UTF8)
        .substring(0, 47));
  }
  
  @Test (expected = NullPointerException.class)
  public void internalErrorNull() {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    query.internalError(null);
  }
  
  @Test
  public void badRequestDeprecated() { 
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/", ec);
    try {
      throw new BadRequestException("Bad user error");
    } catch (BadRequestException e) {
      query.badRequest(e);
    }
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());    
    assertEquals(
        "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">", 
        response.content().toString(UTF8)
        .substring(0, 63));
  }
  
  @Test
  public void badRequestDeprecatedJSON() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/?json", ec);
    try {
      throw new BadRequestException("Bad user error");
    } catch (BadRequestException e) {
      query.badRequest(e);
    }
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());  
    assertEquals(
        "{\"err\":\"Bad user error\"}", 
        response.content().toString(UTF8));
  }
  
  @Test
  public void badRequestDefaultSerializer() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/error", ec);
    query.getQueryBaseRoute();
    try {
      throw new BadRequestException("Bad user error");
    } catch (BadRequestException e) {
      query.badRequest(e);
    }
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.BAD_REQUEST, response.status()); 
    assertEquals(
        "{\"error\":{\"code\":400,\"message\":\"Bad user error\"", 
        response.content().toString(UTF8)
        .substring(0, 47));
  }
  
  @Test
  public void badRequestDefaultSerializerDiffStatus() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/error", ec);
    query.getQueryBaseRoute();
    try {
      throw new BadRequestException(HttpResponseStatus.FORBIDDEN,
          "Bad user error");
    } catch (BadRequestException e) {
      query.badRequest(e);
    }
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.FORBIDDEN, response.status()); 
    assertEquals(
        "{\"error\":{\"code\":403,\"message\":\"Bad user error\"", 
        response.content().toString(UTF8)
        .substring(0, 47));
  }
  
  @Test
  public void badRequestDefaultSerializerDetails() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/error", ec);
    query.getQueryBaseRoute();
    try {
      throw new BadRequestException(HttpResponseStatus.FORBIDDEN,
          "Bad user error", "Got Details");
    } catch (BadRequestException e) {
      query.badRequest(e);
    }
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.FORBIDDEN, response.status()); 
    assertEquals(
        "{\"error\":{\"code\":403,\"message\":\"Bad user error\",\"details\":\"Got Details\"", 
        response.content().toString(UTF8)
        .substring(0, 71));
  }
  
  @Test (expected = NullPointerException.class)
  public void badRequestNull() {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.badRequest((BadRequestException)null);
  }
  
  @Test
  public void badRequestDeprecatedString() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/", ec);
    query.badRequest("Bad user error");
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());    
    assertEquals(
        "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">", 
        response.content().toString(UTF8)
        .substring(0, 63));
  }
  
  @Test
  public void badRequestDeprecatedJSONString() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/?json", ec);
    query.badRequest("Bad user error");
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());  
    assertEquals(
        "{\"err\":\"Bad user error\"}", 
        response.content().toString(UTF8));
  }
  
  @Test
  public void badRequestDefaultSerializerString() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/error", ec);
    query.getQueryBaseRoute();
    query.badRequest("Bad user error");
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.BAD_REQUEST, response.status()); 
    assertEquals(
        "{\"error\":{\"code\":400,\"message\":\"Bad user error\"", 
        response.content().toString(UTF8)
        .substring(0, 47));
  }
  
  @Test
  public void badRequestNullString() {
    // this won't throw an error, just report "null" back to the user with a 
    // stack trace
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.badRequest((String)null);
  }
  
  @Test
  public void notFoundDeprecated() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/", ec);
    query.notFound();
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.NOT_FOUND, response.status());    
    assertEquals(
        "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">", 
        response.content().toString(UTF8)
        .substring(0, 63));
  }
  
  @Test
  public void notFoundDeprecatedJSON() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/?json", ec);
    query.notFound();
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.NOT_FOUND, response.status());  
    assertEquals(
        "{\"err\":\"Page Not Found\"}", 
        response.content().toString(UTF8));
  }
  
  @Test
  public void notFoundDefaultSerializer() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/error", ec);
    query.getQueryBaseRoute();
    query.notFound();
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.NOT_FOUND, response.status()); 
    assertEquals(
        "{\"error\":{\"code\":404,\"message\":\"Endpoint not found\"}}", 
        response.content().toString(UTF8));
  }
  
  @Test
  public void redirect() {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/", ec);
    query.redirect("/redirect");
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.OK, response.status());
    assertEquals("/redirect", response.headers().get("Location"));
    assertEquals("<html></head><meta http-equiv=\"refresh\" content=\"0; url="
        + "/redirect\"></head></html>", 
        response.content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void redirectNull() {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.redirect(null);
  }
  
  @Test
  public void escapeJson() {
    StringBuilder sb = new StringBuilder();
    String json = "\" \\ ";
    json += Character.toString('\b') + " ";
    json += Character.toString('\f') + " ";
    json += Character.toString('\n') + " ";
    json += Character.toString('\r') + " ";
    json += Character.toString('\t');
    HttpQuery.escapeJson(json, sb);
    assertEquals("\\\" \\\\ \\b \\f \\n \\r \\t", sb.toString());
  }
  
  @Test
  public void sendReplyBytes() throws Exception {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/", ec);
    query.sendReply("Hello World".getBytes());
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.OK, response.status());
    assertEquals("Hello World", 
        response.content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyBytesNull() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.sendReply((byte[])null);
  }
  
  @Test
  public void sendReplyStatusBytes() throws Exception {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/", ec);
    query.sendReply(HttpResponseStatus.CREATED, "Hello World".getBytes());
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.CREATED, response.status());
    assertEquals("Hello World", 
        response.content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStatusBytesNullStatus() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.sendReply(null, "Hello World".getBytes());
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStatusBytesNullBytes() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.sendReply(HttpResponseStatus.CREATED, (byte[])null);
  }
  
  @Test
  public void sendReplySB() throws Exception {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/", ec);
    query.sendReply(new StringBuilder("Hello World"));
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.OK, response.status());
    assertEquals("Hello World", 
        response.content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplySBNull() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.sendReply((StringBuilder)null);
  }
  
  @Test 
  public void sendReplyString() throws Exception {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/", ec);
    query.sendReply("Hello World");
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.OK, response.status());
    assertEquals("Hello World", 
        response.content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStringNull() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.sendReply((String)null);
  }
  
  @Test
  public void sendReplyStatusSB() throws Exception {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/", ec);
    query.sendReply(HttpResponseStatus.CREATED, 
        new StringBuilder("Hello World"));
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.CREATED, response.status());
    assertEquals("Hello World", 
        response.content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStatusSBNullStatus() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.sendReply(null, new StringBuilder("Hello World"));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStatusSBNullSB() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.sendReply(HttpResponseStatus.CREATED, (StringBuilder)null);
  }
  
  @Test
  public void sendReplyCB() throws Exception {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/", ec);
    ByteBuf cb = bufferManager.wrap("Hello World", 
        UTF8);
    query.sendReply(cb);
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.OK, response.status());
    assertEquals("Hello World", 
        response.content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyCBNull() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.sendReply((ByteBuf)null);
  }
  
  @Test
  public void sendReplyStatusCB() throws Exception {
	  EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/", ec);
    ByteBuf cb = bufferManager.wrap("Hello World", 
        UTF8);
    query.sendReply(HttpResponseStatus.CREATED, cb);
    FullHttpResponse response = ec.readOutbound();
    assertEquals(HttpResponseStatus.CREATED, response.status());
    assertEquals("Hello World", 
        response.content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStatusCBNullStatus() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    ByteBuf cb = bufferManager.wrap("Hello World", 
        UTF8);
    query.sendReply(null, cb);
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStatusCBNullCB() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.sendReply(HttpResponseStatus.CREATED, (ByteBuf)null);
  }
  
  @Test
  public void sendStatusOnly() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().status());
    assertEquals(0, query.response().content().readableBytes());
    assertNull(query.response().headers().get("Content-Type"));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendStatusOnlyNull() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/");
    query.sendStatusOnly(null);
  }
  
  @Test
  public void sendBuffer() throws Exception {
	EmbeddedChannel ec = new EmbeddedChannel();
    HttpQuery query = NettyMocks.getQuery(tsdb, "", ec);
    ByteBuf cb = bufferManager.wrap("Hello World", UTF8);
    sendBuffer.invoke(query, HttpResponseStatus.OK, cb);
    FullHttpResponse httpResponse = ec.readOutbound();
    assertEquals(HttpResponseStatus.OK, httpResponse.status());
    assertEquals(cb.toString(UTF8), 
    		httpResponse.content().toString(UTF8));
  }
  
  @Test
  public void sendBufferEmptyCB() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    ByteBuf cb = bufferManager.wrap("", 
        UTF8);
    sendBuffer.invoke(query, HttpResponseStatus.OK, cb);
    assertEquals(HttpResponseStatus.OK, query.response().status());
    assertEquals(cb.toString(UTF8), 
        query.response().content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendBufferNullStatus() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    ByteBuf cb = bufferManager.wrap("Hello World", 
        UTF8);
    sendBuffer.invoke(query, null, cb);
  }
  
  @Test (expected = NullPointerException.class)
  public void sendBufferNullCB() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    sendBuffer.invoke(query, HttpResponseStatus.OK, null);
  }

  @Test
  public void getSerializerStatus() throws Exception {
    HttpQuery.initializeSerializerMaps(tsdb);
    assertNotNull(HttpQuery.getSerializerStatus());
  }

}
