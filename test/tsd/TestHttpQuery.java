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

import static net.opentsdb.tsd.NettyMocks.UTF8;
import static net.opentsdb.tsd.NettyMocks.bufferManager;
import static net.opentsdb.tsd.NettyMocks.deleteQuery;
import static net.opentsdb.tsd.NettyMocks.getMockedHTTPTSDB;
import static net.opentsdb.tsd.NettyMocks.getQuery;
import static net.opentsdb.tsd.NettyMocks.postQuery;
import static net.opentsdb.tsd.NettyMocks.putQuery;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import net.opentsdb.core.TSDB;
import net.opentsdb.plugin.PluginJarBuilder;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
	  "ch.qos.*", "org.slf4j.*",
	  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, HttpQuery.class})
public final class TestHttpQuery {
  private TSDB tsdb = null;
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
    tsdb = getMockedHTTPTSDB();
  }
  
  @Test
  public void getQueryString() {
    final HttpQuery query = getQuery(tsdb, "/api/v1/put?param=value&param2=value2");     		
    Map<String, List<String>> params = query.getQueryString();
    assertNotNull(params);
    assertEquals("value", params.get("param").get(0));
    assertEquals("value2", params.get("param2").get(0));
  }
  
  @Test
  public void getQueryStringEmpty() {
    Map<String, List<String>> params = 
      getQuery(tsdb, "/api/v1/put").getQueryString();
    assertNotNull(params);
    assertEquals(0, params.size());
  }
  
  @Test
  public void getQueryStringMulti() {
    Map<String, List<String>> params = 
      getQuery(tsdb, 
          "/api/v1/put?param=v1&param=v2&param=v3").getQueryString();
    assertNotNull(params);
    assertEquals(1, params.size());
    assertEquals(3, params.get("param").size());
  }
  
  @Test (expected = NullPointerException.class)
  public void getQueryStringNULL() {
    getQuery(tsdb, null).getQueryString();
  }
  
  @Test
  public void getQueryStringParam() {
    assertEquals("value", 
        getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2")
        .getQueryStringParam("param"));
  }
  
  @Test
  public void getQueryStringParamNull() {
    assertNull(getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2").
        getQueryStringParam("nothere"));
  }
  
  @Test
  public void getRequiredQueryStringParam() {
    assertEquals("value", 
        getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2").
        getRequiredQueryStringParam("param"));
  }
  
  @Test (expected = BadRequestException.class)
  public void getRequiredQueryStringParamMissing() {
    getQuery(tsdb, "/api/v1/put?param=value&param2=value2").
      getRequiredQueryStringParam("nothere");
  }
  
  @Test
  public void hasQueryStringParam() {
    assertTrue(getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2").
        hasQueryStringParam("param"));
  }
  
  @Test
  public void hasQueryStringMissing() {
    assertFalse(getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2").
        hasQueryStringParam("nothere"));
  }
  
  @Test
  public void getQueryStringParams() {
    List<String> params = getQuery(tsdb, 
        "/api/v1/put?param=v1&param=v2&param=v3").
      getQueryStringParams("param");
    assertNotNull(params);
    assertEquals(3, params.size());
  }
  
  @Test
  public void getQueryStringParamsNull() {
    List<String> params = getQuery(tsdb, 
        "/api/v1/put?param=v1&param=v2&param=v3").
      getQueryStringParams("nothere");
    assertNull(params);
  }
  
  @Test
  public void getQueryPathA() {
    assertEquals("/api/v1/put", 
        getQuery(tsdb, 
        "/api/v1/put?param=value&param2=value2").
        getQueryPath());
  }
  
  @Test
  public void getQueryPathB() {
    assertEquals("/", getQuery(tsdb, "/").getQueryPath());
  }
  
  @Test (expected = NullPointerException.class)
  public void getQueryPathNull() {
    getQuery(tsdb, null).getQueryPath();
  }
  
  @Test
  public void explodePath() {
    final HttpQuery query = getQuery(tsdb, 
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
    final HttpQuery query = getQuery(tsdb, "/");
    final String[] path = query.explodePath();
    assertNotNull(path);
    assertEquals(1, path.length);
    assertEquals("", path[0]);
  }
  
  @Test (expected = NullPointerException.class)
  public void explodePathNull() {
    getQuery(tsdb, null).explodePath();
  }
  
  @Test
  public void getQueryBaseRouteRoot() {
    final HttpQuery query = getQuery(tsdb, "/");
    assertEquals("", query.getQueryBaseRoute());
    assertEquals(0, query.apiVersion());
  }
  
  @Test
  public void explodeAPIPath() {
    final HttpQuery query = getQuery(tsdb, 
      "/api/v1/put?param=value&param2=value2");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertEquals("put", path[0]);
  }
  
  @Test
  public void explodeAPIPathNoVersion() {
    final HttpQuery query = getQuery(tsdb, 
      "/api/put?param=value&param2=value2");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertEquals("put", path[0]);
  }
  
  @Test
  public void explodeAPIPathExtended() {
    final HttpQuery query = getQuery(tsdb, 
      "/api/v1/uri/assign");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertEquals("uri", path[0]);
    assertEquals("assign", path[1]);
  }
  
  @Test
  public void explodeAPIPathExtendedNoVersion() {
    final HttpQuery query = getQuery(tsdb, 
      "/api/uri/assign");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertEquals("uri", path[0]);
    assertEquals("assign", path[1]);
  }
  
  @Test
  public void explodeAPIPathCase() {
    final HttpQuery query = getQuery(tsdb, 
      "/Api/Uri");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertEquals("Uri", path[0]);
  }
  
  @Test
  public void explodeAPIPathRoot() {
    final HttpQuery query = getQuery(tsdb, 
      "/api");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertTrue(path[0].isEmpty());
  }
  
  @Test
  public void explodeAPIPathRootVersion() {
    final HttpQuery query = getQuery(tsdb, 
      "/api/v1");
    final String[] path = query.explodeAPIPath();
    assertNotNull(path);
    assertTrue(path[0].isEmpty());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void explodeAPIPathNotAPI() {
    final HttpQuery query = getQuery(tsdb, 
      "/q?hello=world");
    query.explodeAPIPath();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void explodeAPIPathHome() {
    final HttpQuery query = getQuery(tsdb, 
      "/");
    query.explodeAPIPath();
  }
  
  @Test
  public void getQueryBaseRouteRootQS() {
    final HttpQuery query = getQuery(tsdb, "/?param=value");
    assertEquals("", query.getQueryBaseRoute());
    assertEquals(0, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteQ() {
    final HttpQuery query = getQuery(tsdb, "/q");
    assertEquals("q", query.getQueryBaseRoute());
    assertEquals(0, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteQSlash() {
    final HttpQuery query = getQuery(tsdb, "/q/");
    assertEquals("q", query.getQueryBaseRoute());
    assertEquals(0, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteLogs() {
    final HttpQuery query = getQuery(tsdb, "/logs");
    assertEquals("logs", query.getQueryBaseRoute());
    assertEquals(0, query.apiVersion());
  }
  
  @Test (expected = BadRequestException.class)
  public void getQueryBaseRouteAPIVNotImplemented() {
    final HttpQuery query = getQuery(tsdb, "/api/v3/put");
    assertEquals("api/put", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteAPICap() {
    final HttpQuery query = getQuery(tsdb, "/API/V1/PUT");
    assertEquals("api/put", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteAPIDefaultV() {
    final HttpQuery query = getQuery(tsdb, "/api/put");
    assertEquals("api/put", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteAPIQS() {
    final HttpQuery query = getQuery(tsdb, 
        "/api/v1/put?metric=mine");
    assertEquals("api/put", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteAPINoEP() {
    final HttpQuery query = getQuery(tsdb, "/api");
    assertEquals("api", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteAPINoEPSlash() {
    final HttpQuery query = getQuery(tsdb, "/api/");
    assertEquals("api", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteFavicon() {
    final HttpQuery query = getQuery(tsdb, "/favicon.ico");
    assertEquals("favicon.ico", query.getQueryBaseRoute());
    assertEquals(0, query.apiVersion());
  }
  
  @Test
  public void getQueryBaseRouteVersion() {
    final HttpQuery query = getQuery(tsdb, "/api/version/query");
    assertEquals("api/version", query.getQueryBaseRoute());
    assertEquals(1, query.apiVersion());
  }
  
  @Test (expected = BadRequestException.class)
  public void getQueryBaseRouteVBadNumber() {
    final HttpQuery query = getQuery(tsdb, "/api/v2d/query");
    query.getQueryBaseRoute();
  }
  
  @Test (expected = NullPointerException.class)
  public void getQueryBaseRouteNull() {
    getQuery(tsdb, null).getQueryBaseRoute();
  }
  
  @Test (expected = BadRequestException.class)
  public void getQueryBaseRouteBad() {
    getQuery(tsdb, "notavalidquery").getQueryBaseRoute();
  }
  
  @Test (expected = BadRequestException.class)
  public void getQueryBaseRouteEmpty() {
    getQuery(tsdb, "").getQueryBaseRoute();
  }
  
  @Test
  public void getCharsetDefault() {
    final HttpQuery query = getQuery(tsdb, "/"); 
    query.request().headers().add("Content-Type", "text/plain");    		
    assertEquals(UTF8, query.getCharset());
  }
  
  @Test
  public void getCharsetDefaultNoHeader() {
    assertEquals(UTF8, 
        getQuery(tsdb, "/").getCharset());
  }
  
  @Test
  public void getCharsetSupplied() {
    final HttpQuery query = getQuery(tsdb, "/");         		   
    query.request().headers().add("Content-Type", "text/plain; charset=UTF-16");    
    assertEquals(Charset.forName("UTF-16"), query.getCharset());
  }
  
  @Test (expected = UnsupportedCharsetException.class)
  public void getCharsetInvalid() {
	final HttpQuery query = getQuery(tsdb, "/");
    query.request().headers().add("Content-Type", "text/plain; charset=foobar");
    assertEquals(Charset.forName("UTF-16"), query.getCharset());
  }
  
  @Test
  public void hasContent() {
    HttpQuery query = postQuery(tsdb, "/", "Hello World", "");
    assertTrue(query.hasContent());
  }
  
  @Test
  public void hasContentFalse() {
    HttpQuery query = postQuery(tsdb, "/", null, "");
    assertFalse(query.hasContent());
  }
  
  @Test
  public void hasContentNotReadable() {
    HttpQuery query = postQuery(tsdb, "/", "", "");
    assertFalse(query.hasContent());
  }
  
  @Test
  public void getContentEncoding() {
	HttpQuery query = getQuery(tsdb, "/");
    query.request().headers().add("Content-Type", "text/plain; charset=UTF-16");
    final ByteBuf buf = bufferManager.wrap("S\u00ED Se\u00F1or", 
            CharsetUtil.UTF_16);
    query.request().content().writeBytes(buf);    
    assertEquals("S\u00ED Se\u00F1or", query.getContent());
  }
  
  @Test
  public void getContentDefault() {
    final HttpQuery query = getQuery(tsdb, "/");
    final ByteBuf buf = bufferManager.wrap("S\u00ED Se\u00F1or", 
        CharsetUtil.UTF_8);
    query.request().content().writeBytes(buf);    
    assertEquals("S\u00ED Se\u00F1or", query.getContent());
  }
  
  @Test
  public void getContentBadEncoding() {
	final HttpQuery query = getQuery(tsdb, "/");
	final ByteBuf buf = bufferManager.wrap("S\u00ED Se\u00F1or", 
        CharsetUtil.ISO_8859_1);
    query.request().content().writeBytes(buf);    
    assertThat("S\u00ED Se\u00F1or", not(equalTo(query.getContent())));
  }
  
  @Test
  public void getContentEmpty() {
    assertTrue(getQuery(tsdb, "/").getContent().isEmpty());
  }
  
  @Test
  public void getAPIMethodGet() {
    assertEquals(HttpMethod.GET, 
        getQuery(tsdb, "/").getAPIMethod());
  }
  
  @Test
  public void getAPIMethodPost() {
    assertEquals(HttpMethod.POST, 
        postQuery(tsdb, "/", null).getAPIMethod());
  }
  
  @Test
  public void getAPIMethodPut() {
    HttpQuery query = putQuery(tsdb, "/", null);
    assertEquals(HttpMethod.PUT, query.getAPIMethod());
  }
  
  @Test
  public void getAPIMethodDelete() {
    HttpQuery query = deleteQuery(tsdb, "/", null);
    assertEquals(HttpMethod.DELETE, query.getAPIMethod());
  }
  
  @Test
  public void getAPIMethodOverrideGet() {
    assertEquals(HttpMethod.GET, 
        getQuery(tsdb, "/?method_override=get").getAPIMethod());
  }
  
  @Test
  public void getAPIMethodOverridePost() {
    assertEquals(HttpMethod.POST, 
        getQuery(tsdb, "/?method_override=post").getAPIMethod());
  }
  
  @Test
  public void getAPIMethodOverridePut() {
    assertEquals(HttpMethod.PUT, 
        getQuery(tsdb, "/?method_override=put").getAPIMethod());
  }
  
  @Test
  public void getAPIMethodOverrideDelete() {
    assertEquals(HttpMethod.DELETE, 
        getQuery(tsdb, "/?method_override=delete").getAPIMethod());
  }
  
  @Test
  public void getAPIMethodOverrideDeleteCase() {
    assertEquals(HttpMethod.DELETE, 
        getQuery(tsdb, "/?method_override=DeLeTe").getAPIMethod());
  }
  
  @Test (expected = BadRequestException.class)
  public void getAPIMethodOverrideMissingValue() {
    getQuery(tsdb, "/?method_override").getAPIMethod();
  }
  
  @Test (expected = BadRequestException.class)
  public void getAPIMethodOverrideInvalidMEthod() {
    getQuery(tsdb, "/?method_override=notaverb").getAPIMethod();
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
            getQuery(tsdb, ""),
            bufferManager.wrap(
                "<HTML>...", UTF8)));
  }
  
  @Test
  public void guessMimeTypeFromContentsJSONObj() throws Exception {
    assertEquals("application/json", 
        guessMimeTypeFromContents.invoke(
            getQuery(tsdb, ""),
            bufferManager.wrap(
                "{\"hello\":\"world\"}", UTF8)));
  }
  
  @Test
  public void guessMimeTypeFromContentsJSONArray() throws Exception {
    assertEquals("application/json", 
        guessMimeTypeFromContents.invoke(
            getQuery(tsdb, ""),
            bufferManager.wrap(
                "[\"hello\",\"world\"]", UTF8)));
  }
  
  @Test
  public void guessMimeTypeFromContentsPNG() throws Exception {
    assertEquals("image/png", 
        guessMimeTypeFromContents.invoke(
            getQuery(tsdb, ""),
            bufferManager.wrap(
                new byte[] {(byte) 0x89, 0x00})));
  }
  
  @Test
  public void guessMimeTypeFromContentsText() throws Exception {
    assertEquals("text/plain", 
        guessMimeTypeFromContents.invoke(
            getQuery(tsdb, ""),
            bufferManager.wrap(
                "Just plain text", UTF8)));
  }
  
  @Test 
  public void guessMimeTypeFromContentsEmpty() throws Exception {
    assertEquals("text/plain", 
        guessMimeTypeFromContents.invoke(
            getQuery(tsdb, ""),
            bufferManager.wrap(
                "", UTF8)));
  }
  
  @Test (expected = NullPointerException.class)
  public void guessMimeTypeFromContentsNull() throws Exception {
    ByteBuf buf = null;
    guessMimeTypeFromContents.invoke(
        getQuery(tsdb, ""), buf);
  }
  
  @Test
  public void initializeSerializerMaps() throws Exception {
    HttpQuery.initializeSerializerMaps(null);
  }
  
  @Test
  public void setSerializer() throws Exception {
    HttpQuery.initializeSerializerMaps(null);
    HttpQuery query = getQuery(tsdb, "/aggregators");
    query.setSerializer();
    assertEquals(HttpJsonSerializer.class.getCanonicalName(), 
        query.serializer().getClass().getCanonicalName());
  }
  
  @Test
  public void setFormatterQS() throws Exception {
    HttpQuery.initializeSerializerMaps(null);
    HttpQuery query = getQuery(tsdb, "/aggregators?formatter=json");
    query.setSerializer();
    assertEquals(HttpJsonSerializer.class.getCanonicalName(), 
        query.serializer().getClass().getCanonicalName());
  }
  
  @Test
  public void setSerializerDummyQS() throws Exception {
    PluginLoader.loadJAR("plugin_test.jar");
    HttpQuery.initializeSerializerMaps(null);
    HttpQuery query = getQuery(tsdb, "/aggregators?serializer=dummy");
    query.setSerializer();
    assertEquals("net.opentsdb.tsd.DummyHttpSerializer", 
        query.serializer().getClass().getCanonicalName());
  }
  
  @Test
  public void setSerializerCT() throws Exception {
    HttpQuery.initializeSerializerMaps(null);
    final HttpQuery query = getQuery(tsdb, "/");
    query.request().headers().add("Content-Type", "application/json");
    query.setSerializer();
    assertEquals(HttpJsonSerializer.class.getCanonicalName(), 
        query.serializer().getClass().getCanonicalName());
  }
  
  @Test
  public void setSerializerDummyCT() throws Exception {
    PluginLoader.loadJAR(PluginJarBuilder.newBuilder("plugin_test.jar").build(false, true));
    HttpQuery.initializeSerializerMaps(null);
    final HttpQuery query = getQuery(tsdb, "/");
    query.request().headers().add("Content-Type", "application/tsdbdummy");
    query.setSerializer();
    assertEquals("net.opentsdb.tsd.DummyHttpSerializer", 
        query.serializer().getClass().getCanonicalName());
  }
  
  @Test
  public void setSerializerDefaultCT() throws Exception {
    HttpQuery.initializeSerializerMaps(null);
    final HttpQuery query = getQuery(tsdb, "/");
    query.request().headers().add("Content-Type", "invalid/notfoundtype");
    query.setSerializer();
    assertEquals(HttpJsonSerializer.class.getCanonicalName(), 
        query.serializer().getClass().getCanonicalName());
  }
  
  @Test (expected = BadRequestException.class)
  public void setSerializerNotFound() throws Exception {
    HttpQuery.initializeSerializerMaps(null);
    HttpQuery query = getQuery(tsdb, 
        "/api/suggest?serializer=notfound");
    query.setSerializer();
  }
  
  @Test
  public void internalErrorDeprecated() {
    HttpQuery query = getQuery(tsdb, "");
    try {
      throw new Exception("Internal Error");
    } catch (Exception e) {
      query.internalError(e);
    }
    assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
        query.response().status());
    assertEquals(
        "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">", 
        query.response().content().toString(UTF8)
        .substring(0, 63));
  }
  
  @Test
  public void internalErrorDeprecatedJSON() {
    HttpQuery query = getQuery(tsdb, "/?json");
    try {
      throw new Exception("Internal Error");
    } catch (Exception e) {
      query.internalError(e);
    }
    assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
        query.response().status());    
    assertEquals(
        "{\"err\":\"java.lang.Exception: Internal Error", 
        query.response().content().toString(UTF8)
        .substring(0, 43));
  }
  
  @Test
  public void internalErrorDefaultSerializer() {
    HttpQuery query = getQuery(tsdb, "/api/error");
    query.getQueryBaseRoute();
    try {
      throw new Exception("Internal Error");
    } catch (Exception e) {
      query.internalError(e);
    }
    assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
        query.response().status());    
    assertEquals(
        "{\"error\":{\"code\":500,\"message\":\"Internal Error\"", 
        query.response().content().toString(UTF8)
        .substring(0, 47));
  }
  
  @Test (expected = NullPointerException.class)
  public void internalErrorNull() {
    HttpQuery query = getQuery(tsdb, "");
    query.internalError(null);
  }
  
  @Test
  public void badRequestDeprecated() {
    HttpQuery query = getQuery(tsdb, "/");
    try {
      throw new BadRequestException("Bad user error");
    } catch (BadRequestException e) {
      query.badRequest(e);
    }
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().status());    
    assertEquals(
        "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">", 
        query.response().content().toString(UTF8)
        .substring(0, 63));
  }
  
  @Test
  public void badRequestDeprecatedJSON() {
    HttpQuery query = getQuery(tsdb, "/?json");
    try {
      throw new BadRequestException("Bad user error");
    } catch (BadRequestException e) {
      query.badRequest(e);
    }
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().status());  
    assertEquals(
        "{\"err\":\"Bad user error\"}", 
        query.response().content().toString(UTF8));
  }
  
  @Test
  public void badRequestDefaultSerializer() {
    HttpQuery query = getQuery(tsdb, "/api/error");
    query.getQueryBaseRoute();
    try {
      throw new BadRequestException("Bad user error");
    } catch (BadRequestException e) {
      query.badRequest(e);
    }
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().status()); 
    assertEquals(
        "{\"error\":{\"code\":400,\"message\":\"Bad user error\"", 
        query.response().content().toString(UTF8)
        .substring(0, 47));
  }
  
  @Test
  public void badRequestDefaultSerializerDiffStatus() {
    HttpQuery query = getQuery(tsdb, "/api/error");
    query.getQueryBaseRoute();
    try {
      throw new BadRequestException(HttpResponseStatus.FORBIDDEN,
          "Bad user error");
    } catch (BadRequestException e) {
      query.badRequest(e);
    }
    assertEquals(HttpResponseStatus.FORBIDDEN, query.response().status()); 
    assertEquals(
        "{\"error\":{\"code\":403,\"message\":\"Bad user error\"", 
        query.response().content().toString(UTF8)
        .substring(0, 47));
  }
  
  @Test
  public void badRequestDefaultSerializerDetails() {
    HttpQuery query = getQuery(tsdb, "/api/error");
    query.getQueryBaseRoute();
    try {
      throw new BadRequestException(HttpResponseStatus.FORBIDDEN,
          "Bad user error", "Got Details");
    } catch (BadRequestException e) {
      query.badRequest(e);
    }
    assertEquals(HttpResponseStatus.FORBIDDEN, query.response().status()); 
    assertEquals(
        "{\"error\":{\"code\":403,\"message\":\"Bad user error\",\"details\":\"Got Details\"", 
        query.response().content().toString(UTF8)
        .substring(0, 71));
  }
  
  @Test (expected = NullPointerException.class)
  public void badRequestNull() {
    HttpQuery query = getQuery(tsdb, "/");
    query.badRequest((BadRequestException)null);
  }
  
  @Test
  public void badRequestDeprecatedString() {
    HttpQuery query = getQuery(tsdb, "/");
    query.badRequest("Bad user error");
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().status());    
    assertEquals(
        "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">", 
        query.response().content().toString(UTF8)
        .substring(0, 63));
  }
  
  @Test
  public void badRequestDeprecatedJSONString() {
    HttpQuery query = getQuery(tsdb, "/?json");
    query.badRequest("Bad user error");
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().status());  
    assertEquals(
        "{\"err\":\"Bad user error\"}", 
        query.response().content().toString(UTF8));
  }
  
  @Test
  public void badRequestDefaultSerializerString() {
    HttpQuery query = getQuery(tsdb, "/api/error");
    query.getQueryBaseRoute();
    query.badRequest("Bad user error");
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().status()); 
    assertEquals(
        "{\"error\":{\"code\":400,\"message\":\"Bad user error\"", 
        query.response().content().toString(UTF8)
        .substring(0, 47));
  }
  
  @Test
  public void badRequestNullString() {
    // this won't throw an error, just report "null" back to the user with a 
    // stack trace
    HttpQuery query = getQuery(tsdb, "/");
    query.badRequest((String)null);
  }
  
  @Test
  public void notFoundDeprecated() {
    HttpQuery query = getQuery(tsdb, "/");
    query.notFound();
    assertEquals(HttpResponseStatus.NOT_FOUND, query.response().status());    
    assertEquals(
        "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">", 
        query.response().content().toString(UTF8)
        .substring(0, 63));
  }
  
  @Test
  public void notFoundDeprecatedJSON() {
    HttpQuery query = getQuery(tsdb, "/?json");
    query.notFound();
    assertEquals(HttpResponseStatus.NOT_FOUND, query.response().status());  
    assertEquals(
        "{\"err\":\"Page Not Found\"}", 
        query.response().content().toString(UTF8));
  }
  
  @Test @Ignore // FIXME
  public void notFoundDefaultSerializer() {
    HttpQuery query = getQuery(tsdb, "/api/error");
    query.getQueryBaseRoute();
    query.notFound();
    assertEquals(HttpResponseStatus.NOT_FOUND, query.response().status()); 
    assertEquals(
        "{\"error\":{\"code\":404,\"message\":\"Endpoint not found\"}}", 
        query.response().content().toString(UTF8));
  }
  
//  @Test
//  public void notFoundDefaultSerializer() {
//	EmbeddedChannel ec = new EmbeddedChannel();
//    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/error", ec);
//    query.getQueryBaseRoute();
//    query.notFound();
//    FullHttpResponse response = ec.readOutbound();
//    assertEquals(HttpResponseStatus.NOT_FOUND, response.status()); 
//    assertEquals(
//        "{\"error\":{\"code\":404,\"message\":\"Endpoint not found\"}}", 
//        response.content().toString(UTF8));
//  }
  
  
  @Test
  public void redirect() {
    HttpQuery query = getQuery(tsdb, "/");
    query.redirect("/redirect");
    assertEquals(HttpResponseStatus.OK, query.response().status());
    assertEquals("/redirect", query.response().headers().get("Location"));
    assertEquals("<html></head><meta http-equiv=\"refresh\" content=\"0; url="
        + "/redirect\"></head></html>", 
        query.response().content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void redirectNull() {
    HttpQuery query = getQuery(tsdb, "/");
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
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply("Hello World".getBytes());
    assertEquals(HttpResponseStatus.OK, query.response().status());
    assertEquals("Hello World", 
        query.response().content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyBytesNull() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply((byte[])null);
  }
  
  @Test
  public void sendReplyStatusBytes() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply(HttpResponseStatus.CREATED, "Hello World".getBytes());
    assertEquals(HttpResponseStatus.CREATED, query.response().status());
    assertEquals("Hello World", 
        query.response().content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStatusBytesNullStatus() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply(null, "Hello World".getBytes());
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStatusBytesNullBytes() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply(HttpResponseStatus.CREATED, (byte[])null);
  }
  
  @Test
  public void sendReplySB() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply(new StringBuilder("Hello World"));
    assertEquals(HttpResponseStatus.OK, query.response().status());
    assertEquals("Hello World", 
        query.response().content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplySBNull() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply((StringBuilder)null);
  }
  
  @Test
  public void sendReplyString() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply("Hello World");
    assertEquals(HttpResponseStatus.OK, query.response().status());
    assertEquals("Hello World", 
        query.response().content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStringNull() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply((String)null);
  }
  
  @Test
  public void sendReplyStatusSB() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply(HttpResponseStatus.CREATED, 
        new StringBuilder("Hello World"));
    assertEquals(HttpResponseStatus.CREATED, query.response().status());
    assertEquals("Hello World", 
        query.response().content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStatusSBNullStatus() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply(null, new StringBuilder("Hello World"));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStatusSBNullSB() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply(HttpResponseStatus.CREATED, (StringBuilder)null);
  }
  
  @Test
  public void sendReplyCB() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    ByteBuf cb = bufferManager.wrap("Hello World", 
        UTF8);
    query.sendReply(cb);
    assertEquals(HttpResponseStatus.OK, query.response().status());
    assertEquals("Hello World", 
        query.response().content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyCBNull() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply((ByteBuf)null);
  }
  
  @Test
  public void sendReplyStatusCB() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    ByteBuf cb = bufferManager.wrap("Hello World", 
        UTF8);
    query.sendReply(HttpResponseStatus.CREATED, cb);
    assertEquals(HttpResponseStatus.CREATED, query.response().status());
    assertEquals("Hello World", 
        query.response().content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStatusCBNullStatus() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    ByteBuf cb = bufferManager.wrap("Hello World", 
        UTF8);
    query.sendReply(null, cb);
  }
  
  @Test (expected = NullPointerException.class)
  public void sendReplyStatusCBNullCB() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendReply(HttpResponseStatus.CREATED, (ByteBuf)null);
  }
  
  @Test
  public void sendStatusOnly() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().status());
    assertEquals(0, query.response().content().capacity());
    assertNull(query.response().headers().get("Content-Type"));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendStatusOnlyNull() throws Exception {
    HttpQuery query = getQuery(tsdb, "/");
    query.sendStatusOnly(null);
  }
  
  @Test
  public void sendBuffer() throws Exception {
    HttpQuery query = getQuery(tsdb, "");
    ByteBuf cb = bufferManager.wrap("Hello World", 
        UTF8);
    sendBuffer.invoke(query, HttpResponseStatus.OK, cb.copy());
    assertEquals(HttpResponseStatus.OK, query.response().status());
    assertEquals(cb.toString(UTF8), 
        query.response().content().toString(UTF8));
  }
  
  @Test
  public void sendBufferEmptyCB() throws Exception {
    HttpQuery query = getQuery(tsdb, "");
    ByteBuf cb = bufferManager.wrap("", 
        UTF8);
    sendBuffer.invoke(query, HttpResponseStatus.OK, cb);
    assertEquals(HttpResponseStatus.OK, query.response().status());
    assertEquals(cb.toString(UTF8), 
        query.response().content().toString(UTF8));
  }
  
  @Test (expected = NullPointerException.class)
  public void sendBufferNullStatus() throws Exception {
    HttpQuery query = getQuery(tsdb, "");
    ByteBuf cb = bufferManager.wrap("Hello World", 
        UTF8);
    sendBuffer.invoke(query, null, cb);
  }
  
  @Test (expected = NullPointerException.class)
  public void sendBufferNullCB() throws Exception {
    HttpQuery query = getQuery(tsdb, "");
    sendBuffer.invoke(query, HttpResponseStatus.OK, null);
  }

  @Test
  public void getSerializerStatus() throws Exception {
    HttpQuery.initializeSerializerMaps(tsdb);
    assertNotNull(HttpQuery.getSerializerStatus());
  }

}
