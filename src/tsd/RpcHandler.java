// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

/**
 * Stateless handler for all RPCs: telnet-style, built-in or plugin
 * HTTP.
 */
@ChannelHandler.Sharable
final class RpcHandler extends ChannelDuplexHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RpcHandler.class);
  
  private static final AtomicLong telnet_rpcs_received = new AtomicLong();
  private static final AtomicLong http_rpcs_received = new AtomicLong();
  private static final AtomicLong http_plugin_rpcs_received = new AtomicLong();
  private static final AtomicLong exceptions_caught = new AtomicLong();

  /** RPC executed when there's an unknown telnet-style command. */
  private final TelnetRpc unknown_cmd = new Unknown();
  /** List of domains to allow access to HTTP. By default this will be empty and
   * all CORS headers will be ignored. */
  private final HashSet<String> cors_domains;
  /** List of headers allowed for access to HTTP. By default this will contain a
   * set of known-to-work headers */
  private final String cors_headers;
  /** RPC plugins.  Contains the handlers we dispatch requests to. */
  private final RpcManager rpc_manager;

  /** The TSDB to use. */
  private final TSDB tsdb;
  
  /**
   * Constructor that loads the CORS domain list and prepares for
   * handling requests. This constructor creates its own {@link RpcManager}.
   * @param tsdb The TSDB to use.
   * @param manager instance of a ready-to-use {@link RpcManager}.
   * @throws IllegalArgumentException if there was an error with the CORS domain
   * list
   */
  public RpcHandler(final TSDB tsdb) {
    this(tsdb, RpcManager.instance(tsdb));
  }
  
  
  /**
   * Constructor that loads the CORS domain list and prepares for handling 
   * requests.
   * @param tsdb The TSDB to use.
   * @param manager instance of a ready-to-use {@link RpcManager}.
   * @throws IllegalArgumentException if there was an error with the CORS domain
   * list
   */
  
  public RpcHandler(final TSDB tsdb, final RpcManager manager) {
	//super(0,0,tsdb.getConfig().getInt("tsd.core.socket.timeout"), TimeUnit.SECONDS);  // tsd.core.connections.limit ?
	
    this.tsdb = tsdb;
    this.rpc_manager = manager;

    final String cors = tsdb.getConfig().getString("tsd.http.request.cors_domains");
    final TSDMode mode = tsdb.getConfig().getTSDMode("tsd.mode");

    LOG.info("TSD is in [{}] mode", mode);

    if (cors == null || cors.isEmpty()) {
      cors_domains = null;
      LOG.info("CORS domain list was empty, CORS will not be enabled");
    } else {
      final String[] domains = cors.split(",");
      cors_domains = new HashSet<String>(domains.length);
      for (final String domain : domains) {
        if (domain.equals("*") && domains.length > 1) {
          throw new IllegalArgumentException(
              "tsd.http.request.cors_domains must be a public resource (*) or " 
              + "a list of specific domains, you cannot mix both.");
        }
        cors_domains.add(domain.trim().toUpperCase());
        LOG.info("Loaded CORS domain (" + domain + ")");
      }
    }

    cors_headers = tsdb.getConfig().getString("tsd.http.request.cors_headers")
        .trim();
    if ((cors_headers == null) || !cors_headers.matches("^([a-zA-Z0-9_.-]+,\\s*)*[a-zA-Z0-9_.-]+$")) {
      throw new IllegalArgumentException(
          "tsd.http.request.cors_headers must be a list of validly-formed "
          + "HTTP header names. No wildcards are allowed.");
    } else {
      LOG.info("Loaded CORS headers (" + cors_headers + ")");
    }
  }
  
  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object message) throws Exception {
	  LOG.debug("Handling message [{}]", message.getClass().getSimpleName());
  	final Channel channel = ctx.channel();
    try {      
      if (message instanceof String[]) {
        handleTelnetRpc(ctx, (String[]) message);
        ctx.flush();
        super.channelRead(ctx, message);
      } else if (message instanceof FullHttpRequest) {
    	final FullHttpRequest request = (FullHttpRequest)message;
//    	ReferenceCountUtil.retain(request);
        handleHttpQuery(tsdb, ctx, request);
        ctx.flush();
//        super.channelRead(ctx, message);
      } else if (message instanceof HttpRequest) {
    	  ReferenceCountUtil.retain(message);
    	  super.channelRead(ctx, message);
      } else {
        logError(channel, "Unexpected message type "
                 + message.getClass() + ": " + message);
        exceptions_caught.incrementAndGet();
      }
    } catch (Exception e) {
      Object pretty_message = message;
      if (pretty_message instanceof String[]) {
        pretty_message = Arrays.toString((String[]) pretty_message);
      }
      logError(channel, "Unexpected exception caught"
               + " while serving " + pretty_message, e);
      exceptions_caught.incrementAndGet();
    }

//  	super.channelRead(ctx, msg);
  }
  

  
  /**
   * Finds the right handler for a telnet-style RPC and executes it.
   * @param chan The channel handler context on which the RPC was received.
   * @param command The split telnet-style command.
   */
  private void handleTelnetRpc(final ChannelHandlerContext ctx, final String[] command) {
    TelnetRpc rpc = rpc_manager.lookupTelnetRpc(command[0]);
    if (rpc == null) {
      rpc = unknown_cmd;
    }
    telnet_rpcs_received.incrementAndGet();
    rpc.execute(tsdb, ctx, command);    
  }

  /**
   * Using the request URI, creates a query instance capable of handling 
   * the given request.
   * @param tsdb the TSDB instance we are running within
   * @param request the incoming HTTP request
   * @param ctx the channel handler context
   * @return a subclass of {@link AbstractHttpQuery}
   * @throws BadRequestException if the request is invalid in a way that
   * can be detected early, here.
   */
  private AbstractHttpQuery createQueryInstance(final TSDB tsdb,
        final FullHttpRequest request,
        final ChannelHandlerContext ctx) 
            throws BadRequestException {
    final String uri = request.uri();
    if (Strings.isNullOrEmpty(uri)) {
      throw new BadRequestException("Request URI is empty");
    } else if (uri.charAt(0) != '/') {
      throw new BadRequestException("Request URI doesn't start with a slash");
    } else if (rpc_manager.isHttpRpcPluginPath(uri)) {
      http_plugin_rpcs_received.incrementAndGet();
      return new HttpRpcPluginQuery(tsdb, request, ctx);
    } else {
      http_rpcs_received.incrementAndGet();
      HttpQuery builtinQuery = new HttpQuery(tsdb, request, ctx);
      return builtinQuery;
    }
  }
  
  /**
   * Helper method to apply CORS configuration to a request, either a built-in
   * RPC or a user plugin.
   * @return <code>true</code> if a status reply was sent (in the the case of
   * certain HTTP methods); <code>false</code> otherwise.
   */
  private boolean applyCorsConfig(final HttpRequest req, final AbstractHttpQuery query) 
        throws BadRequestException {
    final String domain = req.headers().get("Origin");
    
    // catch CORS requests and add the header or refuse them if the domain
    // list has been configured
    if (query.method() == HttpMethod.OPTIONS || 
        (cors_domains != null && domain != null && !domain.isEmpty())) {          
      if (cors_domains == null || domain == null || domain.isEmpty()) {
        throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
            "Method not allowed", "The HTTP method [" + 
            query.method().name() + "] is not permitted");
      }
      
      if (cors_domains.contains("*") || 
          cors_domains.contains(domain.toUpperCase())) {

        // when a domain has matched successfully, we need to add the header
        query.response().headers().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN,
            domain);
        query.response().headers().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS,
            "GET, POST, PUT, DELETE");
        query.response().headers().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS,
            cors_headers);

        // if the method requested was for OPTIONS then we'll return an OK
        // here and no further processing is needed.
        if (query.method() == HttpMethod.OPTIONS) {
          query.sendStatusOnly(HttpResponseStatus.OK);
          return true;
        }
      } else {
        // You'd think that they would want the server to return a 403 if
        // the Origin wasn't in the CORS domain list, but they want a 200
        // without the allow origin header. We'll return an error in the
        // body though.
        throw new BadRequestException(HttpResponseStatus.OK, 
            "CORS domain not allowed", "The domain [" + domain + 
            "] is not permitted access");
      }
    }
    return false;
  }
  
  /**
   * Finds the right handler for an HTTP query (either built-in or user plugin) 
   * and executes it. Also handles simple and pre-flight CORS requests if 
   * configured, rejecting requests that do not match a domain in the list.
   * @param chan The channel on which the query was received.
   * @param req The parsed HTTP request.
   */
  private void handleHttpQuery(final TSDB tsdb, final ChannelHandlerContext ctx, final HttpObject req) {
	  FullHttpRequest fullRequest = null;
	  DecoderResult result = req.decoderResult();
	  if (!result.isSuccess()) {
		  throw new BadRequestException(result.cause());
	  }

	  if (req instanceof FullHttpRequest) {
		  fullRequest = (FullHttpRequest) req;
	  }	 else {
		  // FIXME: what do we do here ?
		  return;
	  }
	  AbstractHttpQuery abstractQuery = null;
	  try {
		  abstractQuery = createQueryInstance(tsdb, fullRequest, ctx);
		  if (!tsdb.getConfig().enable_chunked_requests() && HttpUtil.isTransferEncodingChunked(fullRequest)) {
			  logError(abstractQuery, "Received an unsupported chunked request: "
					  + abstractQuery.request());
			  abstractQuery.badRequest(new BadRequestException("Chunked request not supported."));
			  return;
		  }
		  // NOTE: Some methods in HttpQuery have side-effects (getQueryBaseRoute and 
		  // setSerializer for instance) so invocation order is important here.
		  final String route = abstractQuery.getQueryBaseRoute();
		  if (abstractQuery.getClass().isAssignableFrom(HttpRpcPluginQuery.class)) {
			  if (applyCorsConfig(fullRequest, abstractQuery)) {
				  //fullRequest.release();
				  return;
			  }
			  final HttpRpcPluginQuery pluginQuery = (HttpRpcPluginQuery) abstractQuery;
			  final HttpRpcPlugin rpc = rpc_manager.lookupHttpRpcPlugin(route);
			  if (rpc != null) {
				  rpc.execute(tsdb, pluginQuery);
			  } else {
				  pluginQuery.notFound();
			  }
		  } else if (abstractQuery.getClass().isAssignableFrom(HttpQuery.class)) {
			  final HttpQuery builtinQuery = (HttpQuery) abstractQuery;
			  builtinQuery.setSerializer();
			  if (applyCorsConfig(fullRequest, abstractQuery)) {
				  return;
			  }
			  final HttpRpc rpc = rpc_manager.lookupHttpRpc(route);
			  if (rpc != null) {
				  rpc.execute(tsdb, builtinQuery);
			  } else {
				  builtinQuery.notFound();
			  }
		  } else {
			  throw new IllegalStateException("Unknown instance of AbstractHttpQuery: " 
					  + abstractQuery.getClass().getName());
		  }
	  } catch (BadRequestException ex) {
		  if (abstractQuery == null) {
			  LOG.warn("{} Unable to create query for {}. Reason: {}", ctx.channel(), req, ex);
			  sendStatusAndClose(ctx.channel(), HttpResponseStatus.BAD_REQUEST);
		  } else {
			  abstractQuery.badRequest(ex);
		  }
	  } catch (Exception ex) {
		  exceptions_caught.incrementAndGet();
		  if (abstractQuery == null) {
			  LOG.warn("{} Unexpected error handling HTTP request {}. Reason: {} ", ctx.channel(), req, ex);
			  sendStatusAndClose(ctx.channel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
		  } else {
			  abstractQuery.internalError(ex);
		  }
	  } finally {
	  	if(fullRequest!=null) {
	  		fullRequest.touch("RpcHandler");
	  		final int refs = fullRequest.refCnt();
	  		if(refs > 0) {
	  			ReferenceCountUtil.release(fullRequest, refs);
	  			LOG.debug("--------------------Released:[{}]", refs);
	  		} else {
	  			LOG.debug("--------------------Nothing to release:[{}]", refs);
	  		}
	  	}
	  }
  }
  

//  /**
//   * Finds the right handler for an HTTP query (either built-in or user plugin) 
//   * and executes it. Also handles simple and pre-flight CORS requests if 
//   * configured, rejecting requests that do not match a domain in the list.
//   * @param chan The channel on which the query was received.
//   * @param req The parsed HTTP request.
//   */
//  private void handleHttpQuery(final TSDB tsdb, final Channel chan, final HttpRequest req) {
//    AbstractHttpQuery abstractQuery = null;
//    try {
//      abstractQuery = createQueryInstance(tsdb, req, chan);
//      if (!tsdb.getConfig().enable_chunked_requests() && req.isChunked()) {
//        logError(abstractQuery, "Received an unsupported chunked request: "
//            + abstractQuery.request());
//        abstractQuery.badRequest(new BadRequestException("Chunked request not supported."));
//        return;
//      }
//      // NOTE: Some methods in HttpQuery have side-effects (getQueryBaseRoute and 
//      // setSerializer for instance) so invocation order is important here.
//      final String route = abstractQuery.getQueryBaseRoute();
//      if (abstractQuery.getClass().isAssignableFrom(HttpRpcPluginQuery.class)) {
//        if (applyCorsConfig(req, abstractQuery)) {
//          return;
//        }
//        final HttpRpcPluginQuery pluginQuery = (HttpRpcPluginQuery) abstractQuery;
//        final HttpRpcPlugin rpc = rpc_manager.lookupHttpRpcPlugin(route);
//      if (rpc != null) {
//          rpc.execute(tsdb, pluginQuery);
//      } else {
//          pluginQuery.notFound();
//        }
//      } else if (abstractQuery.getClass().isAssignableFrom(HttpQuery.class)) {
//        final HttpQuery builtinQuery = (HttpQuery) abstractQuery;
//        builtinQuery.setSerializer();
//        if (applyCorsConfig(req, abstractQuery)) {
//          return;
//        }
//        final HttpRpc rpc = rpc_manager.lookupHttpRpc(route);
//        if (rpc != null) {
//          rpc.execute(tsdb, builtinQuery);
//        } else {
//          builtinQuery.notFound();
//        }
//      } else {
//        throw new IllegalStateException("Unknown instance of AbstractHttpQuery: " 
//            + abstractQuery.getClass().getName());
//      }
//    } catch (BadRequestException ex) {
//      if (abstractQuery == null) {
//        LOG.warn("{} Unable to create query for {}. Reason: {}", chan, req, ex);
//        sendStatusAndClose(chan, HttpResponseStatus.BAD_REQUEST);
//      } else {
//        abstractQuery.badRequest(ex);
//      }
//    } catch (Exception ex) {
//      exceptions_caught.incrementAndGet();
//      if (abstractQuery == null) {
//        LOG.warn("{} Unexpected error handling HTTP request {}. Reason: {} ", chan, req, ex);
//        sendStatusAndClose(chan, HttpResponseStatus.INTERNAL_SERVER_ERROR);
//      } else {
//        abstractQuery.internalError(ex);
//      }
//    }
//  }
  
  /**
   * Helper method for sending a status-only HTTP response.  This is used in cases where 
   * {@link #createQueryInstance(TSDB, HttpRequest, Channel)} failed to determine a query
   * and we still want to return an error status to the client.
   */
  private void sendStatusAndClose(final Channel chan, final HttpResponseStatus status) {
    if (chan.isOpen()) {
      final DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
      final ChannelFuture future = chan.write(response);
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("rpc.received", telnet_rpcs_received, "type=telnet");
    collector.record("rpc.received", http_rpcs_received, "type=http");
    collector.record("rpc.received", http_plugin_rpcs_received, "type=http_plugin");
    collector.record("rpc.exceptions", exceptions_caught);
    HttpQuery.collectStats(collector);
    GraphHandler.collectStats(collector);
    PutDataPointRpc.collectStats(collector);
    QueryRpc.collectStats(collector);
  }

  /**
   * Returns the directory path stored in the given system property.
   * @param prop The name of the system property.
   * @return The directory path.
   * @throws IllegalStateException if the system property is not set
   * or has an invalid value.
   */
  static String getDirectoryFromSystemProp(final String prop) {
    final String dir = System.getProperty(prop);
    String err = null;
    if (dir == null) {
      err = "' is not set.";
    } else if (dir.isEmpty()) {
      err = "' is empty.";
    } else if (dir.charAt(dir.length() - 1) != '/') {  // Screw Windows.
      err = "' is not terminated with `/'.";
    }
    if (err != null) {
      throw new IllegalStateException("System property `" + prop + err);
    }
    return dir;
  }
  
  // ---------------------------- //
  // Individual command handlers. //
  // ---------------------------- //
  
  /** For unknown commands. */
  private static final class Unknown implements TelnetRpc {
    public Deferred<Object> execute(final TSDB tsdb, final ChannelHandlerContext ctx,
                                    final String[] cmd) {
      logWarn(ctx.channel(), "unknown command : " + Arrays.toString(cmd));
      ctx.writeAndFlush("unknown command: " + cmd[0] + ".  Try `help'.\n");
      return Deferred.fromResult(null);
    }
  }

  
//  @Override
//	public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
//		ctx.flush();		
//	}
  
  @Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
	  LOG.error("Caught exception", cause);
	  super.exceptionCaught(ctx, cause);
	}
  

  // ---------------- //
  // Logging helpers. //
  // ---------------- //

  private static void logWarn(final AbstractHttpQuery query, final String msg) {
    LOG.warn(query.channel().toString() + ' ' + msg);
  }

  private void logError(final AbstractHttpQuery query, final String msg) {
    LOG.error(query.channel().toString() + ' ' + msg);
  }

  private static void logWarn(final Channel chan, final String msg) {
    LOG.warn(chan.toString() + ' ' + msg);
  }

  private void logError(final Channel chan, final String msg) {
    LOG.error(chan.toString() + ' ' + msg);
  }

  private void logError(final Channel chan, final String msg, final Exception e) {
    LOG.error(chan.toString() + ' ' + msg, e);
  }

}
