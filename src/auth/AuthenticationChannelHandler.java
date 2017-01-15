package net.opentsdb.auth;
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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import net.opentsdb.core.TSDB;

/**
 * @since 2.3
 */
public class AuthenticationChannelHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationChannelHandler.class);
  private AuthenticationPlugin authentication = null;

  public AuthenticationChannelHandler(TSDB tsdb) {
    LOG.info("Setting up AuthenticationChannelHandler");
    this.authentication = tsdb.getAuth();
    if (this.authentication == null) {
      LOG.info("No Authentication Plugin Configured");
    }
  }
  
  

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
    e.printStackTrace();
    ctx.channel().close();
  }
  
  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object authCommand) throws Exception {
	  if (this.authentication == null) {
		  LOG.info("Attempted to use null authentication plugin. This should not happen");
		  LOG.debug("Removing Authentication Handler from Connection");
		  ctx.pipeline().remove(this);
	  }
	  try {	      
	      String authResponse = "AUTH_FAIL\r\n";
	      // Telnet Auth
	      if (authCommand instanceof String[]) {
	        LOG.debug("Passing auth command to Authentication Plugin");
	        if (this.authentication.authenticateTelnet((String[]) authCommand)) {
	          LOG.debug("Authentication Completed");
	          authResponse = "AUTH_SUCCESS.\r\n";
	          LOG.debug("Removing Authentication Handler from Connection");
	          ctx.pipeline().remove(this);
	        }
	        ctx.channel().writeAndFlush(authResponse);
	      // HTTTP Auth
	      } else if (authCommand instanceof HttpRequest) {
	        HttpResponseStatus status;
	        if (this.authentication.authenticateHTTP((HttpRequest) authCommand)) {
	          LOG.debug("Authentication Completed");
	          ctx.pipeline().remove(this);
	        } else {
	          LOG.debug("Authentication Failed");
	          status = HttpResponseStatus.FORBIDDEN;
	          HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
	          ctx.channel().writeAndFlush(response);
	        }
	      // Unknown Authentication
	      } else {
	        LOG.error("Unexpected message type "
	                + authCommand.getClass() + ": " + authCommand);
	      }
		  
	  } catch (Exception e) {
		  LOG.error("Unexpected exception caught"
	                + " while serving: " + e);
	  }


  }

}
