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

import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Timer;
import net.opentsdb.core.TSDB;

/**
 * Creates a newly configured {@link ChannelPipeline} for a new channel.
 * This class is supposed to be a singleton.
 */
@ChannelHandler.Sharable
public final class PipelineFactory extends ChannelInitializer<Channel> {
	
	// Why this instead of UTF-8 ? I don't know. TODO: find out
	public static final Charset ISO8859 = Charset.forName("ISO-8859-1");
  // Those are entirely stateless and thus a single instance is needed.
  private static final StringEncoder ENCODER = new StringEncoder(ISO8859);
  
  private static final StringArrayDecoder STR_ARRAY_DECODER = new StringArrayDecoder();
  
//  private static final WordSplitter DECODER = new WordSplitter(); // ISO-8859-1
  public static final ByteBuf SPACE = Unpooled.wrappedBuffer(" ".getBytes(ISO8859)).asReadOnly();
  // Those are sharable but maintain some state, so a single instance per
  // PipelineFactory is needed.
  private final ConnectionManager connmgr = new ConnectionManager();
  private final DetectHttpOrRpc HTTP_OR_RPC = new DetectHttpOrRpc();
  private final Timer timer;
  private final IdleStateHandler timeoutHandler;
//  private final ChannelHandler timeoutHandlerX;

  /** Stateless handler for RPCs. */
  private final RpcHandler rpchandler;
  
  /** The TSDB to which we belong */ 
  private final TSDB tsdb;
  
  /** The server side socket timeout. **/
  private final int socketTimeout;
  
  /**
   * Constructor that initializes the RPC router and loads HTTP formatter 
   * plugins. This constructor creates its own {@link RpcManager}.
   * @param tsdb The TSDB to use.
   * @throws RuntimeException if there is an issue loading plugins
   * @throws Exception if the HttpQuery handler is unable to load 
   * serializers
   */
  public PipelineFactory(final TSDB tsdb) {
    this(tsdb, RpcManager.instance(tsdb));
  }

  /**
   * Constructor that initializes the RPC router and loads HTTP formatter 
   * plugins using an already-configured {@link RpcManager}.
   * @param tsdb The TSDB to use.
   * @param manager instance of a ready-to-use {@link RpcManager}.
   * @throws RuntimeException if there is an issue loading plugins
   * @throws Exception if the HttpQuery handler is unable to load 
   * serializers
   */
  public PipelineFactory(final TSDB tsdb, final RpcManager manager) {
    this.tsdb = tsdb;
    this.socketTimeout = tsdb.getConfig().getInt("tsd.core.socket.timeout");
    timer = tsdb.getTimer();
    this.timeoutHandler = new IdleStateHandler(0, 0, this.socketTimeout);
    this.rpchandler = new RpcHandler(tsdb, manager);
    try {
      HttpQuery.initializeSerializerMaps(tsdb);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize formatter plugins", e);
    }
  }
  
  private static boolean isHttp(int magic1, int magic2) {
  	return
  			magic1 == 'G' && magic2 == 'E' || // GET
  			magic1 == 'P' && magic2 == 'O' || // POST
  			magic1 == 'P' && magic2 == 'U' || // PUT
  			magic1 == 'H' && magic2 == 'E' || // HEAD
  			magic1 == 'O' && magic2 == 'P' || // OPTIONS
  			magic1 == 'P' && magic2 == 'A' || // PATCH
  			magic1 == 'D' && magic2 == 'E' || // DELETE
  			magic1 == 'T' && magic2 == 'R' || // TRACE
  			magic1 == 'C' && magic2 == 'O';   // CONNECT
  }  

	@Override
	protected void initChannel(final Channel ch) throws Exception {
	  final ChannelPipeline pipeline = ch.pipeline();		
	  pipeline.addLast("connmgr", connmgr);
	  pipeline.addLast("detect", HTTP_OR_RPC);
	}
  
	/**
	 * Switches the passed context for handling telnet calls
	 * @param ctx The channel handler context of the channel to switch
	 */
	public void switchToTelnet(final Channel channel) {
		final ChannelPipeline p = channel.pipeline();
		p.addLast("connmgr", connmgr);
  		p.addLast("framer", new LineBasedFrameDecoder(1024, true, true));
  		p.addLast("encoder", ENCODER);
  		p.addLast("arrDecoder", new StringArrayDecoder());
  		p.addLast("handler", rpchandler);
		
	}


  /**
   * Dynamically changes the {@link ChannelPipeline} based on the request.
   * If a request uses HTTP, then this changes the pipeline to process HTTP.
   * Otherwise, the pipeline is changed to processes an RPC.
   */
	@ChannelHandler.Sharable
  final class DetectHttpOrRpc extends SimpleChannelInboundHandler<ByteBuf> {
	private final Logger log = LoggerFactory.getLogger(getClass());
	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf in) throws Exception {
  		if (in.readableBytes() < 5) {
  			return;
  		}
  		log.debug("Detecting Http vs. Telnet. Pipeline is {}", ctx.pipeline().names());
  		final int magic1 = in.getUnsignedByte(in.readerIndex());
  		final int magic2 = in.getUnsignedByte(in.readerIndex() + 1);
  		if(isHttp(magic1, magic2)) {
  			log.debug("Switching to Http [{}]", ctx.channel());
  			switchToHttp(ctx, 512 * 1024); //tsdb.getConfig().max_chunked_requests());
  		} else {
  			log.debug("Switching to Telnet [{}]", ctx.channel());
  			switchToTelnet(ctx);
  		}
  		ctx.pipeline().remove(this);
  		in.retain();
  		log.info("Sending [{}] up pipeline {}", in, ctx.pipeline().names());
  		ctx.fireChannelRead(in);  		
	}
	

  	private void switchToTelnet(final ChannelHandlerContext ctx) {
  		ChannelPipeline p = ctx.pipeline();
  		p.addLast("framer", new LineBasedFrameDecoder(1024, true, true));
  		p.addLast("encoder", ENCODER);
//  		p.addLast("decoder", new DelimiterBasedFrameDecoder(1024, true, true, SPACE)); //new DelimiterBasedFrameDecoder(1024, true, SPACE));
  		p.addLast("arrDecoder", new StringArrayDecoder());
  		p.addLast("handler", rpchandler);
  	}

  	private void switchToHttp(final ChannelHandlerContext ctx, final int maxRequestSize) {
  		ChannelPipeline p = ctx.pipeline();
  		p.addLast("compressor", new HttpContentCompressor());
  		p.addLast("httpHandler", new HttpServerCodec());  // TODO: config ?
  		p.addLast("decompressor", new HttpContentDecompressor());
  		p.addLast("aggregator", new HttpObjectAggregator(maxRequestSize)); 
	  	p.addLast("handler", rpchandler);
  	}  
  }
	
	
//p.addLast("logger", new LoggingHandler(getClass(), LogLevel.INFO));
	
}
 