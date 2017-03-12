// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
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
package net.opentsdb.tsd.optimized;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.util.ByteProcessor;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.tsd.optimized.OptimizedTelnetRpc.OptimizedTelnetRpcCall;

/**
 * <p>Title: OptimizedTelnetDecoder</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.optimized.OptimizedTelnetDecoder</code></p>
 */

public class OptimizedTelnetDecoder extends ReplayingDecoder<OptimizedTelnetDecoder.CallDecodeState>  {
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** The call being built by this decoder */
	protected OptimizedTelnetRpcCall call = null;
	/** The command */
	protected String[] command = null;
	/** The decoded OptimizedTelnetRpc */
	protected OptimizedTelnetRpc rpcToCall = null;
	/** The decoded OptimizedRPC */
	protected OptimizedRPC orpc = null;
	/** The number of reads before we drop the current request */
	protected final int maxReads;
	/** The number of reads so far */
	protected int reads = 0;
	/** The parent TSDB instance */
	protected final TSDB tsdb;
	
	protected ByteBuf outBuffer = null; 
	
	private TSDB getTSDB() {
		return tsdb;
	}
	
	public enum OptimizedRPC {
		PUTBATCH(new OptimizedTelnetPut(), "\n\n".getBytes(UTF8)),
		PUTBATCHJSON(new OptimizedTelnetJSONPut(), "]}".getBytes(UTF8));
		
		private OptimizedRPC(final OptimizedTelnetRpc rpc, final byte[] payloadEnd) {
			this.rpc = rpc;
			this.payloadEnd = payloadEnd;
		}
		
		final byte[] payloadEnd;
		final OptimizedTelnetRpc rpc;
		
		public static boolean isOptimizedRPC(final String name) {
			if(name==null || name.trim().isEmpty()) return false;
			try {
				valueOf(name.trim().toUpperCase());
				return true;
			} catch (Exception ex) {
				return false;
			}
		}
		
		public static OptimizedRPC decode(final String name) {
			if(name==null || name.trim().isEmpty()) throw new IllegalArgumentException("The passed name was null or empty");
			return valueOf(name.trim().toUpperCase());
		}
		
	}
	

	public static enum CallDecodeState {
		COMMAND,
		PAYLOAD;
	}	
	
	public OptimizedTelnetDecoder(final TSDB tsdb, final int maxReads) {
		super(CallDecodeState.COMMAND);
		this.maxReads = maxReads;
		this.tsdb = tsdb;
		
	}
	
	protected void reset() {
		reads = 0;
		command = null;
		state(CallDecodeState.COMMAND);
		rpcToCall = null;
		call = null;
		orpc = null;
	}
	
	public static void main(String[] args) {
//		final EmbeddedChannel ec = new EmbeddedChannel(new OptimizedTelnetDecoder(10), new SimpleChannelInboundHandler<OptimizedTelnetRpcCall>() {
//			@Override
//			protected void channelRead0(final ChannelHandlerContext ctx, final OptimizedTelnetRpcCall rpc) throws Exception {
//				System.err.println("channelRead0 RPC:" + rpc);
//				System.err.println("Payload: [" + rpc.getPayload().toString(UTF8) + "]");
//			}
//			@Override
//			public boolean acceptInboundMessage(Object msg) throws Exception {
//				final boolean accepted = msg!=null && (msg instanceof OptimizedTelnetRpcCall);
//				System.err.println("Accepted");
//				return accepted;
//			}
//			@Override
//			public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//				OptimizedTelnetRpcCall rpc = (OptimizedTelnetRpcCall)msg;
//				System.err.println("channelRead RPC:" + rpc);
//				System.err.println("Payload: [" + rpc.getPayload().toString(UTF8) + "]");
//			}
//		});
//		System.err.println("PIPELINE:" + ec.pipeline().names());
//		final ByteBuf commandPayload = BufferManager.getInstance().wrap("putbatch\nfoo\nbar\nsna\nfu\n\n");
//		ec.writeInbound(commandPayload);
//		ec.flushInbound();
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.handler.codec.ByteToMessageDecoder#decode(io.netty.channel.ChannelHandlerContext, io.netty.buffer.ByteBuf, java.util.List)
	 */
	@Override
	protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
		reads++;
		if(reads > maxReads) {
			reset();
			throw new RuntimeException("Max Reads [" + maxReads + "] exceeded");
		}
		int cmdEol = -1;
		byte[] bytes = null;
		switch(state()) {
			case COMMAND:
				cmdEol = in.forEachByte(ByteProcessor.FIND_CRLF);
				if(cmdEol!=-1) {
					bytes = new byte[cmdEol];
					in.readBytes(bytes);
					command = Tags.splitString(new String(bytes, UTF8), ' ');
					command[0] = command[0].toUpperCase();
					if(!OptimizedRPC.isOptimizedRPC(command[0])) {
						log.info("Not optimized command {}. Sending upstream", Arrays.toString(command));
						out.add(in.readBytes(super.actualReadableBytes()));
						reset();
						return;
					} 
					orpc = OptimizedRPC.decode(command[0]);
					rpcToCall = orpc.rpc;
					in.readByte();
//					in.readBytes(bytes);
//					in.discardReadBytes();
					log.debug("Read Optimized Command: {}", Arrays.toString(command));
					outBuffer = in.alloc().buffer(4096);
					checkpoint(CallDecodeState.PAYLOAD);
				} else {
					return;
				}				
			case PAYLOAD:
				if(super.actualReadableBytes()>0) {
					outBuffer.writeBytes(in, super.actualReadableBytes());
//					log.info("Outbuffer: {}", outBuffer);
				}
				if(isPayloadComplete(outBuffer)) {
//					log.info("Payload [{}] complete after {} reads", outBuffer, reads);
					
					
					rpcToCall.execute(tsdb, ctx, new OptimizedTelnetRpcCall(command, outBuffer.retain(2)));
//					ctx.executor().submit(new Runnable() {
//						public void run() {
//							rpc.execute(ctx, rpcCall);
//						}
//					});
					
					reset();
					return;
				} else {
//					log.warn("Payload incomplete after {} reads and {} bytes", reads, outBuffer.readableBytes());										
//					reset();
					return;
				}
			default:
				throw new RuntimeException("Programmer Error. Unimplemented state:" + state());
		}
	}
	
	protected boolean isPayloadComplete(final ByteBuf buf) {
		final int readable = buf.readableBytes();		
		if(readable<2) return false;		
		final int readerIndex = buf.readerIndex();
		final byte[] endDelim = orpc.payloadEnd;
		for(int i = 0; i < endDelim.length; i++) {
			if(endDelim[i] != buf.getByte(readable+readerIndex-1-i)) return false;
		}
		return true;
	}
	
	public String readBuf(final ByteBuf buf) {
		final int readable = super.actualReadableBytes();
		final byte[] bytes = new byte[readable];
		buf.getBytes(buf.readerIndex(), bytes);
		return new String(bytes, UTF8);
	}
	
	public static final byte CR = '\r';
	public static final byte LF = '\n';
	public static final byte END_ARR = ']';
	public static final byte END_OBJ = '}';
	
	protected boolean isCrLf(final byte value) {
		return value == CR || value == LF;
	}

}
