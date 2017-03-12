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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseRpc;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;
import org.hbase.async.jsr166e.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ByteProcessor;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.stats.ThreadAllocationReaderImpl;
import net.opentsdb.tsd.ImportResult;

/**
 * <p>Title: OptimizedTelnetJSONPut</p>
 * <p>Description: Another spike to test putting datapoints via streaming JSON</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.optimized.OptimizedTelnetJSONPut</code></p>
 */

public class OptimizedTelnetJSONPut implements OptimizedTelnetRpc {
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");

	
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** Put datapoints counter */
	protected final LongAdder put_datapoints = new LongAdder();
	/** JSON datapoints streamer */
	final JSONDatapointStreamer dpStreamer = new JSONDatapointStreamer();
	
	protected static final ThreadAllocationReaderImpl allocReader = new ThreadAllocationReaderImpl(); 
	
	

	public OptimizedTelnetJSONPut() {

	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.optimized.OptimizedTelnetRpc#execute(net.opentsdb.core.TSDB, io.netty.channel.ChannelHandlerContext, net.opentsdb.tsd.optimized.OptimizedTelnetRpc.OptimizedTelnetRpcCall)
	 */
	@Override
	public Deferred<Object> execute(final TSDB tsdb, final ChannelHandlerContext ctx, final OptimizedTelnetRpcCall rpcCall) {
		String results = null;
		try {
			results = importDataPoints(tsdb, rpcCall.getPayload(), rpcCall.getCommand());
		} catch (Exception ex) {
			log.error("Optimized Put failed", ex);
			results = ImportResult.format(ex); 
		}
		final String finalResult = results;
		ctx.channel().writeAndFlush(finalResult).addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(final ChannelFuture f) throws Exception {
				if(f.isSuccess()) {
					log.debug("Wrote Response: {}", finalResult);
				} else {
					log.error("Error sending response", f.cause());
				}
			
			}
		});
		return null;
	}
	
	
	
	private WritableDataPoints getDataPoints(
			final TSDB tsdb, 
			final Map<String, WritableDataPoints> datapoints,
	        final String metric,
	        final Map<String, String> tags) {
	    final String key = metric + tags;
	    WritableDataPoints dp = datapoints.get(key);
	    if (dp != null) {
	      return dp;
	    }
	    dp = tsdb.newDataPoints();
	    dp.setSeries(metric, tags);
	    dp.setBatchImport(false);  // TODO: config
	    datapoints.put(key, dp);
	    return dp;
	  }

	
	private String importDataPoints(final TSDB tsdb, final ByteBuf buf, final String...cmdOpts) throws IOException {
//		log.info("Importing datapoints from buffer: {}", buf);
//		buf.retain(2);
		final Map<ImportResult, LongAdder> resultMap = ImportResult.resultsMap();
		final LongAdder total = resultMap.get(ImportResult.TOTAL);
		final LongAdder points = resultMap.get(ImportResult.COMPLETE);
		final LongAdder errors = resultMap.get(ImportResult.ERRORS);
		final LongAdder elapsed = resultMap.get(ImportResult.ELAPSED);
		final LongAdder allocated = resultMap.get(ImportResult.ALLOCATED);
		final long id = Thread.currentThread().getId();
		final long start_time = System.nanoTime();
		final long start_mem = allocReader.getAllocatedBytes(id);
		dpStreamer.reset(buf);
		final boolean skip_errors = true; // FIXME
		final AtomicBoolean throttle = new AtomicBoolean(false);
		//final Iterator<String> lines = new ByteBufStringSplitter(buf, ByteProcessor.FIND_CRLF, false, UTF8);
		final Iterator<ByteBuf> lines = new ByteBufSplitter(buf, false, ByteProcessor.FIND_CRLF);
		final HBaseClient client = tsdb.getClient();
		final Map<String, WritableDataPoints> datapoints =
			    new HashMap<String, WritableDataPoints>();		
		//String line = null;
//		ByteBuf line = null;
		try {
			final class Errback implements Callback<Object, Exception> {
				public Object call(final Exception arg) {
					if (arg instanceof PleaseThrottleException) {
						final PleaseThrottleException e = (PleaseThrottleException) arg;
						log.warn("Need to throttle, HBase isn't keeping up.", e);
						throttle.set(true);
						final HBaseRpc rpc = e.getFailedRpc();
						if (rpc instanceof PutRequest) {
							client.put((PutRequest) rpc);  // Don't lose edits.
						}
						return null;
					}
					log.error("Exception caught while processing UDP put" , arg);					
					return arg;
				}
				public String toString() {
					return "Optimized Telnet import errback";
				}
			};
			final Errback errback = new Errback();
			while(dpStreamer.hasNext()) {
				final IncomingDataPoint dp = dpStreamer.next();
				total.increment();
				
//				log.info("Processing DP: {}", dp);
				try {					
					final WritableDataPoints wdp = getDataPoints(tsdb, datapoints, dp.getMetric(), dp.getTags());
					Deferred<Object> d;
					if (Tags.looksLikeInteger(dp.getValue())) {
						d = wdp.addPoint(dp.getTimestamp(), Tags.parseLong(dp.getValue()));
					} else {  // floating point value
						d = wdp.addPoint(dp.getTimestamp(), Float.parseFloat(dp.getValue()));
					}
					d.addErrback(errback);
					points.increment();
					if (throttle.get()) {
						log.info("Throttling...");
						long throttle_time = System.nanoTime();
						try {
							d.joinUninterruptibly();
						} catch (final Exception e) {
							throw new RuntimeException("Should never happen", e);
						}
						throttle_time = System.nanoTime() - throttle_time;
						if (throttle_time < 1000000000L) {
							log.info("Got throttled for only " + throttle_time + 
									"ns, sleeping a bit now");
							try { 
								Thread.sleep(1000); 
							} catch (InterruptedException e) { 
								throw new RuntimeException("interrupted", e); 
							}
						}
						log.info("Done throttling...");
						throttle.set(false);
					}
				} catch (final RuntimeException e) {
					if (skip_errors) {
						errors.increment();
						log.error("Exception: " + e.getMessage());
						log.error("error while processing optimized telnet import "
								+ " dp=[" + dp + "]... Continuing");
						continue;
					} else {
						throw e;
					}
				}
			}
		} catch (RuntimeException e) {
			log.error("Exception caught while processing optimized telnet JSON import", e);
			throw e;
		} finally {
			//ReferenceCountUtil.safeRelease(buf);
		}
		final int tpoints = points.intValue();
		put_datapoints.add(tpoints);
		tsdb.incrementDataPointsAdded(tpoints);
		final long time_delta = System.nanoTime() - start_time;
		elapsed.add(time_delta);
		allocated.add(allocReader.getAllocatedBytes(id) - start_mem);
		
//		log.info(String.format("Processed optimized telnet Import in %d micros, %d data points"
//				+ " (%.1f points/s)",
//				time_delta, tpoints,
//				(tpoints * 1000000.0 / time_delta)));
		return ImportResult.format(resultMap);
	}	


}
