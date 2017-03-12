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

import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.util.ReferenceCountUtil;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.utils.JSONException;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: JSONDatapointStreamer</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.optimized.JSONDatapointStreamer</code></p>
 */

public class JSONDatapointStreamer implements Iterator<IncomingDataPoint>, Iterable<IncomingDataPoint> {
	public static final Charset UTF8 = Charset.forName("UTF8");
	protected ByteBuf buf;
	protected ByteBufInputStream bbis = null;
	protected Reader breader = null;
	protected final IncomingDataPoint dp = new IncomingDataPoint(); 
	protected JsonReader jp = null;
	protected final EnumSet<DPField> fields = EnumSet.allOf(DPField.class);
	protected static final Set<DPField> allFields = Collections.unmodifiableSet(EnumSet.allOf(DPField.class));
	protected final HashMap<String, String> tags = new HashMap<String, String>(8);
	protected final AtomicBoolean open = new AtomicBoolean(false);
	protected String metric = null;
	protected String value = null;
	protected long timestamp = -1;
	JsonToken t = null;
	
	/** Static class logger */
	protected static final Logger log = LoggerFactory.getLogger(JSONDatapointStreamer.class);

	
	private static final String DPARRAY = "[" + 
			"{\"metric\": \"sys.cpu.nice\",\"timestamp\": 1346846400,\"value\": 18,\"tags\": {\"host\": \"web01\",\"dc\": \"lga\"}}," +
			"{\"metric\": \"sys.cpu.nice\",\"timestamp\": 1346846400,\"value\": 12,\"tags\": {\"host\": \"web02\",\"dc\": \"lga\"}}," +
			"{\"metric\": \"sys.cpu.nice\",\"timestamp\": 1346846400,\"value\": 48.3,\"tags\": {\"host\": \"web03\",\"dc\": \"lga\"}}" +
			"]";
	
	
/*
START_ARRAY
START_OBJECT
FIELD_NAME
VALUE_STRING
FIELD_NAME
VALUE_NUMBER_INT
FIELD_NAME
VALUE_NUMBER_INT
FIELD_NAME
START_OBJECT
FIELD_NAME
VALUE_STRING
FIELD_NAME
VALUE_STRINGjp.nextToken();
END_OBJECT
END_OBJECT	
 */
	
	public static void main(String[] args) {
		log("Test");
		final JSONDatapointStreamer streamer = new JSONDatapointStreamer();

		final ByteBuf b = BufferManager.getInstance().wrap(DPARRAY);
//		JsonParser j = JSON.createParser(DPARRAY);
//		JsonToken t = null;
//		try {
//			String n = null;
//			String v = null;
//			outer:
//			while((t = j.nextToken())!= null) {
//				switch(t) {
//				case START_ARRAY:
//				case START_OBJECT:
//					j.nextToken();
//					break;
//				case FIELD_NAME:
//					n = j.getCurrentName();
//					break;
//				case VALUE_STRING:
//				case VALUE_NUMBER_FLOAT:
//				case VALUE_NUMBER_INT:
//					v = j.getValueAsString();
//					log("Data: [" + n + "]:[" + v + "]");
//					break;
//				case END_ARRAY:
//					break outer;
//					
//				}
//			}
//		} catch (Exception x) {}
		log("Readable:" + b);
		streamer.reset(b);
		while(streamer.hasNext()) {
			log("Reading...");
			IncomingDataPoint dpx = streamer.next();
			log(dpx);
		}
		streamer.close();
	}
	
	public static void log(Object fmt) {
		System.out.println(fmt);
	}
	
	public static enum DPField {
		metric,
		timestamp,
		value,
		tags;
		
		
		private static final DPField[] values = values();
		public static final int SIZE = values.length;
		
		public static DPField field(final JsonReader jp) {
			
			try {
				final String name = jp.nextName();
				if(name==null) return null;
				return valueOf(name);
			} catch (Exception ex) {
				throw new JSONException("Failed to get current field name", ex);
			}
		}
	}
	
	
	/**
	 * Creates a new JSONDatapointStreamer
	 */
	public JSONDatapointStreamer() {
	}
	
	public void reset(final ByteBuf buf) {
		close(); // this is closing the passed in buf reference
		this.buf = buf;
//		log.info("Reset with buff [{}]:[{}]", System.identityHashCode(this.buf), this.buf);
		bbis = new ByteBufInputStream(this.buf);
		breader = new InputStreamReader(bbis, UTF8);
		jp = new JsonReader(breader);	
		fields.addAll(allFields);
		try {
			jp.beginArray();
			open.set(true);
		} catch (Exception ex) {
			throw new JSONException("Failed to start parser on buf [" + buf + "]", ex);
		}
	}
	
	public void close() {
		open.set(false);		
		tags.clear();
		metric = null;
		value = null;
		timestamp = -1;
		if(breader!=null) {
			try { breader.close(); } catch (Exception x) {/* No Op */}
		}
		
		if(bbis!=null) {
			try { bbis.close(); } catch (Exception x) {/* No Op */}
		}
		if(this.buf!=null && !this.buf.isReadable()) {
//			log.info("Closing buff [{}]:[{}]", System.identityHashCode(this.buf), this.buf);
			final int refs = this.buf.refCnt();
			if(refs>0) {
				ReferenceCountUtil.safeRelease(this.buf, refs);
			}
		}
		if(jp!=null) {
			try { jp.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	@Override
	public IncomingDataPoint next() {
		try {
			if(!open.get()) throw new NoSuchElementException("JasonParser Closed");
			DPField dpf = null;
			jp.beginObject();
			while(jp.hasNext()) {				
				dpf = DPField.field(jp);
				switch(dpf) {
					case metric:
						metric = jp.nextString();
						fields.remove(dpf);
						break;
					case value:
						value = jp.nextString();
						fields.remove(dpf);							
						break;
					case timestamp:
						timestamp = jp.nextLong();
						fields.remove(dpf);												
						break;						
					case tags:
						jp.beginObject();						
						while (jp.hasNext()) {
							tags.put(jp.nextName(), jp.nextString());
						}
						jp.endObject();
						fields.remove(dpf);
						break;
					default:
						break;
				}			
			}
			jp.endObject();
			if(!fields.isEmpty()) throw new Exception("DataPoint Field[s] missing [" + fields + "]");
			dp.setMetric(metric);
			dp.setTags(new HashMap<String, String>(tags));
			dp.setTimestamp(timestamp);
			dp.setValue(value);
			fields.addAll(allFields);
			if(jp.peek()==JsonToken.END_ARRAY) {
				close();
			}
			return dp;
		} catch (Exception ex) {
			try { jp.close(); } catch (Exception x) {/* No Op */}
			throw new JSONException("Failed to get next datapoint", ex);
		}
	}
	
	

	@Override
	public Iterator<IncomingDataPoint> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		return open.get();
//		try {
//			return jp.peek()!=JsonToken.END_ARRAY;
//		} catch (Exception ex) {
//			throw new JSONException("Failed to check next", ex);
//		}
	}


	@Override
	public void remove() {
		/* No Op */		
	}

}
