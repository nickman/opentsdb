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

package net.opentsdb.utils;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * <p>Title: NettySerializers</p>
 * <p>Description: Serializers to handle Netty 4.1+ objects that no longer have <b><code>get</code></b>ers</p> 
 * <p><code>net.opentsdb.utils.NettySerializers</code></p>
 */

public class NettySerializers {
	
	/**
	 * <p>Title: NettyHttpResponseStatusSerializer</p>
	 * <p>Description: JSON serializer for {@link HttpResponseStatus}. Generates as <b><code>{"code":200,"reasonPhrase":"OK"}</code></b></p> 
	 * <p><code>net.opentsdb.utils.NettyHttpResponseStatusSerializer</code></p>
	 */
	public static class NettyHttpResponseStatusSerializer extends JsonSerializer<HttpResponseStatus> {
		/** Sharable static instance */
		public static final NettyHttpResponseStatusSerializer INSTANCE = new NettyHttpResponseStatusSerializer();
		/**
		 * {@inheritDoc}
		 * @see com.fasterxml.jackson.databind.JsonSerializer#serialize(java.lang.Object, com.fasterxml.jackson.core.JsonGenerator, com.fasterxml.jackson.databind.SerializerProvider)
		 */
		@Override
		public void serialize(final HttpResponseStatus status, final JsonGenerator jgen, final SerializerProvider provider) throws IOException, JsonProcessingException {
			jgen.writeStartObject();
			jgen.writeNumberField("code", status.code());
			jgen.writeStringField("reason", status.reasonPhrase());
			jgen.writeEndObject();
		}
	}
	
	private NettySerializers() {}

}
