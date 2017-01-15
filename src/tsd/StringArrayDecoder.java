// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.package net.opentsdb.tsd;
package net.opentsdb.tsd;

import io.netty.buffer.ByteBuf;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.Charset;
import java.util.List;

import net.opentsdb.core.Tags;


/**
 * <p>Title: StringArrayDecoder</p>
 * <p>Description: Returns the split frames as Strings</p> 
 * <p><code>net.opentsdb.tsd.StringArrayDecoder</code></p>
 */
//@ChannelHandler.Sharable
public class StringArrayDecoder extends ByteToMessageDecoder {
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");

	/**
	 * Converts the passed list of ByteBufs into an array of strings
	 * {@inheritDoc}
	 * @see io.netty.handler.codec.ByteToMessageDecoder#decode(io.netty.channel.ChannelHandlerContext, io.netty.buffer.ByteBuf, java.util.List)
	 */
	@Override
	protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
		out.add(Tags.splitString(in.toString(UTF8), ' '));
		in.clear();
		ReferenceCountUtil.touch(in, "StringArrayDecoder");
	}

}
