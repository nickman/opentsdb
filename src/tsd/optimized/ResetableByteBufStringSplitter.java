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
import java.util.Iterator;

import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import io.netty.util.ReferenceCountUtil;

/**
 * <p>Title: ResetableByteBufStringSplitter</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.optimized.ResetableByteBufStringSplitter</code></p>
 */

public class ResetableByteBufStringSplitter implements Iterable<String>, Iterator<String> {
	
	private static final Charset UTF8 = Charset.forName("UTF8");
	
	/** The character set to read strings as */
	protected final Charset charset;
	/** The copy or reference option */
	protected final boolean copy;
	/** The buf splitter */
	protected ResetableByteBufSplitter splitter;
	
	public ResetableByteBufStringSplitter(final ByteProcessor processor, final boolean copy, final Charset charset) {
		this.charset = charset==null ? UTF8 : charset;
		splitter = new ResetableByteBufSplitter(copy, processor);
		this.copy = copy;
	}
	
	public void reset(final ByteBuf buf) {
		splitter.reset(buf);
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {		
		return splitter.hasNext();
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.Iterator#next()
	 */
	@Override
	public String next() {
		final ByteBuf sbuf = splitter.next(); 
		final String s = sbuf.toString(charset);
		if(copy) ReferenceCountUtil.safeRelease(sbuf);
		return s;
	}

	/**
	 * <p>Not implemented</p>
	 * {@inheritDoc}
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		/* No Op */		
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<String> iterator() {
		return this;
	}
	
	

}

