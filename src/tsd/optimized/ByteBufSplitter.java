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
import java.util.NoSuchElementException;

import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: ByteBufSplitter</p>
 * <p>Description: Splits a ByteBuf into sub-ByteBufs by a ByteProcessor delimiter.
 * If <code>copy</code> is true, the sub bufs are copies of the data and the reader index
 * of the source buf is not modified. Otherwise, the source buf is "consumed" and returned bufs
 * are slices of the source.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.optimized.ByteBufSplitter</code></p>
 */

public class ByteBufSplitter implements Iterable<ByteBuf>, Iterator<ByteBuf> {
	/** The ByteBuf to split */
	protected final ByteBuf buf;
	/** The copy or reference option */
	protected final boolean copy;
	/** The byte processor to split with */
	protected final ByteProcessor processor;
	
	
	final int total;
	int rindex = 0;
	
	public static void main(String[] args) {
		final Charset UTF8 = Charset.forName("UTF8");
		ByteBuf buf = BufferManager.getInstance().wrap("put\nfoo\nbar\nsna\nfu\n\n", UTF8);
		final ByteBufSplitter iter = new ByteBufSplitter(buf, false, ByteProcessor.FIND_CRLF);
		int loops = 0;
		for(ByteBuf b: iter) {
			System.out.println("[" + b.toString(UTF8) + "]");
			loops++;
			if(loops==10) throw new RuntimeException("Overloop");			
		}
		System.out.println("Remaining Buff: [" + buf + "]");
	}
	
	
	
	/**
	 * Creates a new ByteBufSplitter
	 * @param buf The buffer to split
	 * @param copy true to return buffer copies, false to return sub-references to the original
	 * @param processor The byte processor to split with
	 */
	public ByteBufSplitter(final ByteBuf buf, final boolean copy, final ByteProcessor processor) {		
		this.buf = buf;
		this.copy = copy;
		this.processor = processor;
		total = buf.readableBytes();
	}


	/**
	 * {@inheritDoc}
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		if(!copy) {
			return buf.readableBytes() > 0;
		}
		return rindex  < total; 
	}


	@Override
	public ByteBuf next() {
		if(!hasNext()) throw new NoSuchElementException();
		final int index = copy ? buf.forEachByte(rindex, total-rindex, processor) : buf.forEachByte(processor);
		if(!copy) {
			final ByteBuf r;
			if(index==-1) {
				r = buf.slice(buf.readerIndex(), buf.readableBytes());
				buf.readerIndex(buf.readerIndex() + buf.readableBytes());
				//return buf.readBytes(buf.readableBytes());
			} else {
//				ByteBuf r =  buf.readBytes(index - buf.readerIndex());
				final int length = index - buf.readerIndex();
				r = buf.slice(buf.readerIndex(), length);
				buf.readerIndex(buf.readerIndex() + length + 1);
			}
			return r;
		}
		final int x;
		final ByteBuf cop;
		if(index==-1) {
			x = total - rindex;
			cop =  buf.alloc().buffer(x);
			buf.getBytes(rindex, cop, 0, x);
//			cop = buf.slice(rindex, x);
			rindex = total;
		} else {
			x = index - rindex;
			cop =  buf.alloc().buffer(index);
			buf.getBytes(rindex, cop, 0, x);
//			cop = buf.slice(rindex, x);
			rindex += x + 1;
		}
		cop.writerIndex(x);
		return cop;			
	}


	@Override
	public void remove() {
		/* No Op */		
	}


	/**
	 * {@inheritDoc}
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<ByteBuf> iterator() {
		return this;
	}
	
	
}
