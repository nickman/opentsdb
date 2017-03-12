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
 * <p>Title: ResetableByteBufSplitter</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.optimized.ResetableByteBufSplitter</code></p>
 */
public class ResetableByteBufSplitter implements Iterable<ByteBuf>, Iterator<ByteBuf> {
	/** The ByteBuf to split */
	protected ByteBuf buf = null;
	/** The copy or reference option */
	protected final boolean copy;
	/** The byte processor to split with */
	protected final ByteProcessor processor;
	
	
	int total;
	int rindex = 0;
	
	/**
	 * Creates a new ByteBufSplitter
	 * @param buf The buffer to split
	 * @param copy true to return buffer copies, false to return sub-references to the original
	 * @param processor The byte processor to split with
	 */
	public ResetableByteBufSplitter(final boolean copy, final ByteProcessor processor) {		
		this.copy = copy;
		this.processor = processor;
	}
	
	public void reset(final ByteBuf buf) {
		this.buf = buf;
		total = this.buf.readableBytes();
		rindex = 0;
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

