package net.opentsdb.tsd.optimized;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;

import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: ByteBufLineIterator</p>
 * <p>Description: Iterates through lines of text in a UTF8 encoded ByteBuf</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.optimized.ByteBufLineIterator</code></p>
 */

public class ByteBufLineIterator implements Iterable<String>, Iterator<String> {
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	/** The bytebuf to read from */
	protected final ByteBuf buf;
	
	public static void main(String[] args) {
		ByteBuf buf = BufferManager.getInstance().wrap("put\nfoo\nbar\nsna\nfu\n", UTF8);
		final ByteBufLineIterator iter = new ByteBufLineIterator(buf);
		for(String s: iter) {
			System.out.println("[" + s + "]");
		}
	}
	
	/**
	 * Creates a new ByteBufLineIterator
	 * @param buf the buffer the lines will be read from
	 */
	public ByteBufLineIterator(final ByteBuf buf) {		
		this.buf = buf;
	}

	@Override
	public Iterator<String> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		return buf.readableBytes() > 0;
	}

	@Override
	public String next() {
		final int index = buf.forEachByte(ByteProcessor.FIND_CRLF);
		if(index==-1) {
			if(!hasNext()) throw new NoSuchElementException();
			return buf.toString(UTF8);
		}
		final byte[] bytes = new byte[index - buf.readerIndex()];
		buf.readBytes(bytes);
		buf.readByte();
		return new String(bytes, UTF8);
	}

	/**
	 * No Op. Not supported
	 * {@inheritDoc}
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		/* No Op. Not supported */
		
	}
  }