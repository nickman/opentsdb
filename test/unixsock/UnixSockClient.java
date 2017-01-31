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
package net.opentsdb.unixsock;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import io.netty.channel.epoll.Epoll;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.Socket;


/**
 * <p>Title: UnixSockClient</p>
 * <p>Description: Unix socket client to test the unix socket server</p> 
 * <p><code>net.opentsdb.unixsock.UnixSockClient</code></p>
 */

public class UnixSockClient {

	public static final Charset UTF8 = Charset.forName("UTF8");
	/**
	 * Creates a new UnixSockClient
	 */
	public UnixSockClient() {
		// TODO Auto-generated constructor stub
	}
	
	public static void log(final Object msg) {
		System.out.println(msg);
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Epoll.ensureAvailability();
		log("Epoll available:" + Epoll.isAvailable());
		final File file = new File("/tmp/opentsdb.sock");
		final DomainSocketAddress address = new DomainSocketAddress(file);
		log("DomainSocketAddress:" + address + "\n\tPort:");
		Socket usock = null;
		try {
			usock = Socket.newSocketDomain();			
			log("Created Socket:" + usock);
			boolean connected = usock.connect(address);
			log("Connected Socket:" + usock + ":" + connected);
			connected = usock.finishConnect();
			log("Connect finished:" + connected + ", Remote:" + usock.remoteAddress() + ", Port:" + usock.remoteAddress().getPort());
			for(int i = 0; i < 1000; i++) {
				final String put = String.format("put sna.foo2 %s 4 dc=dcX%s host=host7\n", System.currentTimeMillis()/1000, i);
				final byte[] bytes = put.getBytes(UTF8);
				final ByteBuffer b = ByteBuffer.allocateDirect(bytes.length);
				b.put(bytes);
	//			final int bytesWritten = usock.sendTo(b, 0, bytes.length, usock.remoteAddress().getAddress(), usock.remoteAddress().getPort());
				final int bytesWritten = usock.write(b, 0, bytes.length);			
				log("Wrote Bytes:" + bytesWritten);
				Thread.sleep(1000);
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		} finally {
			try { usock.close(); } catch (Exception x) {/* No Op */}			
		}
	}

}
