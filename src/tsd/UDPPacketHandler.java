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
package net.opentsdb.tsd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import net.opentsdb.core.TSDB;

/**
 * <p>Title: UDPPacketHandler</p>
 * <p>Description: TSDB UDP datagram packet handler</p> 
 * <p><code>net.opentsdb.tsd.UDPPacketHandler</code></p>
 */

public class UDPPacketHandler extends SimpleChannelInboundHandler<DatagramPacket> {
	/** The parent TSDB instance */
	protected final TSDB tsdb;
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	
	
	
	/**
	 * Creates a new UDPPacketHandler
	 * @param tsdb The parent TSDB instance
	 */
	public UDPPacketHandler(final TSDB tsdb) {
		if(tsdb==null) throw new IllegalArgumentException("The passed TSDB was null");
		this.tsdb = tsdb;
	}


	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, final DatagramPacket msg) throws Exception {
		
		
	}

}
