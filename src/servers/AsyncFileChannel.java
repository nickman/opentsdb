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
package net.opentsdb.servers;

import java.net.SocketAddress;

import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.EventLoop;

/**
 * <p>Title: AsyncFileChannel</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.servers.AsyncFileChannel</code></p>
 */

public class AsyncFileChannel extends AbstractChannel {

	/**
	 * Creates a new AsyncFileChannel
	 * @param parent
	 */
	public AsyncFileChannel(Channel parent) {
		super(parent);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Creates a new AsyncFileChannel
	 * @param parent
	 * @param id
	 */
	public AsyncFileChannel(Channel parent, ChannelId id) {
		super(parent, id);
		// TODO Auto-generated constructor stub
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.Channel#config()
	 */
	@Override
	public ChannelConfig config() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.Channel#isOpen()
	 */
	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.Channel#isActive()
	 */
	@Override
	public boolean isActive() {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.Channel#metadata()
	 */
	@Override
	public ChannelMetadata metadata() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.AbstractChannel#newUnsafe()
	 */
	@Override
	protected AbstractUnsafe newUnsafe() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.AbstractChannel#isCompatible(io.netty.channel.EventLoop)
	 */
	@Override
	protected boolean isCompatible(EventLoop loop) {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.AbstractChannel#localAddress0()
	 */
	@Override
	protected SocketAddress localAddress0() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.AbstractChannel#remoteAddress0()
	 */
	@Override
	protected SocketAddress remoteAddress0() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.AbstractChannel#doBind(java.net.SocketAddress)
	 */
	@Override
	protected void doBind(SocketAddress localAddress) throws Exception {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.AbstractChannel#doDisconnect()
	 */
	@Override
	protected void doDisconnect() throws Exception {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.AbstractChannel#doClose()
	 */
	@Override
	protected void doClose() throws Exception {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.AbstractChannel#doBeginRead()
	 */
	@Override
	protected void doBeginRead() throws Exception {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.AbstractChannel#doWrite(io.netty.channel.ChannelOutboundBuffer)
	 */
	@Override
	protected void doWrite(ChannelOutboundBuffer in) throws Exception {
		// TODO Auto-generated method stub

	}

}
