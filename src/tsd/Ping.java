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

import java.io.IOException;

import com.stumbleupon.async.Deferred;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpResponseStatus;
import net.opentsdb.core.TSDB;

/**
 * <p>Title: Ping</p>
 * <p>Description: Simple handshake rpc so clients can tell if we're still here.</p> 
 * <p><code>net.opentsdb.tsd.Ping</code></p>
 */

public class Ping implements TelnetRpc, HttpRpc {

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpc#execute(net.opentsdb.core.TSDB, net.opentsdb.tsd.HttpQuery)
	 */
	@Override
	public void execute(final TSDB tsdb, final HttpQuery query) throws IOException {
		query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.TelnetRpc#execute(net.opentsdb.core.TSDB, io.netty.channel.Channel, java.lang.String[])
	 */
	@Override
	public Deferred<Object> execute(final TSDB tsdb, final Channel chan, final String[] command) {
		chan.writeAndFlush("pong");
		return Deferred.fromResult(null);
	}

}
