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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import net.opentsdb.core.TSDB;

/**
 * <p>Title: ChannelInitializerFactory</p>
 * <p>Description: Provides channel initializers specific to each TSD server type</p> 
 * <p><code>net.opentsdb.tsd.ChannelInitializerFactory</code></p>
 */

public interface ChannelInitializerFactory {
	
	/**
	 * Creates and returns a ChannelInitializer 
	 * @param tsdb The parent TSDB instance
	 * @return the initialized ChannelInitializer
	 */
	public ChannelInitializer<Channel> initializer(final TSDB tsdb);
	
}
