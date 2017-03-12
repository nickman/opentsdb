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

import java.util.Arrays;

import com.stumbleupon.async.Deferred;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import net.opentsdb.core.TSDB;

/**
 * <p>Title: OptimizedTelnetRpc</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.optimized.OptimizedTelnetRpc</code></p>
 */

public interface OptimizedTelnetRpc {
	  /**
	   * Executes this RPC.
	   * @param tsdb The parent TSDB
	   * @param ctx The rpc handler channel handler context on which the RPC was received.
	   * @param command The command received, split.
	   * @return A deferred result.
	   */
	  Deferred<Object> execute(final TSDB tsdb, ChannelHandlerContext ctx, OptimizedTelnetRpcCall rpcCall);

	  
	  public static class OptimizedTelnetRpcCall {
		  final String[] command;
		  final ByteBuf payload;
		  
		  public OptimizedTelnetRpcCall(final String[] command, final ByteBuf payload) {			
			this.command = command;
			this.payload = payload;
		  }

		public String[] getCommand() {
			return command;
		}

		public ByteBuf getPayload() {
			return payload;
		}
		  
		
		public String toString() {
			return new StringBuilder("OptimizedTelnetRpc[\n\tcommand:")
				.append(Arrays.toString(command)).append("\n\tpayload:")
				.append(payload)
				.append("\n\t]")
				.toString();
		}
		  
	  }
	  
}
