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
package net.opentsdb.tools;

import net.opentsdb.core.TSDB;

/**
 * <p>Title: UNIXTSDServer</p>
 * <p>Description: Unix Domain Socket TSD server implementation</p> 
 * <p><code>net.opentsdb.tools.UNIXTSDServer</code></p>
 */

public class UNIXTSDServer extends AbstractTSDServer {
	
	private UNIXTSDServer(TSDB tsdb) {
		super(tsdb, TSDProtocol.UNIX);
	}

}
