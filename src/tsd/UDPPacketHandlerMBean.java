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

import java.util.Map;
import java.util.Set;

/**
 * <p>Title: UDPPacketHandlerMBean</p>
 * <p>Description: JMX MBean interface for {@link UDPPacketHandler}</p> 
 * <p><code>net.opentsdb.tsd.UDPPacketHandlerMBean</code></p>
 */

public interface UDPPacketHandlerMBean {
	/** The UDPPacketHandler JMX object name */
	public static final String OBJECT_NAME = "net.opentsdb.udp:service=UDPPacketHandler";
	
	/**
	 * Returns the addresses of the active UDP addresses
	 * @return the addresses of the active UDP addresses
	 */
	public Set<String> getActiveRemotes();
	
	/**
	 * Returns the count of UDP rpc incovations by remote address
	 * @return the count of UDP rpc incovations by remote address
	 */
	public Map<String, Long> getRpcCountsByRemote();
	
	
}
