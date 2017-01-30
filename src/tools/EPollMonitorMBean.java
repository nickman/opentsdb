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

import java.util.Date;

/**
 * <p>Title: EPollMonitorMBean</p>
 * <p>Description: JMX MBean interface for {@link EPollMonitor} instances</p> 
 * <p><code>net.opentsdb.tools.EPollMonitorMBean</code></p>
 */

public interface EPollMonitorMBean {
	/** The JMX ObjectName format for the 
	 * EPollMonitor management interface. 
	 * Single token is channel type */
	public static final String OBJECT_NAME = "net.opentsdb.epoll:type=%s,service=EPollMonitor";

	/**
	 * Returns the number of channels being tracked
	 * @return the number of channels
	 */
	public int getChannelCount();	
	
	/**
	 * Returns the total epoll caState count
	 * @return the total caState count
	 */
	public int getCaState();

	/**
	 * Returns the total epoll rcvWscale count
	 * @return the total rcvWscale count
	 */
	public int getRcvWscale();

	/**
	 * Returns the total epoll rto count
	 * @return the total rto count
	 */
	public long getRto();

	/**
	 * Returns the total epoll ato count
	 * @return the total ato count
	 */
	public long getAto();

	/**
	 * Returns the total epoll sndWscale count
	 * @return the total sndWscale count
	 */
	public int getSndWscale();

	/**
	 * Returns the total epoll backoff count
	 * @return the total backoff count
	 */
	public int getBackoff();

	/**
	 * Returns the total epoll probes count
	 * @return the total probes count
	 */
	public int getProbes();

	/**
	 * Returns the total epoll retransmits count
	 * @return the total retransmits count
	 */
	public int getRetransmits();

	/**
	 * Returns the total epoll sacked count
	 * @return the total sacked count
	 */
	public long getSacked();

	/**
	 * Returns the total epoll lost count
	 * @return the total lost count
	 */
	public long getLost();

	/**
	 * Returns the total epoll retrans count
	 * @return the total retrans count
	 */
	public long getRetrans();

	/**
	 * Returns the total epoll fackets count
	 * @return the total fackets count
	 */
	public long getFackets();

	/**
	 * Returns the total epoll lastDataSent count
	 * @return the total lastDataSent count
	 */
	public long getLastDataSent();

	/**
	 * Returns the total epoll lastAckSent count
	 * @return the total lastAckSent count
	 */
	public long getLastAckSent();

	/**
	 * Returns the total epoll lastDataRecv count
	 * @return the total lastDataRecv count
	 */
	public long getLastDataRecv();

	/**
	 * Returns the total epoll lastAckRecv count
	 * @return the total lastAckRecv count
	 */
	public long getLastAckRecv();

	/**
	 * Returns the total epoll pmtu count
	 * @return the total pmtu count
	 */
	public long getPmtu();

	/**
	 * Returns the total epoll rcvSsthresh count
	 * @return the total rcvSsthresh count
	 */
	public long getRcvSsthresh();

	/**
	 * Returns the total epoll rtt count
	 * @return the total rtt count
	 */
	public long getRtt();

	/**
	 * Returns the total epoll rttvar count
	 * @return the total rttvar count
	 */
	public long getRttvar();

	/**
	 * Returns the total epoll sndSsthresh count
	 * @return the total sndSsthresh count
	 */
	public long getSndSsthresh();

	/**
	 * Returns the total epoll sndCwnd count
	 * @return the total sndCwnd count
	 */
	public long getSndCwnd();

	/**
	 * Returns the total epoll advmss count
	 * @return the total advmss count
	 */
	public long getAdvmss();

	/**
	 * Returns the total epoll reordering count
	 * @return the total reordering count
	 */
	public long getReordering();

	/**
	 * Returns the total epoll rcvRtt count
	 * @return the total rcvRtt count
	 */
	public long getRcvRtt();

	/**
	 * Returns the total epoll rcvSpace count
	 * @return the total rcvSpace count
	 */
	public long getRcvSpace();

	/**
	 * Returns the total epoll totalRetrans count
	 * @return the total totalRetrans count
	 */
	public long getTotalRetrans();

	/**
	 * Returns the total epoll sndMss count
	 * @return the total sndMss count
	 */
	public long getSndMss();

	/**
	 * Returns the total epoll rcvMss count
	 * @return the total rcvMss count
	 */
	public long getRcvMss();

	/**
	 * Returns the total epoll unacked count
	 * @return the total unacked count
	 */
	public long getUnacked();

	/**
	 * Returns the total epoll options count
	 * @return the total options count
	 */
	public int getOptions();

	/**
	 * Returns the total epoll state count
	 * @return the total state count
	 */
	public int getState();

	/**
	 * Returns the clock time of the last update in UDT ms 
	 * @return the time of the last update
	 */
	public long getLastUpdateClockTime();
	
	/**
	 * Returns the date of the last update 
	 * @return the date of the last update
	 */
	public Date getLastUpdateDate();
	
	/**
	 * Returns the elapsed time of the last update in ns
	 * @return the ns elapsed
	 */
	public long getLastUpdateElapsedNs();
	
	/**
	 * Returns the elapsed time of the last update in us
	 * @return the micros elapsed
	 */
	public long getLastUpdateElapsedUs();
	
	/**
	 * Returns the elapsed time of the last update in ms
	 * @return the ms elapsed
	 */
	public long getLastUpdateElapsedMs();
	
	/**
	 * Returns the channel type being monitored
	 * @return the channel type being monitored
	 */
	public String getChannelType();	

}
