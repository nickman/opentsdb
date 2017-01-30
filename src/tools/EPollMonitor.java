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

import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.epoll.EpollTcpInfo;
import io.netty.channel.group.ChannelGroup;
import net.opentsdb.stats.StatsCollector;

/**
 * <p>Title: EPollMonitor</p>
 * <p>Description: Monitor for epoll stats</p> 
 * <p><code>net.opentsdb.tools.EPollMonitor</code></p>
 * TODO: update javadoc for better descriptions. 
 * TODO: fix aggregation in update. Some should be summed, some averaged....
 * See https://www.frozentux.net/ipsysctl-tutorial/chunkyhtml/tcpvariables.html
 */

public class EPollMonitor implements EPollMonitorMBean {
	
	/** The monitor scheduler */
	private static final ScheduledExecutorService scheduler;
	/** The schedule handle */
	private static final ScheduledFuture<?> handle;
	/** A set of all the epoll monitors */
	private static final Set<EPollMonitor> monitors = new CopyOnWriteArraySet<EPollMonitor>();
	/** Static class logger */
	private static final Logger LOG = LoggerFactory.getLogger(EPollMonitor.class);
	
	/** The elapsed time in ns. of the last update */
	private long updateElapsedNs = 0L;
	/** The clock time of the last update */
	private long updateLastTime = 0L;
	
	/** The epoll channel group */
	private final ChannelGroup channelGroup;
	/**The aggregated epoll caState */
	private int caStateTotal = 0;
	/**The aggregated epoll rcvWscale */
	private int rcvWscaleTotal = 0;
	/**The aggregated epoll rto */
	private long rtoTotal = 0;
	/**The aggregated epoll ato */
	private long atoTotal = 0;
	/**The aggregated epoll sndWscale */
	private int sndWscaleTotal = 0;
	/**The aggregated epoll backoff */
	private int backoffTotal = 0;
	/**The aggregated epoll probes */
	private int probesTotal = 0;
	/**The aggregated epoll retransmits */
	private int retransmitsTotal = 0;
	/**The aggregated epoll sacked */
	private long sackedTotal = 0;
	/**The aggregated epoll lost */
	private long lostTotal = 0;
	/**The aggregated epoll retrans */
	private long retransTotal = 0;
	/**The aggregated epoll fackets */
	private long facketsTotal = 0;
	/**The aggregated epoll lastDataSent */
	private long lastDataSentTotal = 0;
	/**The aggregated epoll lastAckSent */
	private long lastAckSentTotal = 0;
	/**The aggregated epoll lastDataRecv */
	private long lastDataRecvTotal = 0;
	/**The aggregated epoll lastAckRecv */
	private long lastAckRecvTotal = 0;
	/**The aggregated epoll pmtu */
	private long pmtuTotal = 0;
	/**The aggregated epoll rcvSsthresh */
	private long rcvSsthreshTotal = 0;
	/**The aggregated epoll rtt */
	private long rttTotal = 0;
	/**The aggregated epoll rttvar */
	private long rttvarTotal = 0;
	/**The aggregated epoll sndSsthresh */
	private long sndSsthreshTotal = 0;
	/**The aggregated epoll sndCwnd */
	private long sndCwndTotal = 0;
	/**The aggregated epoll advmss */
	private long advmssTotal = 0;
	/**The aggregated epoll reordering */
	private long reorderingTotal = 0;
	/**The aggregated epoll rcvRtt */
	private long rcvRttTotal = 0;
	/**The aggregated epoll rcvSpace */
	private long rcvSpaceTotal = 0;
	/**The aggregated epoll totalRetrans */
	private long totalRetransTotal = 0;
	/**The aggregated epoll sndMss */
	private long sndMssTotal = 0;
	/**The aggregated epoll rcvMss */
	private long rcvMssTotal = 0;
	/**The aggregated epoll unacked */
	private long unackedTotal = 0;
	/**The aggregated epoll options */
	private int optionsTotal = 0;
	/**The aggregated epoll state */
	private int stateTotal = 0;
	/** The tcp info to read epoll stats into for update */
	private final EpollTcpInfo tinfo = new EpollTcpInfo();
	/** The channel type being monitored */
	private final String type;
	
	static {
		scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
			@Override
			public Thread newThread(final Runnable r) {
				final Thread t = new Thread(r, "EPollMonitor");
				t.setDaemon(true);
				return t;
			}
		});
		
		handle = scheduler.scheduleWithFixedDelay(new Runnable(){
			public void run() {
				for(EPollMonitor monitor: monitors) {
					monitor.update();
				}				
			}
		}, 5, 5, TimeUnit.SECONDS);
		LOG.info("EPollTCPMonitor scheduler started: {}", !handle.isCancelled() && !handle.isDone());		
	}
	
	/**
	 * Creates a new EPollMonitor
	 * @param type The type of epoll channels being monitored (e.g. tcp or unix)
	 * @param channelGroup The channel group containing epoll channels
	 * 
	 */
	public EPollMonitor(final String type, final ChannelGroup channelGroup) {
		this.type = type;
		this.channelGroup = channelGroup;
		ObjectName tmp = null;		
		try {
			tmp = new ObjectName(String.format(OBJECT_NAME, type));
			ManagementFactory.getPlatformMBeanServer().registerMBean(this, tmp);
		} catch (Exception ex) {
			tmp = null;
			LOG.warn("Failed to register EPollMonitor for channel type [{}]. Continuing without", type, ex);
		}
		monitors.add(this);
	}
	
	private void update() {
		final long start = System.nanoTime();				
		for(Channel channel : channelGroup) {
			try {
				if(!channel.isOpen()) continue;
				final EpollSocketChannel ch = (EpollSocketChannel)channel;
				ch.tcpInfo(tinfo);
				caStateTotal += tinfo.caState();
				rcvWscaleTotal += tinfo.rcvWscale();
				rtoTotal += tinfo.rto();
				atoTotal += tinfo.ato();
				sndWscaleTotal += tinfo.sndWscale();
				backoffTotal += tinfo.backoff();
				probesTotal += tinfo.probes();
				retransmitsTotal += tinfo.retransmits();
				sackedTotal += tinfo.sacked();
				lostTotal += tinfo.lost();
				retransTotal += tinfo.retrans();
				facketsTotal += tinfo.fackets();
				lastDataSentTotal += tinfo.lastDataSent();
				lastAckSentTotal += tinfo.lastAckSent();
				lastDataRecvTotal += tinfo.lastDataRecv();
				lastAckRecvTotal += tinfo.lastAckRecv();
				pmtuTotal += tinfo.pmtu();
				rcvSsthreshTotal += tinfo.rcvSsthresh();
				rttTotal += tinfo.rtt();
				rttvarTotal += tinfo.rttvar();
				sndSsthreshTotal += tinfo.sndSsthresh();
				sndCwndTotal += tinfo.sndCwnd();
				advmssTotal += tinfo.advmss();
				reorderingTotal += tinfo.reordering();
				rcvRttTotal += tinfo.rcvRtt();
				rcvSpaceTotal += tinfo.rcvSpace();
				totalRetransTotal += tinfo.totalRetrans();
				sndMssTotal += tinfo.sndMss();
				rcvMssTotal += tinfo.rcvMss();
				unackedTotal += tinfo.unacked();
				optionsTotal += tinfo.options();
				stateTotal += tinfo.state();
			} catch (Exception x) {/* No Op */}
		}
		updateLastTime = System.currentTimeMillis();
		updateElapsedNs = System.nanoTime() - start;
	}
	
	/**
	 * Collects the stats and metrics tracked by this instance.
	 * @param collector The collector to use.
	 */
	public void collectStats(final StatsCollector collector) {
		collector.record("tsdserver.epoll.connections", channelGroup.size());
		collector.record("tsdserver.epoll.castate", caStateTotal);
		collector.record("tsdserver.epoll.rcvwscale", rcvWscaleTotal);
		collector.record("tsdserver.epoll.rto", rtoTotal);
		collector.record("tsdserver.epoll.ato", atoTotal);
		collector.record("tsdserver.epoll.sndwscale", sndWscaleTotal);
		collector.record("tsdserver.epoll.backoff", backoffTotal);
		collector.record("tsdserver.epoll.probes", probesTotal);
		collector.record("tsdserver.epoll.retransmits", retransmitsTotal);
		collector.record("tsdserver.epoll.sacked", sackedTotal);
		collector.record("tsdserver.epoll.lost", lostTotal);
		collector.record("tsdserver.epoll.retrans", retransTotal);
		collector.record("tsdserver.epoll.fackets", facketsTotal);
		collector.record("tsdserver.epoll.lastdatasent", lastDataSentTotal);
		collector.record("tsdserver.epoll.lastacksent", lastAckSentTotal);
		collector.record("tsdserver.epoll.lastdatarecv", lastDataRecvTotal);
		collector.record("tsdserver.epoll.lastackrecv", lastAckRecvTotal);
		collector.record("tsdserver.epoll.pmtu", pmtuTotal);
		collector.record("tsdserver.epoll.rcvssthresh", rcvSsthreshTotal);
		collector.record("tsdserver.epoll.rtt", rttTotal);
		collector.record("tsdserver.epoll.rttvar", rttvarTotal);
		collector.record("tsdserver.epoll.sndssthresh", sndSsthreshTotal);
		collector.record("tsdserver.epoll.sndcwnd", sndCwndTotal);
		collector.record("tsdserver.epoll.advmss", advmssTotal);
		collector.record("tsdserver.epoll.reordering", reorderingTotal);
		collector.record("tsdserver.epoll.rcvrtt", rcvRttTotal);
		collector.record("tsdserver.epoll.rcvspace", rcvSpaceTotal);
		collector.record("tsdserver.epoll.totalretrans", totalRetransTotal);
		collector.record("tsdserver.epoll.sndmss", sndMssTotal);
		collector.record("tsdserver.epoll.rcvmss", rcvMssTotal);
		collector.record("tsdserver.epoll.unacked", unackedTotal);
		collector.record("tsdserver.epoll.options", optionsTotal);
		collector.record("tsdserver.epoll.state", stateTotal);		
		collector.record("tsdserver.epoll.updatens", updateElapsedNs);
	}
	
	
	/**
	 * Returns the number of channels being tracked
	 * @return the number of channels
	 */
	public int getChannelCount() {
		return channelGroup.size();
	}
	
	/**
	 * Returns the total epoll caState count
	 * @return the total caState count
	 */
	public int getCaState() {
	   return caStateTotal;
	}

	/**
	 * Returns the total epoll rcvWscale count
	 * @return the total rcvWscale count
	 */
	public int getRcvWscale() {
	   return rcvWscaleTotal;
	}

	/**
	 * Returns the total epoll rto count
	 * @return the total rto count
	 */
	public long getRto() {
	   return rtoTotal;
	}

	/**
	 * Returns the total epoll ato count
	 * @return the total ato count
	 */
	public long getAto() {
	   return atoTotal;
	}

	/**
	 * Returns the total epoll sndWscale count
	 * @return the total sndWscale count
	 */
	public int getSndWscale() {
	   return sndWscaleTotal;
	}

	/**
	 * Returns the total epoll backoff count
	 * @return the total backoff count
	 */
	public int getBackoff() {
	   return backoffTotal;
	}

	/**
	 * Returns the total epoll probes count
	 * @return the total probes count
	 */
	public int getProbes() {
	   return probesTotal;
	}

	/**
	 * Returns the total epoll retransmits count
	 * @return the total retransmits count
	 */
	public int getRetransmits() {
	   return retransmitsTotal;
	}

	/**
	 * Returns the total epoll sacked count
	 * @return the total sacked count
	 */
	public long getSacked() {
	   return sackedTotal;
	}

	/**
	 * Returns the total epoll lost count
	 * @return the total lost count
	 */
	public long getLost() {
	   return lostTotal;
	}

	/**
	 * Returns the total epoll retrans count
	 * @return the total retrans count
	 */
	public long getRetrans() {
	   return retransTotal;
	}

	/**
	 * Returns the total epoll fackets count
	 * @return the total fackets count
	 */
	public long getFackets() {
	   return facketsTotal;
	}

	/**
	 * Returns the total epoll lastDataSent count
	 * @return the total lastDataSent count
	 */
	public long getLastDataSent() {
	   return lastDataSentTotal;
	}

	/**
	 * Returns the total epoll lastAckSent count
	 * @return the total lastAckSent count
	 */
	public long getLastAckSent() {
	   return lastAckSentTotal;
	}

	/**
	 * Returns the total epoll lastDataRecv count
	 * @return the total lastDataRecv count
	 */
	public long getLastDataRecv() {
	   return lastDataRecvTotal;
	}

	/**
	 * Returns the total epoll lastAckRecv count
	 * @return the total lastAckRecv count
	 */
	public long getLastAckRecv() {
	   return lastAckRecvTotal;
	}

	/**
	 * Returns the total epoll pmtu count
	 * @return the total pmtu count
	 */
	public long getPmtu() {
	   return pmtuTotal;
	}

	/**
	 * Returns the total epoll rcvSsthresh count
	 * @return the total rcvSsthresh count
	 */
	public long getRcvSsthresh() {
	   return rcvSsthreshTotal;
	}

	/**
	 * Returns the total epoll rtt count
	 * @return the total rtt count
	 */
	public long getRtt() {
	   return rttTotal;
	}

	/**
	 * Returns the total epoll rttvar count
	 * @return the total rttvar count
	 */
	public long getRttvar() {
	   return rttvarTotal;
	}

	/**
	 * Returns the total epoll sndSsthresh count
	 * @return the total sndSsthresh count
	 */
	public long getSndSsthresh() {
	   return sndSsthreshTotal;
	}

	/**
	 * Returns the total epoll sndCwnd count
	 * @return the total sndCwnd count
	 */
	public long getSndCwnd() {
	   return sndCwndTotal;
	}

	/**
	 * Returns the total epoll advmss count
	 * @return the total advmss count
	 */
	public long getAdvmss() {
	   return advmssTotal;
	}

	/**
	 * Returns the total epoll <a ref="https://paperpicker.wordpress.com/2006/11/07/packet-reordering/">reordering</a> count
	 * @return the total reordering count
	 */
	public long getReordering() {
	   return reorderingTotal;
	}

	/**
	 * Returns the total epoll rcvRtt count
	 * @return the total rcvRtt count
	 */
	public long getRcvRtt() {
	   return rcvRttTotal;
	}

	/**
	 * Returns the total epoll rcvSpace count
	 * @return the total rcvSpace count
	 */
	public long getRcvSpace() {
	   return rcvSpaceTotal;
	}

	/**
	 * Returns the total epoll totalRetrans count
	 * @return the total totalRetrans count
	 */
	public long getTotalRetrans() {
	   return totalRetransTotal;
	}

	/**
	 * Returns the total epoll sndMss count
	 * @return the total sndMss count
	 */
	public long getSndMss() {
	   return sndMssTotal;
	}

	/**
	 * Returns the total epoll rcvMss count
	 * @return the total rcvMss count
	 */
	public long getRcvMss() {
	   return rcvMssTotal;
	}

	/**
	 * Returns the total epoll unacked count
	 * @return the total unacked count
	 */
	public long getUnacked() {
	   return unackedTotal;
	}

	/**
	 * Returns the total epoll options count
	 * @return the total options count
	 */
	public int getOptions() {
	   return optionsTotal;
	}

	/**
	 * Returns the total epoll state count
	 * @return the total state count
	 */
	public int getState() {
	   return stateTotal;
	}

	/**
	 * Returns the clock time of the last update in UDT ms 
	 * @return the time of the last update
	 */
	public long getLastUpdateClockTime() {
		return updateLastTime; 
	}
	
	/**
	 * Returns the date of the last update 
	 * @return the date of the last update
	 */
	public Date getLastUpdateDate() {
		return new Date(updateLastTime); 
	}
	
	/**
	 * Returns the elapsed time of the last update in ns
	 * @return the ns elapsed
	 */
	public long getLastUpdateElapsedNs() {
		return updateElapsedNs;
	}
	
	/**
	 * Returns the elapsed time of the last update in us
	 * @return the micros elapsed
	 */
	public long getLastUpdateElapsedUs() {
		return TimeUnit.NANOSECONDS.toMicros(updateElapsedNs);
	}
	
	/**
	 * Returns the elapsed time of the last update in ms
	 * @return the ms elapsed
	 */
	public long getLastUpdateElapsedMs() {
		return TimeUnit.NANOSECONDS.toMillis(updateElapsedNs);
	}

	/**
	 * Returns the channel type being monitored
	 * @return the channel type being monitored
	 */
	public String getChannelType() {
		return type;
	}
	
	
	

	
}
