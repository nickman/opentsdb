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
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseRpc;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;
import org.hbase.async.jsr166e.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ByteProcessor;
import io.netty.util.ReferenceCountUtil;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.servers.AbstractTSDServer;
import net.opentsdb.servers.TSDBThreadPoolExecutor;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.stats.ThreadAllocationReaderImpl;
import net.opentsdb.stats.ThreadPoolMonitor;
import net.opentsdb.stats.ThreadStat;
import net.opentsdb.tsd.UDPPacketHandler.UDPCommand;
import net.opentsdb.tsd.optimized.ByteBufSplitter;
import net.opentsdb.utils.buffermgr.BufferManager;

/**
 * <p>Title: UDPPacketHandler</p>
 * <p>Description: TSDB UDP datagram packet handler</p> 
 * <p><code>net.opentsdb.tsd.UDPPacketHandler</code></p>
 */

public class UDPPacketHandler extends SimpleChannelInboundHandler<DatagramPacket> implements UDPPacketHandlerMBean, RemovalListener<String, Map<UDPCommand, LongAdder>>, NotificationBroadcaster {
	/** Singleton instance */
	private static volatile UDPPacketHandler instance = null;
	/** Singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** JMX Notification type prefix */
	public static final String NOTIF_TYPE_PREFIX = "udp.client.";
	/** JMX Notification type for new UDP clients */
	public static final String NOTIF_TYPE_NEW = NOTIF_TYPE_PREFIX + "new";
	/** JMX Notification type for expired UDP clients */
	public static final String NOTIF_TYPE_EXPIRED = NOTIF_TYPE_PREFIX + "expired";
	
	private static final MBeanNotificationInfo[] NOTIF_INFOS = new MBeanNotificationInfo[]{
		new MBeanNotificationInfo(new String[]{NOTIF_TYPE_NEW}, Notification.class.getName(), "Notification emitted when a new UDP client starts"),
		new MBeanNotificationInfo(new String[]{NOTIF_TYPE_EXPIRED}, Notification.class.getName(), "Notification emitted when a UDP client expires")
	}; 
	
	/**
	 * Acquires the UDPPacketHandler singleton instance
	 * @param tsdb The parent TSBD instance
	 * @return the UDPPacketHandler singleton instance
	 */
	public static UDPPacketHandler getInstance(final TSDB tsdb) {
		if(instance==null) {
			if(tsdb==null) throw new IllegalArgumentException("The passed TSDB instance was null");
			synchronized(lock) {
				if(instance==null) {
					instance = new UDPPacketHandler(tsdb);
				}
			}
		}
		return instance;
	}
	
	
	
	/** A counter of all UDP requests */
	protected static final AtomicLong all_requests = new AtomicLong();	
	/** A counter of PUT UDP requests */
	protected static final AtomicLong put_requests = new AtomicLong();
	/** A counter of PUT UDP data points */
	protected static final AtomicLong put_datapoints = new AtomicLong();
	/** A counter of incoming UDP bytes */
	protected static final AtomicLong in_bytes = new AtomicLong();	
	/** A counter of outgoing UDP bytes */
	protected static final AtomicLong out_bytes = new AtomicLong();	
	
	protected static final AtomicLong raw_dps = new AtomicLong();
	protected static final AtomicLong rollup_dps = new AtomicLong();
	protected static final AtomicLong raw_stored = new AtomicLong();
	protected static final AtomicLong rollup_stored = new AtomicLong();
	protected static final AtomicLong hbase_errors = new AtomicLong();
	protected static final AtomicLong unknown_errors = new AtomicLong();
	protected static final AtomicLong invalid_values = new AtomicLong();
	protected static final AtomicLong illegal_arguments = new AtomicLong();
	protected static final AtomicLong unknown_metrics = new AtomicLong();
	protected static final AtomicLong inflight_exceeded = new AtomicLong();
	protected static final AtomicLong writes_blocked = new AtomicLong();
	protected static final AtomicLong writes_timedout = new AtomicLong();
	protected static final AtomicLong requests_timedout = new AtomicLong();
	/** Keep track of the latency of UDP requests.  */
	protected static final Histogram udp_latency = new Histogram(16000, (short) 2, 100);
	/** Keep track of the latency of UDP put requests .  */
	protected static final Histogram put_latency = new Histogram(16000, (short) 2, 100);
	/** Keep track of the latency of UDP stats requests .  */
	protected static final Histogram stats_latency = new Histogram(16000, (short) 2, 100);
	/** Keep track of the latency of UDP cache requests .  */
	protected static final Histogram cache_latency = new Histogram(16000, (short) 2, 100);
	
	/** Keep track of the PUT decompression rate  */
	protected static final Histogram putDecomp = new Histogram(16000, (short) 2, 100);
	
	
	
	/** The parent TSDB instance */
	protected final TSDB tsdb;
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** The buffer manager */
	protected final BufferManager bufferManager;
    /** Indicates if rpc thread monitoring is enabled */
    protected final boolean threadMonitorEnabled;
    /** Thread stats keyed by the UDP rpc */
    protected final Map<UDPCommand, Map<ThreadStat, LongAdder>> rpcThreadStats;
	
	/** Notification serial */
	protected final AtomicLong notifSerial = new AtomicLong();
	/** Async execution pool */
	protected final ThreadPoolExecutor threadPool = TSDBThreadPoolExecutor.newBuilder("UDPPacketAsyncHandler-%d")
			.corePoolSize(5)
			.daemon(true)
			.keepAliveTimeSecs(60)
			.prestart(5)
			.build();
	
	/** Command cache */
	protected final Cache<String, UDPCommandOptions> commandCache = CacheBuilder.newBuilder()
			.concurrencyLevel(Runtime.getRuntime().availableProcessors())
			.initialCapacity(128)
			.maximumSize(1024)
			.softValues()
			.recordStats()
			.build();
	
	/** Remote addresses timedout cache */
	protected final Cache<String, Map<UDPCommand, LongAdder>> rpcsFromRemotes = CacheBuilder.newBuilder()
			.concurrencyLevel(Runtime.getRuntime().availableProcessors())
			.initialCapacity(128)
			.maximumSize(1024)
			.expireAfterAccess(120, TimeUnit.SECONDS)
			.removalListener(this)
			.recordStats()
			.build();
	
	/** Embedded notification broadcaster */
	protected final NotificationBroadcasterSupport broadcaster = new NotificationBroadcasterSupport(NOTIF_INFOS);
	/** The packet handler's JMX ObjectName */
	protected final ObjectName objectName;
	
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	/** Empty String Set const */
	private static final Set<String> EMPTY_SET = Collections.unmodifiableSet(new HashSet<String>(0));
	/** Empty String Map const */
	private static final Map<String, String> EMPTY_MAP = Collections.unmodifiableMap(new HashMap<String, String>(0));
	

	/**
	 * Parses the first line of text in the datagram content to extract 
	 * the command and related subCommands, flags and options
	 * @param buf The buffer to parse from
	 * @return a UDPCommandOptions instance or null if the parse failed.
	 */
	UDPCommandOptions parse(final ByteBuf buf) {		
		try {
			final int commandIndex = buf.forEachByte(ByteProcessor.FIND_CRLF);
			final int readerIndex = commandIndex+1;
			final String cmdLine = buf.readCharSequence(commandIndex, UTF8).toString().trim();
			
			final UDPCommandOptions command = commandCache.get(cmdLine, new Callable<UDPCommandOptions>(){
				@Override
				public UDPCommandOptions call() throws Exception {				
					return new UDPCommandOptions(cmdLine, readerIndex);
				}
			});
			if(command.hasRequestId()) {
				// too unique. we shouldn't cache these.
				commandCache.invalidate(cmdLine);
			}
			return command;
		} catch (Exception ex) {
			String content = null;
			try {
				buf.resetReaderIndex();
				content = buf.toString(UTF8);
			} catch (Exception x) {
				content = "<null>";
			}
			log.error("Failed to parse UDP command [" + content + "]", ex);			
			return null;
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.google.common.cache.RemovalListener#onRemoval(com.google.common.cache.RemovalNotification)
	 */
	@Override
	public void onRemoval(final RemovalNotification<String, Map<UDPCommand, LongAdder>> notification) {
		broadcaster.sendNotification(new Notification(NOTIF_TYPE_EXPIRED, objectName, notifSerial.incrementAndGet(), System.currentTimeMillis(), "UDP Client Expired: [" + notification.getKey() + "]"));
	}

	/**
	 * {@inheritDoc}
	 * @see javax.management.NotificationBroadcaster#addNotificationListener(javax.management.NotificationListener, javax.management.NotificationFilter, java.lang.Object)
	 */
	@Override
	public void addNotificationListener(final NotificationListener listener, final NotificationFilter filter, final Object handback)
			throws IllegalArgumentException {
		broadcaster.addNotificationListener(listener, filter, handback);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see javax.management.NotificationBroadcaster#removeNotificationListener(javax.management.NotificationListener)
	 */
	@Override
	public void removeNotificationListener(final NotificationListener listener) throws ListenerNotFoundException {
		broadcaster.removeNotificationListener(listener);
	}
	
	/**
	 * {@inheritDoc}
	 * @see javax.management.NotificationBroadcaster#getNotificationInfo()
	 */
	@Override
	public MBeanNotificationInfo[] getNotificationInfo() {		
		return broadcaster.getNotificationInfo();
	}
	
	class UDPCommandOptions  {
		final UDPCommand command;
		final Set<String> subCommands;
		final Set<String> flags;
		final Map<String, String> options;
		final int readerIndex;
		final boolean hasRequestId;
		
		static final String CMD_PREFIX = "--";
		
		
		UDPCommandOptions(final String cmdLine, final int readerIndex) {
			this.readerIndex = readerIndex;
			try {
				final String[] commands = Tags.splitString(cmdLine, ' ');
				command = UDPCommand.valueOf(commands[0].trim().toUpperCase());
				if(commands.length < 2) {
					subCommands = EMPTY_SET;
					flags = EMPTY_SET;
					options = EMPTY_MAP;
					hasRequestId = false;
				} else {
					subCommands = new HashSet<String>(command.subCommands.size());
					flags = new HashSet<String>(command.flags.size());
					options = new HashMap<String, String>(command.options.size());
					String cmd = null;
					boolean noSubCommands = false;
					final int maxIndex = commands.length-1;
					for(int i = 1; i < commands.length; i++) {
						cmd = commands[i];
						if(cmd.indexOf(CMD_PREFIX)==0) {
							noSubCommands = true;
							if(command.flags.contains(cmd)) {
								flags.add(cmd);
							} else if(command.options.contains(cmd)) {
								final int eqIndex = cmd.indexOf('=');
								if(eqIndex!=-1) {
									options.put(cmd.substring(0, eqIndex), cmd.substring(eqIndex+1));
								} else {
									if(i==maxIndex) throw new Exception("Missing option value for [" + cmd + "]");
									i++;
									options.put(cmd, commands[i]);
								}
							} else {
								throw new Exception("Unrecognized option or flag [" + cmd + "]");
							}
						} else {
							if(noSubCommands || !command.subCommands.contains(cmd)) throw new Exception("Unexpected sub-command [" + cmd + "]");
							subCommands.add(cmd);
						}						
					}
					hasRequestId = options.containsKey("--request-id");
				}
			} catch (Exception ex) {
				throw new RuntimeException("Failed to parse UDP packet command [" + cmdLine + "]", ex);
			}			
		}
		
		
		boolean hasRequestId() {
			return hasRequestId;
		}
		
		String requestId() {
			return options.get("--request-id");
		}
		
		StringBuilder buildResponse() {
			final StringBuilder b = new StringBuilder(1024);
			b.append(command.name()).append(":");
			if(hasRequestId()) {
				b.append(requestId());
			}
			b.append("\n");
			return b;
			
		}
	}
	
	
	/**
	 * <p>Title: UDPCommand</p>
	 * <p>Description: A functional enumeration of the command supported by the UDP TSD server</p> 
	 * <p><code>net.opentsdb.tsd.UDPPacketHandler.UDPCommand</code></p>
	 */
	public static enum UDPCommand {
		PUTBATCH(null, new String[]{"--request-id"}, new String[]{"--skip-errors", "--send-response"}),
		CACHE(null, new String[]{"--request-id"}, null),
		STATS(null, new String[]{"--request-id"}, new String[]{"--canonical"});
		
		private UDPCommand(final String[] subCommands, final String[] options, final String[] flags) {
			if(subCommands!=null && subCommands.length > 0) {
				final Set<String> tmp = new HashSet<String>(subCommands.length);
				Collections.addAll(tmp, subCommands);
				this.subCommands = Collections.unmodifiableSet(tmp);				
			} else {
				this.subCommands = EMPTY_SET;
			}
			
			if(options!=null && options.length > 0) {
				final Set<String> tmp = new HashSet<String>(options.length);
				Collections.addAll(tmp, options);
				this.options = Collections.unmodifiableSet(tmp);				
			} else {
				this.options = EMPTY_SET;
			}
			if(flags!=null && flags.length > 0) {
				final Set<String> tmp = new HashSet<String>(flags.length);
				Collections.addAll(tmp, flags);
				this.flags = Collections.unmodifiableSet(tmp);				
			} else {
				this.flags = EMPTY_SET;
			}			
		}
		
		public final Set<String> subCommands;
		public final Set<String> flags;
		public final Set<String> options;
		
		private static final UDPCommand[] values = values();
		
		public static Map<UDPCommand, LongAdder> newCommandCounter() {
			final EnumMap<UDPCommand, LongAdder> map = new EnumMap<UDPCommand, LongAdder>(UDPCommand.class);
			for(UDPCommand uc: values) {
				map.put(uc, new LongAdder());
			}
			return Collections.unmodifiableMap(map);			
		}
	}
	
	  /**
	   * Collects the stats and metrics tracked by this instance.
	   * @param collector The collector to use.
	   */
	  public static void collectStats(final StatsCollector collector) {
	    collector.record("udp.latency", udp_latency, "type=all");	    
	    collector.record("udp.received", put_requests, "type=put");
	    collector.record("udp.received", all_requests, "type=all");
	    collector.record("udp.bytes", in_bytes, "dir=in");
	    collector.record("udp.bytes", out_bytes, "dir=out");
	    collector.record("udp.datapoints", put_datapoints, "type=put");	    
	    collector.record("udp.decomp", putDecomp, "type=put");
	  }
	
	
	/**
	 * Creates a new UDPPacketHandler
	 * @param tsdb The parent TSDB instance
	 */
	private UDPPacketHandler(final TSDB tsdb) {
		if(tsdb==null) throw new IllegalArgumentException("The passed TSDB was null");
		this.tsdb = tsdb;
		threadMonitorEnabled = tsdb.getConfig().getBoolean("tsd.rpc.threadmonitoring", false);
		if(threadMonitorEnabled) {
			rpcThreadStats = initRpcThreadStats();
		} else {
			rpcThreadStats = null;
		}
		bufferManager = BufferManager.getInstance();
		ObjectName on = null;
		try {
			on = new ObjectName(OBJECT_NAME);
			ManagementFactory.getPlatformMBeanServer().registerMBean(this, on);
		} catch (Exception ex) {
			log.warn("Failed to register UDPPacketHandler management interface. Will continue without: [{}]", ex);
		}
		objectName = on;
	}
	
	private static Map<UDPCommand, Map<ThreadStat, LongAdder>> initRpcThreadStats() {
		Map<UDPCommand, Map<ThreadStat, LongAdder>> tmp = new EnumMap<UDPCommand, Map<ThreadStat, LongAdder>>(UDPCommand.class);
		for(final UDPCommand cmd: UDPCommand.values()) {
			final Map<ThreadStat, LongAdder> statMap = new EnumMap<ThreadStat, LongAdder>(ThreadStat.class);
			for(ThreadStat stat: ThreadStat.values()) {
				statMap.put(stat, new LongAdder());
			}
			tmp.put(cmd, statMap);
		}
		return Collections.unmodifiableMap(tmp);
	}
	
	
	/**
	 * Increments the counter of calls from a specific sender address
	 * @param sender the sender's address
	 */
	protected void updateRemoteCounts(final InetSocketAddress sender, final UDPCommand command) {
		try {
			final String remote = sender.toString();			
			rpcsFromRemotes.get(remote, new Callable<Map<UDPCommand, LongAdder>>(){
				@Override
				public Map<UDPCommand, LongAdder> call() throws Exception {
					broadcaster.sendNotification(new Notification(NOTIF_TYPE_NEW, objectName, notifSerial.incrementAndGet(), System.currentTimeMillis(), "UDP Client Started: [" + sender.toString() + "]"));
					return UDPCommand.newCommandCounter();
				}
			}).get(command).increment();
		} catch (Exception ex) {
			log.error("Failed to update remote counts", ex);
		}
	}


	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.SimpleChannelInboundHandler#channelRead0(io.netty.channel.ChannelHandlerContext, java.lang.Object)
	 */
	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, final DatagramPacket msg) throws Exception {
		final InetSocketAddress sender = msg.sender();
		final InetSocketAddress recipient = msg.recipient();		
		final long startTime = System.currentTimeMillis();
		all_requests.incrementAndGet();
		final ByteBuf content = msg.content().touch("UDPOriginalContent");
		final int size = content.readableBytes();
		if(size < 5) {
			log.warn("Impossibly small datagram [{}] from [{}]", size, msg.sender());
			return;
		}
		
		log.debug("Processing Datagram: {} bytes", size);
		final boolean wasGZipped;
		final ByteBuf actualContent;
		final int magic1 = content.getUnsignedByte(content.readerIndex());
		final int magic2 = content.getUnsignedByte(content.readerIndex()+1);
		final int d;
		if(isGzip(magic1, magic2)) {
			wasGZipped = true;
			actualContent = bufferManager.buffer(size * 10).touch("UDPUnzippedContent");
			GZIPInputStream gis = null;
			ByteBufInputStream bis = null;
			ByteBufOutputStream bos = null;
			final byte[] buff = new byte[1024];
			int bytesRead = -1;
			try {
				bos = new ByteBufOutputStream(actualContent);
				bis = new ByteBufInputStream(content);
				gis = new GZIPInputStream(bis);
				while((bytesRead = gis.read(buff))!=-1) {
					bos.write(buff, 0, bytesRead);
				}
				bos.flush();				
			} finally {
				if(bos!=null) try { bos.close(); } catch (Exception x) {/* No Op */}
				if(gis!=null) try { gis.close(); } catch (Exception x) {/* No Op */}
				if(bis!=null) try { bis.close(); } catch (Exception x) {/* No Op */}
			}
			int computed = 0;
			in_bytes.addAndGet(actualContent.readableBytes());
			try {
				computed = actualContent.readableBytes() / size * 100;				
			} catch (Exception x) {
				computed = 0;
			}			
			d = computed;
			log.debug("Decomp: final:{}, starting:{} ---> {}", actualContent.readableBytes(), size, d);
		} else {
			wasGZipped = false;
			d = 0;
			actualContent = content;
			actualContent.retain();
			in_bytes.addAndGet(size);
		}						
		handle(d, actualContent, wasGZipped, ctx, sender, recipient).addCallback(new Callback<Void, Map<UDPCommand, ByteBuf>>() {
			@Override
			public Void call(final Map<UDPCommand, ByteBuf> response) throws Exception {
				final long elapsed = System.currentTimeMillis() - startTime;
				try { udp_latency.add((int)elapsed); } catch (Exception x) {/* No Op */}
				
				if(response!=null && !response.isEmpty()) {
					final UDPCommand cmd = response.keySet().iterator().next();					
					switch(cmd) {
					case PUTBATCH:
						put_latency.add((int)elapsed);						
						break;
					case STATS:
						stats_latency.add((int)elapsed);
						break;
					case CACHE:
						cache_latency.add((int)elapsed);
						break;
					default:
						break;
					
					}
					final ByteBuf bb = response.get(cmd);
					if(bb!=null) {
						final int bytes = bb.readableBytes();
						final DatagramPacket dp = new DatagramPacket(bb, msg.sender(), msg.recipient()); 
						ctx.writeAndFlush(dp);
						out_bytes.addAndGet(bytes);
					}
				}
				return null;
			}
		}).addErrback(new Callback<Void, Throwable>() {
			@Override
			public Void call(final Throwable err) throws Exception {
				if(err!=null) log.error("UDP packet handler error", err);
				// TODO
				return null;
			}
		}).addBoth(new Callback<Void, Void>() {
			@Override
			public Void call(final Void arg) throws Exception {
				if(!wasGZipped) ReferenceCountUtil.safeRelease(actualContent);
				return null;
			}
		});
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.UDPPacketHandlerMBean#getActiveRemotes()
	 */
	@Override
	public Set<String> getActiveRemotes() {
		return new HashSet<String>(rpcsFromRemotes.asMap().keySet());
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.UDPPacketHandlerMBean#getRpcCountsByRemote()
	 */
	@Override
	public Map<String, Long> getRpcCountsByRemote() {
		final Map<String, Long> rpcCountsByRemote = new TreeMap<String, Long>();		
		for(Map.Entry<String, Map<UDPCommand, LongAdder>> entry: rpcsFromRemotes.asMap().entrySet()) {
			final String remote = entry.getKey();			
			for(Map.Entry<UDPCommand, LongAdder> centry: entry.getValue().entrySet()) {
				rpcCountsByRemote.put(remote + ":" + centry.getKey().name(), centry.getValue().longValue());
			}
		}
		return rpcCountsByRemote;
	}
	
	
	protected ByteBuf toBuff(final int[] arr, final StringBuilder b) {
		final String s = Arrays.toString(arr).replace(" ", "");
		b.append(Arrays.toString(arr).replace(" ", "").substring(1, s.length()-1));
		return bufferManager.wrap(b, UTF8);
	}
	
	protected Deferred<Map<UDPCommand, ByteBuf>> handle(final int decomp, final ByteBuf buf, final boolean wasGZipped, final ChannelHandlerContext ctx, final InetSocketAddress sender, final InetSocketAddress recipient) {
		final Deferred<Map<UDPCommand, ByteBuf>> def = new Deferred<Map<UDPCommand, ByteBuf>>();
		try {

			threadPool.execute(new Runnable(){
				@Override
				public void run() {
					final long[] threadStats;
					final Map<ThreadStat, LongAdder> rpcStats;
					
					try {
						threadStats = threadMonitorEnabled ? ThreadStat.enter((LongAdder)null) : null;
						final UDPCommandOptions cmd = parse(buf.slice());
						rpcStats = threadMonitorEnabled ? rpcThreadStats.get(cmd.command) : null;
						if(cmd==null) return;												
						updateRemoteCounts(sender, cmd.command);
						switch(cmd.command) {
							case PUTBATCH:
								buf.readerIndex(cmd.readerIndex);
								put_requests.incrementAndGet();
								putDecomp.add(decomp);
								try {
									final String results = importDataPoints(buf, cmd);
									if(cmd.flags.contains("--send-response")) {
										def.callback(Collections.singletonMap(UDPCommand.PUTBATCH, bufferManager.wrap(cmd.command.name() + ":\n" + results)));
									} else {
										def.callback(Collections.singletonMap(UDPCommand.PUTBATCH, null));
									}
								} catch (Exception ex) {
									def.callback(ex);
								}
								break;
							case CACHE:								
								final CacheStats stats = commandCache.stats();
								final StringBuilder b = cmd.buildResponse().append(cmd.command.name()).append(":").append(stats.toString());
								b.append(", Size:").append(commandCache.size());
								b.append(", AvgLoadTime:").append(stats.averageLoadPenalty()).append(" ns\n");
								def.callback(Collections.singletonMap(UDPCommand.CACHE, bufferManager.wrap(b, UTF8)));								
								break;
							case STATS:
								try {
									sendStats(ctx, sender, recipient, cmd);
									def.callback(Collections.singletonMap(UDPCommand.CACHE, null));
								} catch (Exception ex) {
									def.callback(ex);
								}
								break;
							default:
								def.callback(new UnsupportedOperationException("No implementation for UDPCommand [" + cmd.command.name() + "]. Programmer Error."));
						}
						if(threadMonitorEnabled) {							
							ThreadStat.exit(rpcStats, threadStats);
							rpcStats.get(ThreadStat.COUNT).increment();
						}
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					} finally {
						if(wasGZipped) ReferenceCountUtil.safeRelease(buf);				
					}
				}
				
			});
		} catch (Exception ex) {
			def.callback(ex);
		}
		return def;
	}
	
	private static final byte[] CR = "\n".getBytes(UTF8);
	private static final int CRLEN = CR.length;
	
	protected void sendStats(final ChannelHandlerContext ctx, final InetSocketAddress sender,
			final InetSocketAddress recipient, final UDPCommandOptions cmd) {
		ByteBuf statsBuffer = null;
		try {
			final byte[] header = cmd.buildResponse().toString().getBytes(UTF8);								
			statsBuffer = doCollectStats(cmd.flags.contains("--canonical"));
			int total = header.length;			
			int sent = 0;
			int dps = 0;
			boolean hasPoints =  false;
			final ByteBufSplitter splitter = new ByteBufSplitter(statsBuffer, false, ByteProcessor.FIND_CRLF);
			ByteBuf sendBuffer = bufferManager.ioBuffer(2048).writeBytes(header);
			while(splitter.hasNext()) {
				ByteBuf b = splitter.next();
				if(sendBuffer.readableBytes() + b.readableBytes() + CRLEN > 2048) {
					ctx.channel().writeAndFlush(new DatagramPacket(sendBuffer, sender));
					total += sendBuffer.readableBytes();
					sent++;				
					hasPoints = false;
					sendBuffer = bufferManager.ioBuffer(2048).writeBytes(header);
				}
				sendBuffer.writeBytes(b).writeBytes(CR);
				hasPoints = true;
				dps++;				
			}
			if(hasPoints) {
				ctx.channel().writeAndFlush(new DatagramPacket(sendBuffer, sender));
				total += sendBuffer.readableBytes();
			}
			out_bytes.addAndGet(total);
			log.info("[{}] Send Count: {}, DPS: {}", sender, sent, dps);
		} catch (Exception ex) {
			log.error("Failed to process stats", ex);
		} finally {
			if(statsBuffer!=null) {
				ReferenceCountUtil.safeRelease(statsBuffer);
			}
		}
	}
	
				

//				protected void sendStats(final ChannelHandlerContext ctx, final InetSocketAddress sender,
//						final InetSocketAddress recipient, final UDPCommandOptions cmd) {
//					ByteBuf statsBuffer = null;
//					try {
//						final byte[] header = cmd.buildResponse().toString().getBytes(UTF8);								
//						statsBuffer = doCollectStats(cmd.flags.contains("--canonical"));
//						int remainingBytes = statsBuffer.readableBytes(); 
//						int index = -1;
//						int priorIndex = 0;
//						int length = -1;
//						int total = header.length;
//						ByteBuf sendBuffer = bufferManager.ioBuffer(2048).writeBytes(header);
//						while((index = statsBuffer.forEachByte(priorIndex, remainingBytes, ByteProcessor.FIND_CRLF))!=-1) {
//							length = index+1-priorIndex;
//							remainingBytes -= length;
//							if(total + length > 2047) {
//								if(sendBuffer.getChar(sendBuffer.readableBytes()-1)!='\n') sendBuffer.writeChar('\n');
//								ctx.writeAndFlush(new DatagramPacket(sendBuffer, sender, recipient));
//								log.info("Sent stats datagram [{}] bytes", total);
//								out_bytes.addAndGet(total);
//								sendBuffer = bufferManager.ioBuffer(2048).writeBytes(header);
//								total = header.length;
//							}
//							sendBuffer.writeBytes(statsBuffer, index+1, length);
//							total += length;
//							priorIndex = index+1;									
//						}
//						if(total > header.length) {
//							if(sendBuffer.getChar(sendBuffer.readableBytes()-1)!='\n') sendBuffer.writeChar('\n');
//							ctx.writeAndFlush(new DatagramPacket(sendBuffer, sender, recipient));
//							log.info("Sent stats datagram [{}] bytes", total);
//							out_bytes.addAndGet(total);
//						}
//					} catch (Exception ex) {
//						log.error("Failed to process stats", ex);
//					} finally {
//						if(statsBuffer!=null) {
//							ReferenceCountUtil.safeRelease(statsBuffer);
//						}
//					}
//				}
//			});
//		} catch (Exception ex) {
//			def.callback(ex);
//		}
//		return def;
//	}
	
	private static boolean isGzip(int magic1, int magic2) {
		return magic1 == 31 && magic2 == 139;
	}
	
	
	private WritableDataPoints getDataPoints(
			final HashMap<String, WritableDataPoints> datapoints,
	        final String metric,
	        final HashMap<String, String> tags) {
	    final String key = metric + tags;
	    WritableDataPoints dp = datapoints.get(key);
	    if (dp != null) {
	      return dp;
	    }
	    dp = tsdb.newDataPoints();
	    dp.setSeries(metric, tags);
	    dp.setBatchImport(true);
	    datapoints.put(key, dp);
	    return dp;
	  }
	
	protected static final ThreadAllocationReaderImpl allocReader = new ThreadAllocationReaderImpl();
	
	/**
	 * Based on {@link net.opentsdb.tools.TextImporter}
	 * @param tsdb The TSDB instance
	 * @param buf The content to import
	 * @param cmdOpts The PUT command options
	 * @return An in array containing: <ul>
	 * 	<li>The total number of data points submitted</li>
	 *  <li>The total number of successfully processed data points</li>
	 *  <li>The total number of failed data points</li>
	 * </ul>
	 * @throws IOException
	 */
	private String importDataPoints(final ByteBuf buf, final UDPCommandOptions cmdOpts) throws IOException {
		
		final boolean skip_errors = cmdOpts.flags.contains("--skip-errors");
		final AtomicBoolean throttle = new AtomicBoolean(false);
		final String[] lines = Tags.splitString(buf.toString(UTF8), '\n');
		final HBaseClient client = tsdb.getClient();
		final HashMap<String, WritableDataPoints> datapoints =
			    new HashMap<String, WritableDataPoints>();
		String line = null;
		final Map<ImportResult, LongAdder> resultMap = ImportResult.resultsMap();
		final LongAdder total = resultMap.get(ImportResult.TOTAL);
		final LongAdder points = resultMap.get(ImportResult.COMPLETE);
		final LongAdder errors = resultMap.get(ImportResult.ERRORS);
		final LongAdder elapsed = resultMap.get(ImportResult.ELAPSED);
		final LongAdder allocated = resultMap.get(ImportResult.ALLOCATED);
		final long id = Thread.currentThread().getId();
		final long start_time = System.nanoTime();
		final long start_mem = allocReader.getAllocatedBytes(id);
		
		try {
			final class Errback implements Callback<Object, Exception> {
				public Object call(final Exception arg) {
					if (arg instanceof PleaseThrottleException) {
						final PleaseThrottleException e = (PleaseThrottleException) arg;
						log.warn("Need to throttle, HBase isn't keeping up.", e);
						throttle.set(true);
						final HBaseRpc rpc = e.getFailedRpc();
						if (rpc instanceof PutRequest) {
							client.put((PutRequest) rpc);  // Don't lose edits.
						}
						return null;
					}
					log.error("Exception caught while processing UDP put" , arg);					
					return arg;
				}
				public String toString() {
					return "UDP import errback";
				}
			};
			final Errback errback = new Errback();
			for(String sline: lines) {
				line = sline.trim();
				if(line.isEmpty()) continue;
				total.increment();;
				final String[] words = Tags.splitString(line, ' ');
				final String metric = words[0];
				if (metric.length() <= 0) {
					if (skip_errors) {
						errors.increment();
						log.error("invalid metric: " + metric);
						log.error("error while processing UDP import "
								+ " line=" + line + "... Continuing");
						continue;
					} else {
						throw new RuntimeException("invalid metric: " + metric);
					}
				}
				final long timestamp;
				try {
					timestamp = Tags.parseLong(words[1]);
					if (timestamp <= 0) {
						if (skip_errors) {
							errors.increment();
							log.error("invalid timestamp: " + timestamp);
							log.error("error while processing UDP import "
									+ " line=" + line + "... Continuing");
							continue;
						} else {
							throw new RuntimeException("invalid timestamp: " + timestamp);
						}
					}
				} catch (final RuntimeException e) {
					if (skip_errors) {
						errors.increment();
						log.error("invalid timestamp: " + e.getMessage());
						log.error("error while processing UDP import  "
								+ " line=" + line + "... Continuing");
						continue;
					} else {
						throw e;
					}
				}

				final String value = words[2];
				if (value.length() <= 0) {
					if (skip_errors) {
						errors.increment();
						log.error("invalid value: " + value);
						log.error("error while processing UDP import "
								+ " line=" + line + "... Continuing");
						continue;
					} else {
						throw new RuntimeException("invalid value: " + value);
					}
				}

				try {
					final HashMap<String, String> tags = new HashMap<String, String>();
					for (int i = 3; i < words.length; i++) {
						if (!words[i].isEmpty()) {
							Tags.parse(tags, words[i]);
						}
					}

					final WritableDataPoints dp = getDataPoints(datapoints, metric, tags);
					Deferred<Object> d;
					if (Tags.looksLikeInteger(value)) {
						d = dp.addPoint(timestamp, Tags.parseLong(value));
					} else {  // floating point value
						d = dp.addPoint(timestamp, Float.parseFloat(value));
					}
					d.addErrback(errback);
					points.increment();
					if (throttle.get()) {
						log.info("Throttling...");
						long throttle_time = System.nanoTime();
						try {
							d.joinUninterruptibly();
						} catch (final Exception e) {
							throw new RuntimeException("Should never happen", e);
						}
						throttle_time = System.nanoTime() - throttle_time;
						if (throttle_time < 1000000000L) {
							log.info("Got throttled for only " + throttle_time + 
									"ns, sleeping a bit now");
							try { 
								Thread.sleep(1000); 
							} catch (InterruptedException e) { 
								throw new RuntimeException("interrupted", e); 
							}
						}
						log.info("Done throttling...");
						throttle.set(false);
					}
				} catch (final RuntimeException e) {
					if (skip_errors) {
						errors.increment();
						log.error("Exception: " + e.getMessage());
						log.error("error while processing UDP import "
								+ " line=" + line + "... Continuing");
						continue;
					} else {
						throw e;
					}
				}
			}
		} catch (RuntimeException e) {
			log.error("Exception caught while processing UDP import "
					+ " line=[" + line + "]", e);
			throw e;
		}
		final long dpoints = points.longValue();
		put_datapoints.addAndGet(dpoints);
		tsdb.incrementDataPointsAdded(dpoints);
		final long time_delta = System.nanoTime() - start_time;
		elapsed.add(time_delta);
		allocated.add(allocReader.getAllocatedBytes(id) - start_mem);
		
//		log.info(String.format("Processed UDP Import in %d micros, %d data points"
//				+ " (%.1f points/s)",
//				time_delta, points,
//				(points * 1000000.0 / time_delta)));
		return ImportResult.format(resultMap);
	}	
	
	  /**
	   * Helper to record the statistics for the current TSD.
	   * Based on from <code>StatsRpc</code>.
	   * @param canonical true to include host
	   * @param collector The collector class to call for emitting stats
	   */
	  private ByteBuf doCollectStats(final boolean canonical) {
		final ByteBufCollector collector = new ByteBufCollector();
	    collector.addHostTag(canonical);
	    ConnectionManager.collectStats(collector);
	    RpcHandler.collectStats(collector);
	    RpcManager.collectStats(collector);
	    collectThreadStats(collector);
	    tsdb.collectStats(collector);
	    BufferManager.getInstance().collectStats(collector);
	    AbstractTSDServer.collectTSDServerStats(collector);
	    ThreadPoolMonitor.collectStats(collector);
	    return collector.buffer;
	  }

	  /**
	   * Runs through the live threads and counts captures a count of their
	   * states for dumping in the stats page.
	   * Copied from <code>StatsRpc</code>.
	   * @param collector The collector to write to
	   */
	  private void collectThreadStats(final StatsCollector collector) {
	    final Set<Thread> threads = Thread.getAllStackTraces().keySet();
	    final Map<String, Integer> states = new HashMap<String, Integer>(6);
	    states.put("new", 0);
	    states.put("runnable", 0);
	    states.put("blocked", 0);
	    states.put("waiting", 0);
	    states.put("timed_waiting", 0);
	    states.put("terminated", 0);
	    for (final Thread thread : threads) {
	      int state_count = states.get(thread.getState().toString().toLowerCase());
	      state_count++;
	      states.put(thread.getState().toString().toLowerCase(), state_count);
	    }
	    for (final Map.Entry<String, Integer> entry : states.entrySet()) {
	      collector.record("jvm.thread.states", entry.getValue(), "state=" + 
	          entry.getKey());
	    }
	    collector.record("jvm.thread.count", threads.size());
	    if(threadMonitorEnabled) {
		    try {
		        collector.addExtraTag("type", "udp");
		        for (final Map.Entry<UDPCommand, Map<ThreadStat, LongAdder>> entry
		            : rpcThreadStats.entrySet()) {
		        	final String name = "udp-" + entry.getKey().name().toLowerCase();
		        	final Map<ThreadStat, LongAdder> statMap = entry.getValue();
		        	final long[] stats = ThreadStat.compute(statMap, true);
		        	ThreadStat.record(collector, "rpc", name, stats); 
		        }
		      } finally {
		        collector.clearExtraTag("type");
		      }
	    }
	  }

	
	/**
	 * Implements the StatsCollector with ASCII style output in a byte buf. 
	 * Builds a string buffer response to send to the caller.
	 * Based on <code>StatsRpc.ASCIICollector</code>
	 */
	class ByteBufCollector extends StatsCollector {
		final ByteBuf buffer;

		/**
		 * Default constructor
		 * @param headerSpaceBytes The number of bytes to leave blank for the header
		 * May be null. If that's the case, we'll try to write to the {@code buf}
		 */
		public ByteBufCollector() {
			super("tsd");
			buffer = bufferManager.directBuffer(2048);
		}

		/**
		 * Called by the {@link #record} method after a source writes a statistic.
		 */
		@Override
		public final void emit(final String line) {
			buffer.writeCharSequence(line, UTF8);
		}
	}


}
