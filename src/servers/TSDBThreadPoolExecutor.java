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

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hbase.async.jsr166e.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import net.opentsdb.stats.ThreadPoolMonitor;
import net.opentsdb.utils.Threads;

/**
 * <p>Title: TSDBThreadPoolExecutor</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.servers.TSDBThreadPoolExecutor</code></p>
 */

public class TSDBThreadPoolExecutor extends ThreadPoolExecutor implements UncaughtExceptionHandler, RejectedExecutionHandler, ThreadFactory {
	/** The number of available cores */
	public static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
	/** A map of executors keyed by the pool name */
	private static final ConcurrentHashMap<String, TSDBThreadPoolExecutor> executors = new ConcurrentHashMap<String, TSDBThreadPoolExecutor>();

	/** Instance logger */
	private final Logger log;
	/** The pool name */
	private final String poolName;
	/** The backing thread factory */
	private final ThreadFactory threadFactory;
	/** The configured uncaught exception handler */
	private final UncaughtExceptionHandler uncaughtHandler;
	/** The configured rejected task handler */
	private final RejectedExecutionHandler rejectionHandler;
	/** The number of concurrently active threads in this pool */
	private final LongAdder active = new LongAdder();
	/** The total number of rejected submissions */
	private final LongAdder rejected = new LongAdder();
	/** The total number of uncaught exceptions */
	private final LongAdder uncaught = new LongAdder();
	/** The total number of created threads */
	private final LongAdder created = new LongAdder();	
	/** The total number of terminated threads */
	private final LongAdder terminated = new LongAdder();	
	/** Thread tracking */
	private final Map<Long, Thread> threads = new ConcurrentHashMap<Long, Thread>();
	/** Stop flag */
	private final AtomicBoolean stopInitiated = new AtomicBoolean(false);
	/** Indicates if a global shutdown is allowed on this pool */
	private final boolean allowGlobalShutdown;
	
	/**
	 * Stops all registered thread pools
	 * @return a deferred completion
	 */
	public static Deferred<ArrayList<Object>> stopAll() {		
		final List<Deferred<Object>> allDefs = new ArrayList<Deferred<Object>>(executors.size());
		final Collection<TSDBThreadPoolExecutor> exes = new ArrayList<TSDBThreadPoolExecutor>(executors.values()); 
		for(TSDBThreadPoolExecutor exe: exes) {
			allDefs.add(exe.stop());
		}		
		return Deferred.group(allDefs);
	}

	/**
	 * Creates a new TSDBThreadPoolExecutor
	 * @param builder The configured builder
	 */
	private TSDBThreadPoolExecutor(final Builder builder) {
		super(builder.corePoolSize, builder.maximumPoolSize, builder.keepAliveTimeSecs, TimeUnit.SECONDS, builder.queue);
		this.poolName = builder.poolName.replace("-%d", "").replace("%d", "").replace("#", "");
		allowGlobalShutdown = builder.allowGlobalShutdown;
		log = LoggerFactory.getLogger(TSDBThreadPoolExecutor.class.getName() + "." + poolName);
		uncaughtHandler = builder.uncaughtHandler;
		rejectionHandler = builder.rejectionHandler;
		threadFactory = Threads.newThreadFactory(builder.poolName, builder.daemon, builder.threadPriority);
		setThreadFactory(this);
		if(builder.prestart > 0) {
			for(int i = 0; i < builder.prestart; i++) {
				prestartCoreThread();
			}
		}		
		log.info("ThreadPool [{}] started", poolName);
	}
	
	/**
	 * Creates a new pool builder
	 * @param poolName the pool name
	 * @return the builder
	 */
	public static Builder newBuilder(final String poolName) {
		return new Builder(poolName);
	}
	
	
	/**
	 * Stops this thread pool, allowing a 10s period for running tasks to complete.
	 * @return a deferred completion
	 */
	public Deferred<Object> stop() {
		final Deferred<Object> def = new Deferred<Object>();		
		if(allowGlobalShutdown && stopInitiated.compareAndSet(false, true)) {
			final Thread t = new Thread(poolName + "Shutdown") {
				public void run() {
					try {
						if(!awaitTermination(10, TimeUnit.SECONDS)) {
							shutdownNow();
						}
					} catch (InterruptedException iex) {
						shutdownNow();
					}
					log.info(" -- Thread Pool [{}] Shutdown", poolName);
					def.callback(null);						
				}
			};
			t.setDaemon(true);
			t.start();
			return def;
		}
		return Deferred.fromResult(null);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.ThreadPoolExecutor#shutdown()
	 */
	@Override
	public void shutdown() {		
		if(stopInitiated.compareAndSet(false, true)) {
			super.shutdown();
			executors.remove(poolName);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.ThreadPoolExecutor#shutdownNow()
	 */
	@Override
	public List<Runnable> shutdownNow() {
		if(stopInitiated.compareAndSet(false, true)) {
			executors.remove(poolName);
			return super.shutdownNow();
		} else {
			return Collections.emptyList();
		}
	}
	
	/**
	 * Returns a collection of the thread ids currently operating in this thread pool
	 * @return a collection of thread ids
	 */
	public Map<Long, Thread> getThreads() {
		return Collections.unmodifiableMap(threads);
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
	 */
	@Override
	public Thread newThread(final Runnable r) {
		final Runnable wrapper = new Runnable() {
			@Override
			public void run() {
				try {
					r.run();
				} finally {
					threads.remove(Thread.currentThread().getId());
					terminated.increment();
				}
				
			}
		};
		final Thread thread = threadFactory.newThread(wrapper);
		threads.put(thread.getId(), thread);
		created.increment();
		return thread;
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.RejectedExecutionHandler#rejectedExecution(java.lang.Runnable, java.util.concurrent.ThreadPoolExecutor)
	 */
	@Override
	public void rejectedExecution(final Runnable r, final ThreadPoolExecutor executor) {
		rejected.increment();
		if(rejectionHandler!=null) {
			rejectionHandler.rejectedExecution(r, executor);
		} else {
			log.warn("Rejected execution in pool [{}]. Runnable was [{}]", poolName, r);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
	 */
	@Override
	public void uncaughtException(final Thread t, final Throwable e) {
		uncaught.increment();
		if(uncaughtHandler!=null) {
			uncaughtHandler.uncaughtException(t, e);		
		} else {
			log.error("Uncaught exception in pool [{}]. Thread:[{}]", poolName, t, e);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.ThreadPoolExecutor#beforeExecute(java.lang.Thread, java.lang.Runnable)
	 */
	@Override
	protected void beforeExecute(final Thread t, final Runnable r) {
		active.increment();
		super.beforeExecute(t, r);
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.ThreadPoolExecutor#afterExecute(java.lang.Runnable, java.lang.Throwable)
	 */
	@Override
	protected void afterExecute(final Runnable r, final Throwable t) {
		active.decrement();
		super.afterExecute(r, t);
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.ThreadPoolExecutor#getActiveCount()
	 */
	@Override
	public int getActiveCount() {	
		return active.intValue();
	}
	
	/**
	 * Returns the total number of rejected task submissions
	 * @return the total number of rejected task submissions
	 */
	public long getRejectedCount() {
		return rejected.longValue();
	}
	
	/**
	 * Returns the total number of uncaught exceptions
	 * @return the total number of uncaught exceptions
	 */
	public long getUncaughtExceptions() {
		return uncaught.longValue();
	}
	
	/**
	 * Returns the total number of created threads
	 * @return the total number of created threads
	 */
	public long getCreatedThreadCount() {
		return created.longValue();
	}
	
	/**
	 * Returns the total number of terminated threads
	 * @return the total number of terminated threads
	 */
	public long getTerminatedThreadCount() {
		return terminated.longValue();
	}
	
	public static class Builder {
		private final String poolName;
		private int corePoolSize = CORES;
		private int maximumPoolSize = CORES * 3;
		private int queueSize = 1;
		private long keepAliveTimeSecs = 60;
		private int prestart = 1;
		private boolean daemon = true;
		private int threadPriority = Thread.NORM_PRIORITY;
		private UncaughtExceptionHandler uncaughtHandler = null;
		private RejectedExecutionHandler rejectionHandler = null;
		private BlockingQueue<Runnable> queue = null;
		private boolean allowGlobalShutdown = true;
		
		/**
		 * Builds, registers and returns the configured ThreadPoolExecutor  
		 * @return the ThreadPoolExecutor
		 */
		public ThreadPoolExecutor build() {
			if(corePoolSize > maximumPoolSize) throw new IllegalArgumentException("Core pool size [" + corePoolSize + "] was larger than the maximum pool size [" + maximumPoolSize + "] for pool [" + poolName + "]");
			if(queueSize < 2) {
				queue = new SynchronousQueue<Runnable>();
			} else {
				queue = new ArrayBlockingQueue<Runnable>(queueSize, true);
			}
			prestart = Math.min(threadPriority, corePoolSize);
			final String key = poolName.replace("-%d", "").replace("%d", "").replace("#", "");
			if(!executors.containsKey(key)) {
				synchronized(executors) {
					if(!executors.containsKey(key)) {
						final TSDBThreadPoolExecutor pool = new TSDBThreadPoolExecutor(this);
						executors.put(key, pool);
						ThreadPoolMonitor.installMonitor(pool, key);
						return pool;
					}
				}
			}
			throw new IllegalArgumentException("The pool named [" + key + "] already exists");
		}
		
		
		/**
		 * Creates a new Builder
		 * @param poolName The mandatory pool name
		 */
		private Builder(final String poolName) {
			if(poolName==null || poolName.trim().isEmpty()) throw new IllegalArgumentException("The passed pool name was null or empty");
			this.poolName = poolName;
		}

		/**
		 * Indicates if the built pool can be shutdown globally, or if it will be shutdown by its owner
		 * @param allow true to allow, false otherwise
		 * @return this builder
		 */
		public Builder allowGlobalShutdown(final boolean allow) {
			allowGlobalShutdown = allow;
			return this;
		}
		
		/**
		 * Sets the core pool size
		 * @param corePoolSize the core pool size to set
		 * @return this builder
		 */
		public Builder corePoolSize(final int corePoolSize) {
			if(corePoolSize < 0) throw new IllegalArgumentException("Invalid corePoolSize [" + corePoolSize + "] for pool [" + poolName + "]");
			this.corePoolSize = corePoolSize;
			return this;
		}

		/**
		 * Sets the maximum pool size
		 * @param maximumPoolSize the maximum pool size to set
		 * @return this builder
		 */
		public Builder maxPoolSize(final int maximumPoolSize) {
			if(maximumPoolSize < 0) throw new IllegalArgumentException("Invalid maximumPoolSize [" + maximumPoolSize + "] for pool [" + poolName + "]");
			this.maximumPoolSize = maximumPoolSize;
			return this;
		}

		/**
		 * Sets the pool's task queue size
		 * @param queueSize the queue size to set
		 * @return this builder
		 */
		public Builder queueSize(final int queueSize) {
			if(queueSize < 0) throw new IllegalArgumentException("Invalid queueSize [" + queueSize + "] for pool [" + poolName + "]");
			this.queueSize = queueSize;
			return this;
		}

		/**
		 * Sets the pool's non-core thread keep alive time in seconds
		 * @param keepAliveTimeSecs the keep alive time to set
		 * @return this builder
		 */
		public Builder keepAliveTimeSecs(final long keepAliveTimeSecs) {
			if(keepAliveTimeSecs < 0) throw new IllegalArgumentException("Invalid thread keepalive [" + keepAliveTimeSecs + "] for pool [" + poolName + "]");
			this.keepAliveTimeSecs = keepAliveTimeSecs;
			return this;
		}

		/**
		 * Sets the number of core threads to prestart
		 * @param prestart the prestart to set
		 * @return this builder
		 */
		public Builder prestart(final int prestart) {
			if(prestart < 0) throw new IllegalArgumentException("Invalid thread prestart [" + prestart + "] for pool [" + poolName + "]");
			this.prestart = prestart;
			return this;
		}

		/**
		 * Indicates if the pool's threads should be daemon threads
		 * @param daemon true for daemon, false otherwise
		 * @return this builder
		 */
		public Builder daemon(final boolean daemon) {
			this.daemon = daemon;
			return this;
		}

		/**
		 * Sets the pool's uncaught exception handler
		 * @param uncaughtHandler the uncaught exception handler to set
		 * @return this builder
		 */
		public Builder uncaughtHandler(final Thread.UncaughtExceptionHandler uncaughtHandler) {
			if(rejectionHandler==null) throw new IllegalArgumentException("The passed UncaughtExceptionHandler was null for pool [" + poolName + "]");
			this.uncaughtHandler = uncaughtHandler;
			return this;
		}

		/**
		 * Sets the pool's full queue rejection handler
		 * @param rejectionHandler the rejection handler to set
		 * @return this builder
		 */
		public Builder rejectionHandler(final RejectedExecutionHandler rejectionHandler) {
			if(rejectionHandler==null) throw new IllegalArgumentException("The passed RejectedExecutionHandler was null for pool [" + poolName + "]");
			this.rejectionHandler = rejectionHandler;
			return this;
		}
		
	}
	
}
