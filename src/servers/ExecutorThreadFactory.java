package net.opentsdb.servers;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import net.opentsdb.stats.ThreadPoolMonitor;
import net.opentsdb.utils.Threads;

/**
 * <p>Title: ExecutorThreadFactory</p>
 * <p>Description: Combines an executor and thread factory</p> 
 * <p><code>net.opentsdb.tools.TSDTCPServer.ExecutorThreadFactory</code></p>
 */
public class ExecutorThreadFactory implements Executor, ThreadFactory {
	final ExecutorService executor;
	final ThreadFactory threadFactory;
	final String name;
	final AtomicInteger serial = new AtomicInteger();
	final Map<Long, Thread> threads = new ConcurrentHashMap<Long, Thread>();
	
	ExecutorThreadFactory(final String name, final boolean daemon) {
		this.name = name;
		threadFactory = Threads.newThreadFactory(name, daemon, Thread.NORM_PRIORITY);
		executor = Executors.newCachedThreadPool(this);
		if(executor instanceof ThreadPoolExecutor) {
			ThreadPoolMonitor.installMonitor((ThreadPoolExecutor)executor, name);
		}
	}
	
	public ExecutorThreadFactory(final int threadCount, final String name, final boolean daemon) {
		this.name = name;
		threadFactory = Threads.newThreadFactory(name, daemon, Thread.NORM_PRIORITY);
		executor = Executors.newFixedThreadPool(threadCount, this);
		if(executor instanceof ThreadPoolExecutor) {
			ThreadPoolMonitor.installMonitor((ThreadPoolExecutor)executor, name);
		}		
	}
	
	/**
	 * Stops ths thread pool gently
	 */
	public void shutdown() {
		executor.shutdown();
	}
	
	
	/**
	 * Returns a collection of the thread ids currently operating in this thread pool
	 * @return a collection of thread ids
	 */
	public Map<Long, Thread> getThreads() {
		return Collections.unmodifiableMap(threads);
	}
	
	/**
	 * Executes the passed runnable in the executor
	 * @param command The runnable to execute
	 * @see java.util.concurrent.Executor#execute(java.lang.Runnable)
	 */
	@Override
	public void execute(final Runnable command) {
		executor.execute(command);
	}
	
	/**
	 * Creates a new thread
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
				}
				
			}
		};
		final Thread thread = threadFactory.newThread(wrapper);
		threads.put(thread.getId(), thread);
		return thread;
	}
	
	/**
	 * Returns the executor
	 * @return the executor
	 */
	public Executor executor() {
		return executor;
	}
}