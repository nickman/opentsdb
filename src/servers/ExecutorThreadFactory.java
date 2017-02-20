package net.opentsdb.servers;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import net.opentsdb.utils.Threads;

/**
 * <p>Title: ExecutorThreadFactory</p>
 * <p>Description: Combines an executor and thread factory</p> 
 * <p><code>net.opentsdb.tools.TSDTCPServer.ExecutorThreadFactory</code></p>
 */
public class ExecutorThreadFactory implements Executor, ThreadFactory {
	final Executor executor;
	final ThreadFactory threadFactory;
	final String name;
	final AtomicInteger serial = new AtomicInteger();
	
	ExecutorThreadFactory(final String name, final boolean daemon) {
		this.name = name;
		threadFactory = Threads.newThreadFactory(name, daemon, Thread.NORM_PRIORITY);
		executor = Executors.newCachedThreadPool(threadFactory);
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
		return threadFactory.newThread(r);
	}
	
	/**
	 * Returns the executor
	 * @return the executor
	 */
	public Executor executor() {
		return executor;
	}
}