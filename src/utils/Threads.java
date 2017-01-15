// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.util.HashedWheelTimer;


/**
 * Utilities dealing with threads, timers and the like.
 */
public class Threads {
  /** Used to count HashedWheelTimers */
  final static AtomicInteger TIMER_ID = new AtomicInteger();
  
//=================================================================================
//	Issue #918: Netty 4.1+ has no thread name determiner.
//  We can make our own if it's an issue
//=================================================================================
  
//  /** Helps give useful names to the Netty threads */
//  public static class BossThreadNamer implements ThreadNameDeterminer {
//    final static AtomicInteger tid = new AtomicInteger();
//    @Override
//    public String determineThreadName(String currentThreadName,
//        String proposedThreadName) throws Exception {
//      return "OpenTSDB I/O Boss #" + tid.incrementAndGet();
//    }
//  }
//  
//  /** Helps give useful names to the Netty threads */
//  public static class WorkerThreadNamer implements ThreadNameDeterminer {
//    final static AtomicInteger tid = new AtomicInteger();
//    @Override
//    public String determineThreadName(String currentThreadName,
//        String proposedThreadName) throws Exception {
//      return "OpenTSDB I/O Worker #" + tid.incrementAndGet();
//    }
//  }
  
//  /** Simple prepends "OpenTSDB" to all threads */
//  public static class PrependThreadNamer implements ThreadNameDeterminer {
//    @Override
//    public String determineThreadName(String currentThreadName, String proposedThreadName)
//        throws Exception {
//      return "OpenTSDB " + proposedThreadName;
//    }
//  }
  
  
  /**
   * Returns a new HashedWheelTimer with a name and default ticks
   * @param name The name to add to the thread name
   * @return A timer
   */
  public static HashedWheelTimer newTimer(final String name) {
    return newTimer(100, name);
  }
  
  /**
   * Returns a new HashedWheelTimer with a name and default ticks
   * @param ticks How many ticks per second to sleep between executions, in ms
   * @param name The name to add to the thread name
   * @return A timer
   */
  public static HashedWheelTimer newTimer(final int ticks, final String name) {
    return newTimer(ticks, 512, name);
  }
  
  /**
   * Returns a new HashedWheelTimer with a name and default ticks
   * @param ticks How many ticks per second to sleep between executions, in ms
   * @param ticks_per_wheel The size of the wheel
   * @param name The name to add to the thread name
   * @return A timer
   */
  public static HashedWheelTimer newTimer(final int ticks, 
      final int ticks_per_wheel, final String name) {
    return new HashedWheelTimer(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("OpenTSDB Timer-%d " + name + " #" + TIMER_ID.incrementAndGet()).setPriority(Thread.NORM_PRIORITY).build(),
    		ticks, TimeUnit.MILLISECONDS, ticks_per_wheel, true);

  }
  
	/**
	 * Creates a new ThreadFactory 
	 * @param nameFormat The name format as defined in {@link ThreadFactoryBuilder#setNameFormat(String)}
	 * @param daemon true for daemon threads, false otherwise
	 * @param priority The priority of the threads
	 * @return the new ThreadFactory
	 */
	public static ThreadFactory newThreadFactory(final String nameFormat, final boolean daemon, final int priority) {
		return new ThreadFactoryBuilder()
				.setDaemon(daemon)
				.setNameFormat(nameFormat)
				.setPriority(priority)
				.build();
	  }
  
	/**
	 * Creates a new ThreadFactory that creates daemon threads of normal priority 
	 * @param nameFormat The name format as defined in {@link ThreadFactoryBuilder#setNameFormat(String)}
	 * @return the new ThreadFactory
	 */
	  public static ThreadFactory newThreadFactory(final String nameFormat) {
		  return newThreadFactory(nameFormat, true, Thread.NORM_PRIORITY); 
	  }
  
}
