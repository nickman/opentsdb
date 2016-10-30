/**
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 */
package net.opentsdb.tools;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.PipelineFactory;
import net.opentsdb.tsd.RpcManager;
import net.opentsdb.utils.Config;

/**
 * <p>Title: TSDTCPChannelFactory</p>
 * <p>Description: The main TSD channel factory and netty bootstrap singleton</p> 
 */

public class TSDTCPChannelFactory {
	/** The singleton instance */
	private static volatile TSDTCPChannelFactory instance = null;
	/** The singleton instance ctor guard */
	private static final Object lock = new Object();	
	/** Static class logger */
	private static final Logger log = LoggerFactory.getLogger(OpenTSDBMain.class);

	/** The netty channel factory */
	private ServerSocketChannelFactory channelFactory;
	/** The netty server bootstrap */
	private ServerBootstrap server; 
	/** The port netty will listen on */
	private int port;
	/** The netty pipeline factory */
	private PipelineFactory pipelineFactory;
	/** The interface netty will bind to */
	private InetAddress bindAddress;
	/** The socket netty will bind to */
	final InetSocketAddress socketAddress;	
	/** The TSDB instance */
	private TSDB tsdb = null;
	/** The final config */
	private final Config config;
	/** The command line argp */
	private ArgP argp = null;
	/** The number of worker threads */
	private final int workerThreads;
	
	/** The JVM process PID */
	public static final String PID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	
	/**
	 * Initializes and returns the initialized TSDTCPChannelFactory singleton instance.
	 * @param config The prepared TSD configuration
	 * @param cmdLineArgs The command line arguments 
	 * @return the TSDTCPChannelFactory singleton instance
	 */
	static TSDTCPChannelFactory init(final Config config) {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new TSDTCPChannelFactory(config);
				}
			}
		}
		return instance;
	}
	
	private TSDTCPChannelFactory(final Config config) {
		this.config = config;
		final boolean async = config.getBoolean("tsd.network.async_io");
		if(async) {
			workerThreads = config.getInt("tsd.network.worker_threads");
	          channelFactory = new NioServerSocketChannelFactory(
		              Executors.newCachedThreadPool(), Executors.newCachedThreadPool(),
		              workerThreads);
		} else {
			workerThreads = -1;
        	channelFactory = new OioServerSocketChannelFactory(
  	              Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		}

        try {
          tsdb = new TSDB(config);
          tsdb.initializePlugins(true);          
          // Make sure we don't even start if we can't find our tables.
          tsdb.checkNecessaryTablesExist().joinUninterruptibly();
          registerShutdownHook();
          pipelineFactory = new PipelineFactory(tsdb, RpcManager.instance(tsdb), config.getInt("tsd.core.connections.limit"));
          server = getBootstrap();
          
          bindAddress = InetAddress.getByName(config.getString("tsd.network.bind"));
          port = config.getInt("tsd.network.port"); 
          socketAddress = new InetSocketAddress(bindAddress, port);
          server.bind(socketAddress);
          log.info("Process {} Ready to serve on {}", PID, socketAddress);
        } catch (Throwable e) {
          channelFactory.releaseExternalResources();
          try {
            if (tsdb != null)
              tsdb.shutdown().joinUninterruptibly();
          } catch (Exception e2) {
            log.error("Failed to shutdown HBase client", e2);
          }
          throw new RuntimeException("Initialization failed", e);
        }
        // The server is now running in separate threads, we can exit main.		
	}
	
	
	private ServerBootstrap getBootstrap() {
        final ServerBootstrap server = new ServerBootstrap(channelFactory);

        server.setPipelineFactory(pipelineFactory);
        if (config.hasProperty("tsd.network.backlog")) {
          server.setOption("backlog", config.getInt("tsd.network.backlog")); 
        }
        server.setOption("child.tcpNoDelay", 
            config.getBoolean("tsd.network.tcp_no_delay"));
        server.setOption("child.keepAlive", 
            config.getBoolean("tsd.network.keep_alive"));
        server.setOption("reuseAddress", 
            config.getBoolean("tsd.network.reuse_address"));
        
        return server;

	}
	
	private void registerShutdownHook() {
		final class TSDBShutdown extends Thread {
			public TSDBShutdown() {
				super("TSDBShutdown");
			}
			public void run() {
				try {
					tsdb.shutdown().join();
				} catch (Exception e) {
					LoggerFactory.getLogger(TSDBShutdown.class)
					.error("Uncaught exception during shutdown", e);
				}
			}
		}
		Runtime.getRuntime().addShutdownHook(new TSDBShutdown());
	}
	
	
    /** Prints usage and exits with the given retval. */
    static void usage(final ArgP argp, final String errmsg, final int retval) {
      System.err.println(errmsg);
      System.err.println(new ConfigArgP().getDefaultUsage());
      if (argp != null) {
        System.err.print(argp.usage());
      }
      System.exit(retval);
    }
    
    
    
    
	
	
	
}
