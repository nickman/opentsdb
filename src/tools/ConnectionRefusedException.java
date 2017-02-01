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

/**
 * <p>Title: ConnectionRefusedException</p>
 * <p>Description: Exception thrown up the pipeline when a connection is rejected</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tools.ConnectionRefusedException</code></p>
 */

public class ConnectionRefusedException extends Exception {

	/**  */
	private static final long serialVersionUID = -256176043625912266L;

	/**
	 * Creates a new ConnectionRefusedException
	 * @param message The error message
	 */
	public ConnectionRefusedException(final String message) {
		super(message);
	}

}
