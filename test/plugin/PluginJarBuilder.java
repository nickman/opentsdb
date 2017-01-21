// This file is part of OpenTSDB.
// Copyright (C) 2013-2014  The OpenTSDB Authors.
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
package net.opentsdb.plugin;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;



/**
 * <p>Title: PluginJarBuilder</p>
 * <p>Description: Dynamic plugin JAR builder</p> 
 * <p><code>net.opentsdb.plugin.PluginJarBuilder</code></p>
 */

public class PluginJarBuilder {
	/** The unqualfied file name the jar will be written to */
	private final String name;
	/** Sets of classes keyed by the plugin type name */ 
	private final Map<String, Set<Class<?>>> services = new HashMap<String, Set<Class<?>>>();
	
	/** The names of all supported plugin types (the names of the base abstract classes) */
	public static final Set<String> PLUGIN_TYPES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
			"net.opentsdb.auth.AuthenticationPlugin", "net.opentsdb.core.WriteableDataPointFilterPlugin", 
			"net.opentsdb.search.SearchPlugin", "net.opentsdb.tools.StartupPlugin",  
			"net.opentsdb.tsd.HttpRpcPlugin", "net.opentsdb.tsd.RpcPlugin",
			"net.opentsdb.tsd.RTPublisher", "net.opentsdb.uid.UniqueIdFilterPlugin",
			"net.opentsdb.tsd.StorageExceptionHandler"
			
	)));
	
	/**
	 * Creates a new builder
	 * @return the builder
	 * @param name The unqualfied file name the jar will be written to
	 */
	public static PluginJarBuilder newBuilder(final String name) {
		if(name==null || name.trim().isEmpty()) throw new IllegalArgumentException("The passed file name was null or empty");
		return new PluginJarBuilder(name.trim());
	}
	
	/**
	 * Adds a plugin to be included in the built jar
	 * @param pluginClassName The plugin class name
	 * @return this builder
	 */
	public PluginJarBuilder addPlugin(final String pluginClassName) {
		if(pluginClassName==null || pluginClassName.trim().isEmpty()) throw new IllegalArgumentException("The passed plugin class name was null or empty");
		try {
			final Class<?> clazz = Class.forName(pluginClassName.trim());
			Class<?> parent = clazz.getSuperclass();
			String pluginType = null;
			while(parent!=Object.class) {
				if(PLUGIN_TYPES.contains(parent.getName())) {
					pluginType = parent.getName();
					break;
				}
				parent = clazz.getSuperclass();
			}
			if(pluginType==null) throw new RuntimeException("Failed to determine the plugin type for class [" + pluginClassName + "]");
			Set<Class<?>> impls = services.get(pluginType);
			if(impls==null) {
				impls = new HashSet<Class<?>>();
				services.put(pluginType, impls);
			}
			impls.add(parent);
			return this;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to add the plugin class name [" + pluginClassName + "]", ex);
		}
	}
	
	/**
	 * Builds the jar file and returns the fully qualified file name.
	 * The jar file will be deleted on JVM exit and will not include the actual classes.
	 * @return the fully qualified file name of the built jar file
	 */
	public String build() {
		return build(false, true);
	}
	
	
	public static void main(String[] args) {
		System.out.println(
			newBuilder("foo").build(false, false)
		);
	}
	
	/**
	 * Builds the jar file and returns the fully qualified file name
	 * @param includeClasses true to include the classes in the jar, false otherwise
	 * @param deleteOnExit true to delete the created file on JVM exit, false otherwise
	 * @return the fully qualified file name of the built jar file
	 */
	public String build(final boolean includeClasses, final boolean deleteOnExit) {
		FileOutputStream fos = null;
		JarOutputStream jos = null;
		try {
			final File file = File.createTempFile(name, ".jar");
			fos = new FileOutputStream(file, false);
			jos = new JarOutputStream(fos);
			jos.putNextEntry(new JarEntry("META-INF/"));			
			jos.putNextEntry(new JarEntry("META-INF/services/"));
			jos.putNextEntry(new JarEntry("META-INF/services/org.foo.plugin.XYZ"));
			jos.write("impl1\n".getBytes());
			jos.write("impl2\n".getBytes());
			jos.write("impl3\n".getBytes());			
			jos.closeEntry();
			jos.flush();
			jos.close();
			if(deleteOnExit) file.deleteOnExit();
			return file.getAbsolutePath();
		} catch (Exception ex) {
			throw new RuntimeException("Failed to build plugin jar [" + name + "]", ex);
		} finally {
			if(jos!=null) try { jos.close(); } catch (Exception x) {/* No Op */}
			if(fos!=null) try { fos.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	private PluginJarBuilder(final String name) {
		this.name = name;
	}

}
