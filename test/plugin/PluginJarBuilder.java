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
import java.io.InputStream;
import java.nio.charset.Charset;
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
	
	/** The UTF8 Character Set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
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
			impls.add(clazz);
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
	
	
	/**
	 * Example of building a plugin jar
	 * @param args None
	 */
	public static void main(String[] args) {
		System.out.println(
			newBuilder("foo")
			.addPlugin("net.opentsdb.tsd.DummyHttpRpcPlugin")
			.build(true, false)
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
		final Set<String> directoryEntries = new HashSet<String>();
		try {
			final File file = File.createTempFile(name, ".jar");
			if(deleteOnExit) file.deleteOnExit(); 
			fos = new FileOutputStream(file, false);
			jos = new JarOutputStream(fos);
			jos.putNextEntry(new JarEntry("META-INF/"));			
			jos.putNextEntry(new JarEntry("META-INF/services/"));
			for(Map.Entry<String, Set<Class<?>>> entry: services.entrySet()) {
				jos.putNextEntry(new JarEntry("META-INF/services/" + entry.getKey()));
				for(Class<?> clazz: entry.getValue()) {
					jos.write((clazz.getName() + "\n").getBytes(UTF8));
				}
				jos.closeEntry();
				if(includeClasses) {
					for(Class<?> clazz: entry.getValue()) {
						final String packageDir = clazz.getPackage().getName().replace('.', '/') + "/";
						if(directoryEntries.add(packageDir)) {
							jos.putNextEntry(new JarEntry(packageDir));
						}
						final String resourceName = packageDir + clazz.getSimpleName() + ".class";
						jos.putNextEntry(new JarEntry(resourceName));
						writeClass(jos, clazz, resourceName);
						jos.closeEntry();
					}
				}
			}
			jos.flush();
			jos.close();
			
			return file.getAbsolutePath();
		} catch (Exception ex) {
			throw new RuntimeException("Failed to build plugin jar [" + name + "]", ex);
		} finally {
			if(jos!=null) try { jos.close(); } catch (Exception x) {/* No Op */}
			if(fos!=null) try { fos.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	protected void writeClass(final JarOutputStream jos, final Class<?> clazz, final String resource) {
		final ClassLoader cl = clazz.getClassLoader();
		InputStream is = cl.getResourceAsStream(resource);
		try {
			final byte[] byteCode = new byte[1024];
			int bytesRead = -1;
			while((bytesRead = is.read(byteCode))!=-1) {
				jos.write(byteCode, 0, bytesRead);
			}
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read class [" + resource + "]", ex);
		} finally {
			try { is.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	private PluginJarBuilder(final String name) {
		this.name = name;
	}

}
