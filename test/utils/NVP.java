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
package net.opentsdb.utils;

/**
 * <p>Title: NVP</p>
 * <p>Description: Simple generic name value pair</p> 
 * <p><code>net.opentsdb.utils.NVP</code></p>
 */

public class NVP<K,V> {
	/** The NVP key */
	protected K key;
	/** The NVP value */
	protected V value;

	/**
	 * Creates a new NVP
	 * @param key The NVP key
	 * @param value The NVP value
	 */
	public NVP(final K key, final V value) {
		this.key = key;
		this.value = value;
	}
	
	
	

	/**
	 * Returns the NVP value
	 * @return the value
	 */
	public V getValue() {
		return value;
	}

	/**
	 * Returns the NVP key
	 * @return the key
	 */
	public K getKey() {
		return key;
	}
	
	/**
	 * Returns the key type
	 * @return the key type
	 */
	@SuppressWarnings("unchecked")
	public Class<K> getKeyType() {
		return (Class<K>) (key==null ? null : key.getClass());
	}
	
	/**
	 * Returns the value type
	 * @return the value type
	 */
	@SuppressWarnings("unchecked")
	public Class<V> getValueType() {
		return (Class<V>) (value==null ? null : value.getClass());
	}
	
	
	public String dump() {
		final StringBuilder b = new StringBuilder(getClass().getSimpleName()).append(" [");
		b.append(" key: ");
		if(key!=null) {
			b.append(key.getClass().getName()).append(": (").append(key).append(")");
		}
		else {
			b.append("null");
		}
		b.append(" value: ");
		if(value!=null) {
			b.append(value.getClass().getName()).append(": (").append(value).append(")");
		} else {
			b.append("null");		
		}
		return b.toString();
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		final StringBuilder b = new StringBuilder(" [key:(");
		if(key!=null) {
			b.append(key).append("), ");
		}else {
			b.append("null), ");
		}
		
		b.append("value:(");
		if(value!=null) {
			b.append(value).append(")");
		}
		else {
			b.append("null)");
		}
		b.append("]");
		return b.toString();
	}
	
	
}

