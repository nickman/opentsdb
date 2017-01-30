// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
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

/**
 * <p>Title: TSDMode</p>
 * <p>Description: Enumerates the possible TSD read/write modes</p> 
 * <p><code>net.opentsdb.tsd.TSDMode</code></p>
 */

public enum TSDMode {
	RW(true, true),
	WO(true, false),
	RO(false, true);
	
	private TSDMode(final boolean writable, final boolean readable) {
		this.writable = writable;
		this.readable = readable;
	}
	
	/** True if TSD is writable */
	public final boolean writable;
	/** True if TSD is readable */
	public final boolean readable;
	
	/** The default TSDMode (RW) */
	public static final TSDMode DEFAULT_MODE = TSDMode.RW;
	
	/**
	 * Decodes the passed string to a TSDMode, handling trim and upper case
	 * @param mode The mode to decode
	 * @return The decoded mode
	 */
	public static TSDMode decode(final String mode) {
		if(mode==null || mode.trim().isEmpty()) 
			throw new IllegalArgumentException("Invalid TSDMode [" + mode + "]");
		try {
			return TSDMode.valueOf(mode.trim().toUpperCase());
		} catch (Exception ex) {
			throw new IllegalArgumentException("Invalid TSDMode [" + mode + "]");
		}
	}
}
