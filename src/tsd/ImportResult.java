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

package net.opentsdb.tsd;

import java.util.EnumMap;
import java.util.Map;

import org.hbase.async.jsr166e.LongAdder;



/**
 * <p>Title: ImportResult</p>
 * <p>Description: Enum and formatter of import results</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.ImportResult</code></p>
 */
public enum ImportResult {
	TOTAL,
	COMPLETE,
	ERRORS,
	ELAPSED,
	ALLOCATED;
	
	private ImportResult() {
		name = name().toLowerCase();
	}
	final String name;
	
	private static final ImportResult[] values = values();
	
	private static final String errTemplate = "{\"total\":-1, \"complete\":-1, \"errors\":1, \"elapsed\":-1, \"allocated\":-1, \"cause\":\"%s\"}";
	
	public static EnumMap<ImportResult, LongAdder> resultsMap() {
		final EnumMap<ImportResult, LongAdder> map = new EnumMap<ImportResult, LongAdder>(ImportResult.class);
		for(final ImportResult ir: values) {
			map.put(ir, new LongAdder());
		}
		return map;
	}
	
	public static String format(final Throwable t) {
		return String.format(errTemplate, t.toString());
	}
	
	public static String format(final Map<ImportResult, LongAdder> resultsMap) {
		final StringBuilder b = new StringBuilder(120);
		b.append("{");
		for(Map.Entry<ImportResult, LongAdder> entry: resultsMap.entrySet()) {
			b.append("\"").append(entry.getKey().name).append("\":").append(entry.getValue().sum()).append(",");
		}
		b.deleteCharAt(b.length()-1);
		b.append("}");
		return b.toString();
	}
	
}