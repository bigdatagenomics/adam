// Copyright (C) 2011-2012 CRS4.
//
// This file is part of Hadoop-BAM.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

package fi.tkk.ics.hadoop.bam.util;

import org.apache.hadoop.conf.Configuration;

public class ConfHelper
{
	/**
	 * Convert a string to a boolean.
	 *
	 * Accepted values: "yes", "true", "t", "y", "1"
	 *                  "no", "false", "f", "n", "0" 
	 * All comparisons are case insensitive.
	 *
	 * If the value provided is null, defaultValue is returned.
	 *
	 * @exception IllegalArgumentException Thrown if value is not
	 * null and doesn't match any of the accepted strings.
	 */
	public static boolean parseBoolean(String value, boolean defaultValue)
	{
		if (value == null)
			return defaultValue;

		value = value.trim();

		// any of the following will 
		final String[] acceptedTrue = new String[]{ "yes", "true", "t", "y", "1" };
		final String[] acceptedFalse = new String[]{ "no", "false", "f", "n", "0" };

		for (String possible: acceptedTrue)
		{
			if (possible.equalsIgnoreCase(value))
				return true;
		}
		for (String possible: acceptedFalse)
		{
			if (possible.equalsIgnoreCase(value))
				return false;
		}

		throw new IllegalArgumentException("Unrecognized boolean value '" + value + "'");
	}

	public static boolean parseBoolean(Configuration conf, String propertyName, boolean defaultValue)
	{
		return parseBoolean(conf.get(propertyName), defaultValue);
	}
}
