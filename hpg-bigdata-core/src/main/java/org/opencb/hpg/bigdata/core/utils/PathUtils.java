/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.hpg.bigdata.core.utils;

public class PathUtils {

	private final static String FILE_URI = "file://";
	private final static String HDFS_URI = "hdfs://";
	
	public static String clean(String input) {
		if (input.startsWith(FILE_URI)) {
			return input.substring(FILE_URI.length());
		}
		if (input.startsWith(HDFS_URI)) {
			return input.substring(HDFS_URI.length());
		}
		return input;
	}

	public static boolean isHdfs(String input) {
		if (input.startsWith(HDFS_URI)) {
			return true;
		}
		return false;
	}

}
