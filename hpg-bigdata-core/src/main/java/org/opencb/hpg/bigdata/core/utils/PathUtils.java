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
