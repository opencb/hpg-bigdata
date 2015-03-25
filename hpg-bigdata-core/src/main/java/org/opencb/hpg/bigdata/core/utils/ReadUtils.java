package org.opencb.hpg.bigdata.core.utils;

import org.opencb.ga4gh.models.Read;

public class ReadUtils {

	public static String getFastqString(Read read) {
		return "@" + read.getId().toString() + "\n" + read.getSequence().toString() + "\n+\n" + read.getQuality().toString() + "\n";
	}

}
