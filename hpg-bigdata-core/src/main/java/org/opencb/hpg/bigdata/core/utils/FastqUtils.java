package org.opencb.hpg.bigdata.core.utils;

import org.opencb.ga4gh.models.Read;

public class FastqUtils {

	public static String format(Read read) {
		return "@" + read.getId().toString() + "\n" + read.getSequence().toString() + "\n+\n" + read.getQuality().toString() + "\n";
	}

}
