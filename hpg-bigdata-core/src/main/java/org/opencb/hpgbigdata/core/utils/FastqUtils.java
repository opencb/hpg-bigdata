package org.opencb.hpgbigdata.core.utils;

import org.ga4gh.models.Read;

public class FastqUtils {
	public static String format(Read read) {
		return "@" + read.getId().toString() + "\n" + read.getSequence().toString() + "\n+\n" + read.getQuality().toString() + "\n";
	}
}
