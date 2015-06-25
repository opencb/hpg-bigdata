package org.opencb.hpg.bigdata.core.utils;

import org.opencb.commons.utils.CryptoUtils;

/**
 * Created by mh719 on 16/06/15.
 */
public class HBaseUtils {
    public static final int SV_THRESHOLD = 50; // TODO update as needed
    public final static String ROWKEY_SEPARATOR = "_";

    public static String buildRefernceStorageId(CharSequence chr,Long start, CharSequence refBases) {
    	return buildStorageId(chr, start, refBases, refBases);
    }

    public static String buildStorageId(CharSequence chr,Long start, CharSequence refBases, CharSequence altBases) {
        StringBuilder builder = new StringBuilder();

        builder.append(buildStoragePosition(chr,start));

        builder.append(ROWKEY_SEPARATOR);

        if (refBases.length() < SV_THRESHOLD) {
            builder.append(refBases);
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(refBases.toString())));
        }

        builder.append(ROWKEY_SEPARATOR);

        if (altBases.length() < SV_THRESHOLD) {
            builder.append(altBases);
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(altBases.toString())));
        }

        return builder.toString();
    }

    public static String buildStoragePosition(CharSequence chr,Long pos) {
        String chrom = chr.toString();
        // check for chr at chromosome name and remove it (maybe expect it to be done before.
        if (chrom.length() > 2) {
            if (chrom.substring(0, 2).equals("chr")) {
                chrom = chrom.substring(2);
            }
        }
        if (chrom.length() < 2) {
            chrom = "0" + chrom;
        }

        StringBuilder builder = new StringBuilder();
        builder.append(chrom);
        builder.append(ROWKEY_SEPARATOR);
        builder.append(String.format("%012d", pos));
        return builder.toString();
    }
}
