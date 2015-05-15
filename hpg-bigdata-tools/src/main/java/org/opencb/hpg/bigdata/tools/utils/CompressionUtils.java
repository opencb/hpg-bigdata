package org.opencb.hpg.bigdata.tools.utils;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import parquet.hadoop.metadata.CompressionCodecName;

/**
 * Created by hpccoll1 on 15/05/15.
 */
public class CompressionUtils {

    public static Class<? extends org.apache.hadoop.io.compress.CompressionCodec> getHadoopCodec(String name) {
        if (name == null) {
            return null;
        } else if (name.equalsIgnoreCase("gzip")) {
            return GzipCodec.class;
        } else if (name.equalsIgnoreCase("snappy")) {
            return SnappyCodec.class;
        } else if (name.equalsIgnoreCase("bzip2")) {
            return BZip2Codec.class;
        }

        return DeflateCodec.class;
    }

    public static CompressionCodecName getParquetCodec(String name) {
        if (name == null) {
            return CompressionCodecName.UNCOMPRESSED;
        } else if (name.equalsIgnoreCase("gzip")) {
            return CompressionCodecName.GZIP;
        } else if (name.equalsIgnoreCase("lzo")) {
            return CompressionCodecName.LZO;
        }

        return CompressionCodecName.SNAPPY;
    }
}
