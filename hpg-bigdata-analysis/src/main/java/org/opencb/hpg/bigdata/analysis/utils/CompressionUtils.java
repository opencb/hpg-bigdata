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

package org.opencb.hpg.bigdata.analysis.utils;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Created by hpccoll1 on 15/05/15.
 */
public final class CompressionUtils {

    private CompressionUtils() {
    }

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
        if (name == null || name.equalsIgnoreCase("null")) {
            return CompressionCodecName.UNCOMPRESSED;
        }
        CompressionCodecName compressionCodecName;
        switch (name) {
            case "snappy":
                compressionCodecName = CompressionCodecName.SNAPPY;
                break;
            case "gzip":
                compressionCodecName = CompressionCodecName.GZIP;
                break;
            case "lzo":
                compressionCodecName = CompressionCodecName.LZO;
                break;
            default:
                compressionCodecName = CompressionCodecName.SNAPPY;
                break;
        }
        return compressionCodecName;
    }
}
