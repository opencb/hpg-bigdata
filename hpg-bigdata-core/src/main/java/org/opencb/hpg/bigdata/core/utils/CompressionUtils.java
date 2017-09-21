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

import org.apache.avro.file.CodecFactory;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;


public final class CompressionUtils {


    public static Class<? extends org.apache.hadoop.io.compress.CompressionCodec> getHadoopCodec(String name) {
        if (name == null || name.equalsIgnoreCase("null")) {
            return null;
        }

        Class<? extends org.apache.hadoop.io.compress.CompressionCodec> codecClass;
        switch (name.toLowerCase()) {
            case "gzip":
                codecClass = GzipCodec.class;
                break;
            case "snappy":
                codecClass = SnappyCodec.class;
                break;
            case "bzip2":
                codecClass = BZip2Codec.class;
                break;
            default:
                codecClass = DeflateCodec.class;
                break;
        }

        return codecClass;
    }

    public static CodecFactory getAvroCodec(String name) {
        if (name == null || name.equalsIgnoreCase("null")) {
            return CodecFactory.nullCodec();
        }

        CodecFactory codecFactory;
        switch (name.toLowerCase()) {
            case "snappy":
                codecFactory = CodecFactory.snappyCodec();
                break;
            case "gzip":
            case "deflate":
                codecFactory = CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
                break;
            case "bzip2":
                codecFactory = CodecFactory.bzip2Codec();
                break;
            case "xz":
                codecFactory = CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL);
                break;
            default:
                codecFactory = CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
                break;
        }
        return codecFactory;
    }

    public static CompressionCodecName getParquetCodec(String name) {
        if (name == null || name.equalsIgnoreCase("null")) {
            return CompressionCodecName.UNCOMPRESSED;
        }

        CompressionCodecName compressionCodecName;
        switch (name.toLowerCase()) {
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
