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

package org.opencb.hpg.bigdata.tools.alignment;

import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.ga4gh.models.ReadAlignment;
import org.opencb.hpg.bigdata.tools.utils.CompressionUtils;

@Deprecated
public class ReadAlignment2ParquetMR {

    public static int run(String input, String output, String codecName) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "ReadAlignment2Parquet");
        job.setJarByClass(ReadAlignment2ParquetMR.class);

        // point to input data
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // set the output format
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, new Path(output));
        AvroParquetOutputFormat.setSchema(job, ReadAlignment.SCHEMA$);
        AvroParquetOutputFormat.setCompression(job, CompressionUtils.getParquetCodec(codecName));
        AvroParquetOutputFormat.setCompressOutput(job, true);

        // set a large block size to ensure a single row group
        AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

        job.setMapperClass(ReadAlignment2ParquetMapper.class);
        job.setNumReduceTasks(0);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}

/*
private void avro2ParquetLocal(String input, String output) throws IOException {
    // generate the corresponding parquet schema
    MessageType parquetSchema = new AvroSchemaConverter().convert(ReadAlignment.getClassSchema());

    // create a WriteSupport object to serialize your Avro objects
    AvroWriteSupport writeSupport = new AvroWriteSupport(parquetSchema, ReadAlignment.getClassSchema());

    // set parquet file block size and page size values
    int blockSize = 256 * 1024 * 1024;
    int pageSize = 64 * 1024;


    // the ParquetWriter object that will consume Avro GenericRecords
    ParquetWriter parquetWriter = new AvroParquetWriter(new Path(output),
            ReadAlignment.getClassSchema(), CompressionCodecName.GZIP, blockSize, pageSize);
    //ReadAlignment.getClassSchema(), CompressionCodecName.SNAPPY, blockSize, pageSize);
    // reader
    InputStream is = new FileInputStream(input);
    DataFileStream<ReadAlignment> reader =
            new DataFileStream<ReadAlignment>(is, new SpecificDatumReader<ReadAlignment>(ReadAlignment.class));

    // main loop
    for (ReadAlignment readAlignment: reader) {
        parquetWriter.write(readAlignment);
    }

    parquetWriter.close();
}
*/
