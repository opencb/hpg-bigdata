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

package org.opencb.hpg.bigdata.analysis.io.parquet;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.opencb.hpg.bigdata.core.utils.CompressionUtils;

/**
 * Created by hpccoll1 on 05/05/15.
 */
public class ParquetMR {

    private final Schema schema;

    public ParquetMR(Schema schema) {
        this.schema = schema;
    }

    public int run(String input, String output, String codecName) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "ParquetMR");
        job.setJarByClass(this.getClass());

        // point to input data
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, schema);

        // set the output format
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, new Path(output));
        AvroParquetOutputFormat.setSchema(job, schema);
        AvroParquetOutputFormat.setCompression(job, CompressionUtils.getParquetCodec(codecName));
        AvroParquetOutputFormat.setCompressOutput(job, true);

        // set a large block size to ensure a single row group
        AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

        job.setMapperClass(ParquetMapper.class);
        job.setNumReduceTasks(0);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
