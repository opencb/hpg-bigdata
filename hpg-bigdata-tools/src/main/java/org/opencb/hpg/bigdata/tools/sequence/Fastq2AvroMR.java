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

package org.opencb.hpg.bigdata.tools.sequence;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

import org.opencb.biodata.models.sequence.Read;
import org.opencb.hpg.bigdata.tools.utils.CompressionUtils;
import org.seqdoop.hadoop_bam.SequencedFragment;

public class Fastq2AvroMR {

    public static class Fastq2GaMapper extends Mapper<Text, SequencedFragment, LongWritable, AvroValue<Read>> {

        public void map(Text key, SequencedFragment value, Context context) throws IOException, InterruptedException {
            Read read = new Read(key.toString(), value.getSequence().toString(), value.getQuality().toString());
            context.write(new LongWritable(1), new AvroValue<>(read));
        }
    }

    public static class Fastq2GaReducer extends Reducer<LongWritable, AvroValue<Read>, AvroKey<Read>, NullWritable> {

        public void reduce(LongWritable key, Iterable<AvroValue<Read>> values, Context context)
                throws IOException, InterruptedException {
            for (AvroValue<Read> value : values) {
                context.write(new AvroKey<>(value.datum()), NullWritable.get());
            }
        }
    }

    public static int run(String input, String output, String codecName) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Fastq2AvroMR");
        job.setJarByClass(Fastq2AvroMR.class);

        // We call setOutputSchema first so we can override the configuration
        // parameters it sets
        AvroJob.setOutputKeySchema(job, Read.SCHEMA$);
        job.setOutputValueClass(NullWritable.class);
        AvroJob.setMapOutputValueSchema(job, Read.SCHEMA$);

        // point to input data
        FileInputFormat.setInputPaths(job, new Path(input));
        job.setInputFormatClass(FastqInputFormatMODIF.class);

        // set the output format
        FileOutputFormat.setOutputPath(job, new Path(output));
        if (codecName != null) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, CompressionUtils.getHadoopCodec(codecName));
        }
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(AvroValue.class);

        job.setMapperClass(Fastq2GaMapper.class);
        job.setReducerClass(Fastq2GaReducer.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
