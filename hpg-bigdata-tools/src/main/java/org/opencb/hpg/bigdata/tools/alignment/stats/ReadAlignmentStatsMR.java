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

package org.opencb.hpg.bigdata.tools.alignment.stats;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStatsCalculator;
import org.opencb.biodata.tools.alignment.stats.AvroAlignmentGlobalStatsCalculator;
import org.opencb.hpg.bigdata.tools.sequence.stats.ReadAlignmentStatsWritable;
import org.opencb.hpg.bigdata.tools.sequence.stats.ReadStatsWritable;

import java.io.IOException;

public class ReadAlignmentStatsMR {

    public static class ReadAlignmentStatsMapper extends
            Mapper<AvroKey<ReadAlignment>, NullWritable, LongWritable, ReadAlignmentStatsWritable> {

        private int newKey;
        private int numRecords;
        private final int MAX_NUM_AVRO_RECORDS = 1000;

        public void setup(Context context) {
            newKey = 0;
            numRecords = 0;
        }

        @Override
        public void map(AvroKey<ReadAlignment> key, NullWritable value, Context context) throws
                IOException, InterruptedException {
            if (key.datum() == null) {
                return;
            }
            AlignmentGlobalStats stats = new AvroAlignmentGlobalStatsCalculator().compute(key.datum());
            context.write(new LongWritable(newKey), new ReadAlignmentStatsWritable(stats));

            // count records and update new key
            numRecords++;
            if (numRecords >= MAX_NUM_AVRO_RECORDS) {
                newKey++;
                numRecords = 0;
            }
        }
    }

    public static class ReadAlignmentStatsCombiner extends
            Reducer<LongWritable, ReadAlignmentStatsWritable, LongWritable, ReadAlignmentStatsWritable> {

        public void reduce(LongWritable key, Iterable<ReadAlignmentStatsWritable> values, Context context) throws
                IOException, InterruptedException {
            AlignmentGlobalStats stats = new AlignmentGlobalStats();
            AlignmentGlobalStatsCalculator calculator = new AvroAlignmentGlobalStatsCalculator();
            for (ReadAlignmentStatsWritable value : values) {
                calculator.update(value.getStats(), stats);
            }
            context.write(new LongWritable(1), new ReadAlignmentStatsWritable(stats));
        }
    }

    public static class ReadAlignmentStatsReducer extends
            Reducer<LongWritable, ReadAlignmentStatsWritable, Text, NullWritable> {

        public void reduce(LongWritable key, Iterable<ReadAlignmentStatsWritable> values, Context context) throws
                IOException, InterruptedException {
            AlignmentGlobalStats stats = new AlignmentGlobalStats();
            AlignmentGlobalStatsCalculator calculator = new AvroAlignmentGlobalStatsCalculator();
            for (ReadAlignmentStatsWritable value : values) {
                calculator.update(value.getStats(), stats);
            }
            context.write(new Text(stats.toJSON()), NullWritable.get());
        }
    }

    public static int run(String input, String output) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "ReadAlignmentStatsMR");
        job.setJarByClass(ReadAlignmentStatsMR.class);

        // input
        AvroJob.setInputKeySchema(job, ReadAlignment.getClassSchema());
        FileInputFormat.setInputPaths(job, new Path(input));
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // output
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputKeyClass(ReadStatsWritable.class);
        job.setOutputValueClass(NullWritable.class);

        // mapper
        job.setMapperClass(ReadAlignmentStatsMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(ReadAlignmentStatsWritable.class);

        // combiner
        job.setCombinerClass(ReadAlignmentStatsCombiner.class);

        // reducer
        job.setReducerClass(ReadAlignmentStatsReducer.class);
        job.setNumReduceTasks(1);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
