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

package org.opencb.hpg.bigdata.tools.stats.read.mr;

import java.io.IOException;

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
import org.opencb.biodata.models.sequence.Read;
import org.opencb.biodata.tools.sequence.tasks.SequenceStats;
import org.opencb.biodata.tools.sequence.tasks.SequenceStatsCalculator;
import org.opencb.hpg.bigdata.tools.io.ReadStatsWritable;

public class ReadStatsMR {

	public static class ReadStatsMapper extends Mapper<AvroKey<Read>, NullWritable, LongWritable, ReadStatsWritable> {
		
		int newKey;
		int numRecords;
		final int MAX_NUM_AVRO_RECORDS = 1000;

        private static int kvalue = 0;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			kvalue = Integer.parseInt(conf.get("kvalue"));
			newKey = 0;
			numRecords = 0;
        }
		
		@Override
		public void map(AvroKey<Read> key, NullWritable value, Context context) throws IOException, InterruptedException {
            SequenceStats stats = new SequenceStatsCalculator().compute(key.datum(), kvalue);
			context.write(new LongWritable(newKey), new ReadStatsWritable(stats));

			// count records and update new key
			numRecords++;
			if (numRecords >= MAX_NUM_AVRO_RECORDS) {
				newKey++;
				numRecords = 0;
			}
		}
	}

	public static class ReadStatsCombiner extends Reducer<LongWritable, ReadStatsWritable, LongWritable, ReadStatsWritable> {
/*
        // to find out why setup is not called !!!
        private static int kvalue = 0;

        public  void setup(Mapper.Context context) {
            Configuration conf = context.getConfiguration();
            kvalue = Integer.parseInt(conf.get("kvalue"));
            System.out.println("****** =============> combiner setup kvalue = " + kvalue);
        }
*/
        @Override
		public void reduce(LongWritable key, Iterable<ReadStatsWritable> values, Context context) throws IOException, InterruptedException {
            // todo: set kvalue once in the setup function
            int kvalue = Integer.parseInt(context.getConfiguration().get("kvalue"));
            SequenceStats stats = new SequenceStats(kvalue);
            SequenceStatsCalculator calculator = new SequenceStatsCalculator();
			for (ReadStatsWritable value : values) {
                calculator.update(value.getStats(), stats);
			}
			context.write(new LongWritable(1), new ReadStatsWritable(stats));
		}
	}

	public static class ReadStatsReducer extends Reducer<LongWritable, ReadStatsWritable, Text, NullWritable> {
/*
        // to find out why setup is not called !!!
        private static int kvalue = 0;

        public  void setup(Mapper.Context context) {
            Configuration conf = context.getConfiguration();
            kvalue = Integer.parseInt(conf.get("kvalue"));
            System.out.println("****** =============> reducer setup kvalue = " + kvalue);
        }
*/
        @Override
		public void reduce(LongWritable key, Iterable<ReadStatsWritable> values, Context context) throws IOException, InterruptedException {
            // todo: set kvalue once in the setup function
            int kvalue = Integer.parseInt(context.getConfiguration().get("kvalue"));
            SequenceStats stats = new SequenceStats(kvalue);
            SequenceStatsCalculator calculator = new SequenceStatsCalculator();
			for (ReadStatsWritable value : values) {
                calculator.update(value.getStats(), stats);
			}
			context.write(new Text(stats.toJSON()), NullWritable.get());
		}
	}
	
	public static int run(String input, String output, int kvalue) throws Exception {
		Configuration conf = new Configuration();
		conf.set("kvalue", String.valueOf(kvalue));

		Job job = Job.getInstance(conf, "ReadStatsMR");		
		job.setJarByClass(ReadStatsMR.class);

		// input
		FileInputFormat.setInputPaths(job, new Path(input));
		job.setInputFormatClass(AvroKeyInputFormat.class);
		AvroJob.setInputKeySchema(job, Read.SCHEMA$);

		// output
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputKeyClass(ReadStatsWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		// mapper
		job.setMapperClass(ReadStatsMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(ReadStatsWritable.class);

		// combiner
		job.setCombinerClass(ReadStatsCombiner.class);

		// reducer
		job.setReducerClass(ReadStatsReducer.class);
		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
