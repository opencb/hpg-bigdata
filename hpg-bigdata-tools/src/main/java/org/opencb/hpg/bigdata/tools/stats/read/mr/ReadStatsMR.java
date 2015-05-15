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
import org.opencb.hpg.bigdata.core.models.Read;
import org.opencb.hpg.bigdata.tools.io.ReadStatsWritable;

public class ReadStatsMR {
	
	public static class ReadStatsMapper extends Mapper<AvroKey<Read>, NullWritable, LongWritable, ReadStatsWritable> {
		
		private static int kvalue = 0;
		
		public  void setup(Context context) {
			Configuration conf = context.getConfiguration();
			kvalue = Integer.parseInt(conf.get("kvalue"));
		}
		
		@Override
		public void map(AvroKey<Read> key, NullWritable value, Context context) throws IOException, InterruptedException {
			ReadStatsWritable stats = new ReadStatsWritable();
			stats.kmers.kvalue = kvalue;
			stats.updateByRead(key.datum());
			context.write(new LongWritable(1), stats);
		}
	}

	public static class ReadStatsReducer extends Reducer<LongWritable, ReadStatsWritable, Text, NullWritable> {

		private static int kvalue = 0;
		
		public  void setup(Context context) {
			Configuration conf = context.getConfiguration();
			kvalue = Integer.parseInt(conf.get("kvalue"));
		}

		public void reduce(LongWritable key, Iterable<ReadStatsWritable> values, Context context) throws IOException, InterruptedException {
			ReadStatsWritable stats = new ReadStatsWritable();
			stats.kmers.kvalue = kvalue;
			for (ReadStatsWritable value : values) {
				stats.update(value);
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
		AvroJob.setInputKeySchema(job, Read.getClassSchema());
		FileInputFormat.setInputPaths(job, new Path(input));
		job.setInputFormatClass(AvroKeyInputFormat.class);
				
		// output
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputKeyClass(ReadStatsWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		// mapper
		job.setMapperClass(ReadStatsMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(ReadStatsWritable.class);
		
		// reducer
		job.setReducerClass(ReadStatsReducer.class);
		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
