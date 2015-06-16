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
import org.opencb.biodata.tools.sequence.tasks.SequenceKmers;
import org.opencb.biodata.tools.sequence.tasks.SequenceKmersCalculator;
import org.opencb.hpg.bigdata.tools.io.ReadKmersWritable;

public class ReadKmersMR {

	public static class ReadKmersMapper extends Mapper<AvroKey<Read>, NullWritable, LongWritable, ReadKmersWritable> {

		int kvalue = 0;
		int newKey;
		int numRecords;
		final int MAX_NUM_AVRO_RECORDS = 1000;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			kvalue = Integer.parseInt(conf.get("kvalue"));
			System.out.println("setup mapper: kvalue = " + kvalue);
			newKey = 0;
			numRecords = 0;
		}

		@Override
		public void map(AvroKey<Read> key, NullWritable value, Context context) throws IOException, InterruptedException {
			SequenceKmers kmers = new SequenceKmersCalculator().compute(key.datum(), kvalue);
			context.write(new LongWritable(newKey), new ReadKmersWritable(kmers));

			// count records and update new key
			numRecords++;
			if (numRecords >= MAX_NUM_AVRO_RECORDS) {
				newKey++;
				numRecords = 0;
			}
		}
	}

	public static class ReadKmersCombiner extends Reducer<LongWritable, ReadKmersWritable, LongWritable, ReadKmersWritable> {

		int kvalue = 0;

		public void setup(Mapper.Context context) {
			Configuration conf = context.getConfiguration();
			kvalue = Integer.parseInt(conf.get("kvalue"));
			System.out.println("setup combiner: kvalue = " + kvalue);
		}

		@Override
		public void reduce(LongWritable key, Iterable<ReadKmersWritable> values, Context context) throws IOException, InterruptedException {
			SequenceKmers kmers = new SequenceKmers(kvalue);
			SequenceKmersCalculator calculator = new SequenceKmersCalculator();
			for (ReadKmersWritable value : values) {
				calculator.update(value.getKmers(), kmers);
			}
			context.write(new LongWritable(1), new ReadKmersWritable(kmers));
		}
	}

	public static class ReadKmersReducer extends Reducer<LongWritable, ReadKmersWritable, Text, NullWritable> {

		int kvalue = 0;

		public void setup(Mapper.Context context) {
			Configuration conf = context.getConfiguration();
			kvalue = Integer.parseInt(conf.get("kvalue"));
			System.out.println("setup reducer: kvalue = " + kvalue);
		}

		@Override
		public void reduce(LongWritable key, Iterable<ReadKmersWritable> values, Context context) throws IOException, InterruptedException {
			SequenceKmers kmers = new SequenceKmers(kvalue);
			System.out.println("00 kvalue = " + kvalue + ", " + kmers.getKvalue());
			SequenceKmersCalculator calculator = new SequenceKmersCalculator();
			for (ReadKmersWritable value : values) {
				calculator.update(value.getKmers(), kmers);
			}
			context.write(new Text(kmers.toJSON()), NullWritable.get());
			System.out.println("11 kvalue = " + kvalue + ", " + kmers.getKvalue());
		}
	}

	public static int run(String input, String output, int kvalue) throws Exception {
		Configuration conf = new Configuration();
		conf.set("kvalue", String.valueOf(kvalue));
		System.out.println("run: kvalue = " + kvalue);

		Job job = Job.getInstance(conf, "ReadKmersMR");
		job.setJarByClass(ReadKmersMR.class);

		// input
		AvroJob.setInputKeySchema(job, Read.getClassSchema());
		FileInputFormat.setInputPaths(job, new Path(input));
		job.setInputFormatClass(AvroKeyInputFormat.class);

		// output
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputKeyClass(ReadKmersWritable.class);
		job.setOutputValueClass(NullWritable.class);

		// mapper
		job.setMapperClass(ReadKmersMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(ReadKmersWritable.class);

		// combiner
		job.setCombinerClass(ReadKmersCombiner.class);

		// reducer
		job.setReducerClass(ReadKmersReducer.class);
		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
