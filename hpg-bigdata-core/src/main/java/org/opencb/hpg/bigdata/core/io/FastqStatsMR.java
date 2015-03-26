package org.opencb.hpg.bigdata.core.io;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.ga4gh.models.Read;
import org.opencb.hpg.bigdata.core.stats.FastqStatsWritable;

public class FastqStatsMR {
	
	public static class FastqStatsMapper extends Mapper<AvroKey<Read>, NullWritable, LongWritable, FastqStatsWritable> {
		
		private static int kvalue = 0;
		
		public  void setup(Context context) {
			Configuration conf = context.getConfiguration();
			kvalue = Integer.parseInt(conf.get("kvalue"));
		}
		
		@Override
		public void map(AvroKey<Read> key, NullWritable value, Context context) throws IOException, InterruptedException {
			FastqStatsWritable stats = new FastqStatsWritable();
			stats.kmers.kvalue = kvalue;
			stats.updateByRead(key.datum());
			context.write(new LongWritable(1), stats);
		}
	}

	public static class FastqStatsReducer extends Reducer<LongWritable, FastqStatsWritable, Text, NullWritable> {

		private static int kvalue = 0;
		
		public  void setup(Context context) {
			Configuration conf = context.getConfiguration();
			kvalue = Integer.parseInt(conf.get("kvalue"));
		}

		public void reduce(LongWritable key, Iterable<FastqStatsWritable> values, Context context) throws IOException, InterruptedException {
			FastqStatsWritable stats = new FastqStatsWritable();
			stats.kmers.kvalue = kvalue;
			for (FastqStatsWritable value : values) {
				stats.update(value);
			}
			context.write(new Text(stats.toFormat()), NullWritable.get());
		}
	}
	
	public static int run(String input, String output, int kvalue) throws Exception {
		Configuration conf = new Configuration();
		conf.set("kvalue", String.valueOf(kvalue));

		Job job = Job.getInstance(conf, "FastqStatsMR");		
		job.setJarByClass(FastqStatsMR.class);

		// input
		AvroJob.setInputKeySchema(job, Read.getClassSchema());
		FileInputFormat.setInputPaths(job, new Path(input));
		job.setInputFormatClass(AvroKeyInputFormat.class);
				
		// output
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputKeyClass(FastqStatsWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		// mapper
		job.setMapperClass(FastqStatsMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(FastqStatsWritable.class);
		
		// reducer
		job.setReducerClass(FastqStatsReducer.class);
		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
