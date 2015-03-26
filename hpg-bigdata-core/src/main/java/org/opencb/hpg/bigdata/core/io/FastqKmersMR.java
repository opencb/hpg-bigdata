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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.ga4gh.models.Read;
import org.opencb.hpg.bigdata.core.stats.FastqKmersWritable;

public class FastqKmersMR {
		
	public static class FastqKmersMapper extends Mapper<AvroKey<Read>, NullWritable, LongWritable, FastqKmersWritable> {
		
		private static int kvalue = 0;
		
		public  void setup(Context context) {
			Configuration conf = context.getConfiguration();
			kvalue = Integer.parseInt(conf.get("kvalue"));
		}
		
		@Override
		public void map(AvroKey<Read> key, NullWritable value, Context context) throws IOException, InterruptedException {
			FastqKmersWritable kmers = new FastqKmersWritable();
			kmers.updateByRead(key.datum(), kvalue);
			context.write(new LongWritable(1), kmers);
		}
	}

	public static class FastqKmersReducer extends Reducer<LongWritable, FastqKmersWritable, Text, NullWritable> {
		
		public void reduce(LongWritable key, Iterable<FastqKmersWritable> values, Context context) throws IOException, InterruptedException {
			FastqKmersWritable kmers = new FastqKmersWritable();
			for (FastqKmersWritable value : values) {
				kmers.update(value);
			}
			context.write(new Text(kmers.toFormat()), NullWritable.get());
		}
	}
	
	public static int run(String input, String output, int kvalue) throws Exception {
		Configuration conf = new Configuration();
		conf.set("kvalue", String.valueOf(kvalue));

		Job job = Job.getInstance(conf, "FastqKmersMR");
		job.setJarByClass(FastqKmersMR.class);

		// input
		AvroJob.setInputKeySchema(job, Read.getClassSchema());
		FileInputFormat.setInputPaths(job, new Path(input));
		job.setInputFormatClass(AvroKeyInputFormat.class);
				
		// output
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputKeyClass(FastqKmersWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		// mapper
		job.setMapperClass(FastqKmersMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(FastqKmersWritable.class);
		
		// reducer
		job.setReducerClass(FastqKmersReducer.class);
		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
