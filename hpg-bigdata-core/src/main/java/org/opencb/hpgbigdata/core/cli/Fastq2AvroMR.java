package org.opencb.hpgbigdata.core.cli;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ga4gh.models.Read;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;

public class Fastq2AvroMR extends Configured implements Tool {

	public static class HF2HAMapper extends Mapper<Text, SequencedFragment, Text, SequencedFragment> {
		@Override
		public void map(Text key, SequencedFragment value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class HF2HAReducer extends Reducer<Text, SequencedFragment, AvroKey<Read>, NullWritable> {

		public void reduce(Text key, Iterable<SequencedFragment> values, Context context) throws IOException, InterruptedException {
			for (SequencedFragment value : values) {
				Read read = new Read(key.toString(), value.getSequence().toString(), value.getQuality().toString());
				context.write(new AvroKey<Read>(read), NullWritable.get());
			}
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJobName("fastq-2-avro");
		job.setJarByClass(Fastq2AvroMR.class);

		// We call setOutputSchema first so we can override the configuration
		// parameters it sets
		AvroJob.setOutputKeySchema(job, Read.getClassSchema());
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(HF2HAMapper.class);
		job.setReducerClass(HF2HAReducer.class);

		job.setInputFormatClass(FastqInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SequencedFragment.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Fastq2AvroMR(), args);
		System.exit(res);
	}
}