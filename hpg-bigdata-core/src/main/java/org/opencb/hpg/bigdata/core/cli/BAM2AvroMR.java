package org.opencb.hpg.bigdata.core.cli;

import htsjdk.samtools.SAMRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.ga4gh.models.Read;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.io.IOException;

public class BAM2AvroMR extends Configured implements Tool {

	public static class HB2HAMapper extends Mapper<LongWritable, SAMRecordWritable, LongWritable, SAMRecordWritable> {
		@Override
		public void map(LongWritable key, SAMRecordWritable value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class HB2HAReducer extends Reducer<LongWritable, SAMRecordWritable, AvroKey<Read>, NullWritable> {

		public void reduce(LongWritable key, Iterable<SAMRecordWritable> values, Context context) throws IOException, InterruptedException {
			for (SAMRecordWritable value : values) {
				final SAMRecord sam = value.get();
				Read read = new Read(sam.getReadName(), sam.getReadString(), sam.getBaseQualityString());
				context.write(new AvroKey<Read>(read), NullWritable.get());
			}
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJobName("bam-2-avro");
		job.setJarByClass(BAM2AvroMR.class);

		// We call setOutputSchema first so we can override the configuration
		// parameters it sets
		AvroJob.setOutputKeySchema(job, Read.getClassSchema());
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(HB2HAMapper.class);
		job.setReducerClass(HB2HAReducer.class);

		job.setInputFormatClass(AnySAMInputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(SAMRecordWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BAM2AvroMR(), args);
		System.exit(res);
	}
}