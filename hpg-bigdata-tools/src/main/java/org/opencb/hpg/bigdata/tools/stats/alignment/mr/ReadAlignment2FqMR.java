package org.opencb.hpg.bigdata.tools.stats.alignment.mr;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.sequence.Read;
import org.opencb.hpg.bigdata.core.utils.ReadAlignmentUtils;
import org.opencb.hpg.bigdata.tools.utils.CompressionUtils;

import java.io.IOException;

public class ReadAlignment2FqMR {

	public static class ReadAlignment2FqMapper extends Mapper<AvroKey<ReadAlignment>, NullWritable, LongWritable, AvroValue<Read>> {

		@Override
		public void map(AvroKey<ReadAlignment> key, NullWritable value, Context context) throws IOException, InterruptedException {
			ReadAlignment ra = key.datum();
			Read read = new Read(ra.getId(), ra.getAlignedSequence(), ReadAlignmentUtils.getQualityString(ra));

			context.write(new LongWritable(1), new AvroValue<>(read));
		}
	}

	public static class ReadAlignment2FqReducer extends Reducer<LongWritable, AvroValue<Read>, AvroKey<Read>, NullWritable> {

		public void reduce(LongWritable key, Iterable<AvroValue<Read>> values, Context context) throws IOException, InterruptedException {
			for (AvroValue<Read> value : values) {
				context.write(new AvroKey<>(value.datum()), NullWritable.get());
			}
		}
	}

	public static int run(String input, String output, String compression) throws Exception {
		return run(input, output, compression, new Configuration());
	}

	public static int run(String input, String output, String compression, Configuration conf) throws Exception {

		Job job = Job.getInstance(conf, "ReadAlignment2FqMR");
		job.setJarByClass(ReadAlignment2FqMR.class);

		// We call setOutputSchema first so we can override the configuration
		// parameters it sets
		AvroJob.setOutputKeySchema(job, Read.SCHEMA$);
		job.setOutputValueClass(NullWritable.class);
		AvroJob.setMapOutputValueSchema(job, Read.SCHEMA$);

		// point to input data
		FileInputFormat.setInputPaths(job, new Path(input));
		job.setInputFormatClass(AvroKeyInputFormat.class);

		// set the output format
		FileOutputFormat.setOutputPath(job, new Path(output));
		if (compression != null) {
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, CompressionUtils.getHadoopCodec(compression));
		}
		job.setOutputFormatClass(AvroKeyOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(AvroValue.class);

		job.setMapperClass(ReadAlignment2FqMapper.class);
		job.setReducerClass(ReadAlignment2FqReducer.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
