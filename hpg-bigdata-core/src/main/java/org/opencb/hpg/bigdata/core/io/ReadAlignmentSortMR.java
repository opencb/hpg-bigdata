package org.opencb.hpg.bigdata.core.io;

import java.io.IOException;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.ga4gh.models.LinearAlignment;
import org.ga4gh.models.ReadAlignment;

public class ReadAlignmentSortMR {
	
	public static class ReadAlignmentSortMapper extends Mapper<AvroKey<ReadAlignment>, NullWritable, ReadAlignmentKey, AvroValue<ReadAlignment>> {

		public void map(AvroKey<ReadAlignment> key, NullWritable value, Context context) throws IOException, InterruptedException {
			ReadAlignmentKey newKey;
			LinearAlignment la = (LinearAlignment) key.datum().getAlignment();
			if (la == null) {
				newKey = new ReadAlignmentKey(new String("unmapped"), (long) 0);
			} else {
				newKey = new ReadAlignmentKey(la.getPosition().getReferenceName().toString(), la.getPosition().getPosition()); 
			}
			context.write(newKey, new AvroValue<ReadAlignment>(key.datum()));
		}
	}

	public static class ReadAlignmentSortReducer extends Reducer<ReadAlignmentKey, AvroValue<ReadAlignment>, AvroKey<ReadAlignment>, NullWritable> {

		public void reduce(ReadAlignmentKey key, Iterable<AvroValue<ReadAlignment>> values, Context context) throws IOException, InterruptedException {
			for (AvroValue<ReadAlignment> value : values) {
				context.write(new AvroKey<ReadAlignment>(value.datum()), NullWritable.get());
			}
		}
	}

	public static int run(String input, String output) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "ReadAlignmentSortMR");		
		job.setJarByClass(ReadAlignmentSortMR.class);

		// input
		AvroJob.setInputKeySchema(job, ReadAlignment.SCHEMA$);
		FileInputFormat.setInputPaths(job, new Path(input));
		job.setInputFormatClass(AvroKeyInputFormat.class);

		// output
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		//job.setOutputKeyClass(AvroKeyInputFormat.class);
		//job.setOutputValueClass(NullWritable.class);

		// mapper
		job.setMapperClass(ReadAlignmentSortMapper.class);
		//AvroJob.setMapOutputKeySchema(job, ReadAlignment.SCHEMA$);
		AvroJob.setMapOutputValueSchema(job, ReadAlignment.SCHEMA$);
		job.setMapOutputKeyClass(ReadAlignmentKey.class);
		//job.setMapOutputValueClass(AvroKeyInputFormat.class);

		// reducer
		job.setReducerClass(ReadAlignmentSortReducer.class);
		AvroJob.setOutputKeySchema(job, ReadAlignment.SCHEMA$);
/*
		System.out.println("mapreduce.map.output.compress = " + conf.get("mapreduce.map.output.compress"));
		System.out.println("mapreduce.output.fileoutputformat.compress = " + conf.get("mapreduce.output.fileoutputformat.compress"));
		System.out.println("mapred.map.output.compression.codec = " + conf.get("mapred.map.output.compression.codec"));
		System.out.println("AvroJob.CONF_OUTPUT_CODEC = " + AvroJob.CONF_OUTPUT_CODEC + " -> " + conf.get(AvroJob.CONF_OUTPUT_CODEC));
		System.exit(-1);
*/
		// Compress Map output
		conf.set("mapred.compress.map.output","true");
		conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");

		// Compress MapReduce output
		conf.set("mapred.output.compress","true");
		conf.set("mapred.output.compression","org.apache.hadoop.io.compress.SnappyCodec");
		
		//conf.setBoolean("mapreduce.map.output.compress", true);
		//conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
		//conf.setBoolean("mapred.compress.map.output", true);
		//conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
		//mapred.map.output.compression.codec
		
		//conf.set(AvroJob.CONF_OUTPUT_CODEC, CodecFactory.snappyCodec().toString());

//		System.out.println("mapreduce.map.output.compress = " + conf.get("mapreduce.map.output.compress"));
//		System.out.println("mapreduce.output.fileoutputformat.compress = " + conf.get("mapreduce.output.fileoutputformat.compress"));
//		conf.set("mapreduce.map.output.compress", true)
//		conf.set("mapreduce.output.fileoutputformat.compress", false)
		
//		conf.setBoolean("mapreduce.output.fileoutputformat.compres", true);
		//conf.setBoolean("mapreduce.output.fileoutputformat.compress", false);
		//System.out.println("AvroJob.CONF_OUTPUT_CODEC = " + AvroJob.CONF_OUTPUT_CODEC + " -> " + conf.get(AvroJob.CONF_OUTPUT_CODEC));
		//System.exit(-1);
		//job.setNumReduceTasks(1);

		//job.waitForCompletion(true);
		//System.out.println("Output in " + output);
		//System.exit(-1);
		//return 0;
		 
		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
