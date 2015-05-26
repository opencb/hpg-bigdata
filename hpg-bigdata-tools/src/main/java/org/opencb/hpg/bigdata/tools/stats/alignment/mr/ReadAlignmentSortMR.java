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

package org.opencb.hpg.bigdata.tools.stats.alignment.mr;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.ga4gh.models.LinearAlignment;
import org.ga4gh.models.ReadAlignment;
import org.opencb.hpg.bigdata.tools.converters.mr.ChunkKey;

public class ReadAlignmentSortMR {
	
	public static class ReadAlignmentSortMapper extends Mapper<AvroKey<ReadAlignment>, NullWritable, ChunkKey, AvroValue<ReadAlignment>> {

		public void map(AvroKey<ReadAlignment> key, NullWritable value, Context context) throws IOException, InterruptedException {
			ChunkKey newKey;
			LinearAlignment la = (LinearAlignment) key.datum().getAlignment();
			if (la == null) {
				newKey = new ChunkKey(new String("*"), (long) 0);
			} else {
				newKey = new ChunkKey(la.getPosition().getReferenceName().toString(), la.getPosition().getPosition());
			}
			context.write(newKey, new AvroValue<ReadAlignment>(key.datum()));
		}
	}

	public static class ReadAlignmentSortReducer extends Reducer<ChunkKey, AvroValue<ReadAlignment>, AvroKey<ReadAlignment>, NullWritable> {

		public void reduce(ChunkKey key, Iterable<AvroValue<ReadAlignment>> values, Context context) throws IOException, InterruptedException {
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
		job.setMapOutputKeyClass(ChunkKey.class);
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

		job.waitForCompletion(true);

		// copy header to the output folder
		FileSystem fs = FileSystem.get(conf);

		// read header
		Path srcHeaderPath = new Path(input + ".header");
		FSDataInputStream dis = fs.open(srcHeaderPath);
		FileStatus status = fs.getFileStatus(srcHeaderPath);
		byte[] data = new byte[(int) status.getLen()];
		dis.read(data, 0, (int) status.getLen());
		dis.close();

		// copy header
		OutputStream os = fs.create(new Path(output + "/part-r-00000.avro.header"));
		os.write(data);
		os.close();
		
		return 0;
	}
}
