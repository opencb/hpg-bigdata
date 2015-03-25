package org.opencb.hpg.bigdata.core.utils.files;

import java.io.IOException;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencb.ga4gh.models.Read;
import org.opencb.hpg.bigdata.core.hadoopbam.FastqInputFormat;
import org.opencb.hpg.bigdata.core.hadoopbam.SequencedFragment;
import org.opencb.hpg.bigdata.core.io.Avro2ParquetMapper;

import parquet.avro.AvroParquetOutputFormat;
import parquet.format.CompressionCodec;
import parquet.hadoop.metadata.CompressionCodecName;

public class Fastq2GaMR {
	
	public static class Fastq2GaMapper extends Mapper<Text, SequencedFragment, Text, SequencedFragment> {
		@Override
		public void map(Text key, SequencedFragment value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class Fastq2GaReducer extends Reducer<Text, SequencedFragment, AvroKey<Read>, NullWritable> {

		public void reduce(Text key, Iterable<SequencedFragment> values, Context context) throws IOException, InterruptedException {
			for (SequencedFragment value : values) {
				Read read = new Read(key.toString(), value.getSequence().toString(), value.getQuality().toString());
				context.write(new AvroKey<Read>(read), NullWritable.get());
			}
		}
	}
	
	public static int convert(String input, String output, String codecName) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Fastq2GaMR");		
		job.setJarByClass(Fastq2GaMR.class);

		// We call setOutputSchema first so we can override the configuration
		// parameters it sets
		AvroJob.setOutputKeySchema(job, Read.getClassSchema());
		job.setOutputValueClass(NullWritable.class);
				
		// point to input data
		FileInputFormat.setInputPaths(job, new Path(input));
		job.setInputFormatClass(FastqInputFormat.class);
		
		// set the output format
		FileOutputFormat.setOutputPath(job, new Path(output));
		if (codecName != null) {
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, CompressionUtils.getHadoopCodec(codecName));
		}
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SequencedFragment.class);
		
		
/*		
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setOutputPath(job, outputPath);
		AvroParquetOutputFormat.setSchema(job, schema);
		AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		AvroParquetOutputFormat.setCompressOutput(job, true);

		// set a large block size to ensure a single row group.  see discussion
		AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);
*/
		
		job.setMapperClass(Fastq2GaMapper.class);
		job.setReducerClass(Fastq2GaReducer.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
