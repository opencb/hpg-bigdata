package org.opencb.hpg.bigdata.core.io;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.LineReader;
import htsjdk.samtools.util.StringLineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.ga4gh.models.LinearAlignment;
import org.ga4gh.models.ReadAlignment;
import org.opencb.hpg.bigdata.core.stats.ReadAlignmentStats;
import org.opencb.hpg.bigdata.core.stats.ReadAlignmentStatsWritable;
import org.opencb.hpg.bigdata.core.stats.ReadStatsWritable;
import org.opencb.hpg.bigdata.core.stats.RegionDepthWritable;

public class ReadAlignmentDepthMR {

	public final static int CHUNK_SIZE = 4000;
	
	public static class ReadAlignmentDepthMapper extends Mapper<AvroKey<ReadAlignment>, NullWritable, ReadAlignmentKey, RegionDepthWritable> {

		@Override
		public void map(AvroKey<ReadAlignment> key, NullWritable value, Context context) throws IOException, InterruptedException {
			ReadAlignment ra = (ReadAlignment) key.datum();
			LinearAlignment la = (LinearAlignment) ra.getAlignment();

			ReadAlignmentKey newKey;
			RegionDepthWritable newValue;
			
			if (la == null) {
				newKey = new ReadAlignmentKey(new String("unmapped"), (long) 0);
				newValue = new RegionDepthWritable("null", 0, 0, 0);
			} else {
				long start_chunk = la.getPosition().getPosition() / CHUNK_SIZE;
				long end_chunk = (la.getPosition().getPosition() + ra.getAlignedSequence().length())  / CHUNK_SIZE;
				if (start_chunk != end_chunk) {
					System.out.println("-----------> chunks (start, end) = (" + start_chunk + ", " + end_chunk + ")");
					//System.exit(-1);
				}
				newKey = new ReadAlignmentKey(la.getPosition().getReferenceName().toString(), start_chunk); 
				
				newValue = new RegionDepthWritable(newKey.getChrom(), la.getPosition().getPosition(), start_chunk, ra.getAlignedSequence().length());
				newValue.update(la.getPosition().getPosition(), la.getCigar());
			}

			System.out.println("map : " + newKey.toString() + ", chrom. length = " + context.getConfiguration().get(newKey.getChrom()));

			context.write(newKey, newValue);
		}
	}

	public static class ReadAlignmentDepthReducer extends Reducer<ReadAlignmentKey, RegionDepthWritable, Text, NullWritable> {
	
		RegionDepthWritable pendingDepth = null;
		
		public void setup(Context context) throws IOException, InterruptedException {
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
		}

		public void reduce(ReadAlignmentKey key, Iterable<RegionDepthWritable> values, Context context) throws IOException, InterruptedException {
			System.out.println("reduce : " + key.toString());
			RegionDepthWritable currRegionDepth = new RegionDepthWritable(key.getChrom(), key.getPos() * CHUNK_SIZE, key.getPos(), CHUNK_SIZE);
			for (RegionDepthWritable value: values) {
				if (value.size > 0) {
					currRegionDepth.merge(value);
				}
			}
			if (pendingDepth != null && 
				pendingDepth.chrom == currRegionDepth.chrom && 
				pendingDepth.chunk == currRegionDepth.chunk) {
				// there is a pending RegionDepth to merge
				currRegionDepth.merge(pendingDepth);
				pendingDepth = null;
			}
			
			if (currRegionDepth.size > CHUNK_SIZE) {
				// we must split the current RegionDepth and create a pending RegionDepth
				pendingDepth = new RegionDepthWritable(currRegionDepth.chrom, currRegionDepth.position + CHUNK_SIZE,
													   currRegionDepth.chunk + 1, currRegionDepth.size - CHUNK_SIZE);
				pendingDepth.array = Arrays.copyOfRange(currRegionDepth.array, CHUNK_SIZE, currRegionDepth.size - 1);
			}
			context.write(new Text(currRegionDepth.toString(CHUNK_SIZE)), NullWritable.get());
		}
	}

	public static int run(String input, String output) throws Exception {
		Configuration conf = new Configuration();

		{
			// read header, and save sequence name/length in config 
			byte[] data = null;
			Path headerPath = new Path(input + ".header");
			FileSystem hdfs = FileSystem.get(conf);
			FSDataInputStream dis = hdfs.open(headerPath);
			FileStatus status = hdfs.getFileStatus(headerPath);
			data = new byte[(int) status.getLen()];
			dis.read(data, 0, (int) status.getLen());
			dis.close();
			
			String textHeader = new String(data);
			LineReader lineReader = new StringLineReader(textHeader);
			SAMFileHeader header = new SAMTextHeaderCodec().decode(lineReader, textHeader);
			int i = 0;
			SAMSequenceRecord sr;
			while ((sr = header.getSequence(i++)) != null) {
				conf.setInt(sr.getSequenceName(), sr.getSequenceLength());
			}
		}
		
		Job job = Job.getInstance(conf, "ReadAlignmentDepthMR");		
		job.setJarByClass(ReadAlignmentDepthMR.class);

		// input
		AvroJob.setInputKeySchema(job, ReadAlignment.SCHEMA$);
		FileInputFormat.setInputPaths(job, new Path(input));
		job.setInputFormatClass(AvroKeyInputFormat.class);

		// output
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputKeyClass(RegionDepthWritable.class);
		job.setOutputValueClass(NullWritable.class);

		// mapper
		job.setMapperClass(ReadAlignmentDepthMapper.class);
		job.setMapOutputKeyClass(ReadAlignmentKey.class);
		job.setMapOutputValueClass(RegionDepthWritable.class);

		// reducer
		job.setReducerClass(ReadAlignmentDepthReducer.class);
		job.setNumReduceTasks(1);

		job.waitForCompletion(true);
		//System.out.println("Output in " + output);
		//System.exit(-1);
		return 0;
		 
		//return (job.waitForCompletion(true) ? 0 : 1);
	}
}
