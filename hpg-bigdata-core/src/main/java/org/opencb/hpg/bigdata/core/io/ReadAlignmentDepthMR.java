package org.opencb.hpg.bigdata.core.io;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.LineReader;
import htsjdk.samtools.util.StringLineReader;

import java.io.IOException;
import java.util.ArrayList;

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

public class ReadAlignmentDepthMR {

	//public static SAMFileHeader header;
	
	public static class ReadAlignmentStatsMapper extends Mapper<AvroKey<ReadAlignment>, NullWritable, ReadAlignmentKey, ReadAlignmentStatsWritable> {

		public  void setup(Context context) {
		}

		@Override
		public void map(AvroKey<ReadAlignment> key, NullWritable value, Context context) throws IOException, InterruptedException {
			ReadAlignmentKey newKey;
			LinearAlignment la = (LinearAlignment) key.datum().getAlignment();
			if (la == null) {
				newKey = new ReadAlignmentKey(new String("unmapped"), (long) 0);
			} else {
				newKey = new ReadAlignmentKey(la.getPosition().getReferenceName().toString(), la.getPosition().getPosition()); 
				//newKey = new Text(la.getPosition().getReferenceName().toString());
			}

			System.out.println("map : " + newKey.toString());

			ReadAlignmentStatsWritable stats = new ReadAlignmentStatsWritable();
			stats.updateByReadAlignment(key.datum());
			context.write(newKey, stats);
		}
	}

	public static class ReadAlignmentStatsReducer extends Reducer<ReadAlignmentKey, ReadAlignmentStatsWritable, Text, Text> { //NullWritable> {

		ReadAlignmentStatsWritable finalStats;
		ArrayList<ReadAlignmentStats> pending = new ArrayList<ReadAlignmentStats>();
		final int chunkSize = 4000;
		long start = 0;
		long end = start + chunkSize - 1;
		byte tmpCov[] = new byte[chunkSize];
		long accCov;
		
		public void setup(Context context) throws IOException, InterruptedException {
			finalStats = new ReadAlignmentStatsWritable();
			context.write(new Text("{"), new Text(""));			
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("\"summary\": "), new Text(finalStats.toJSON() + " }"));
		}

		public void reduce(ReadAlignmentKey key, Iterable<ReadAlignmentStatsWritable> values, Context context) throws IOException, InterruptedException {
			System.out.println("reduce : " + key.toString());
/*			
			ReadAlignmentStatsWritable stats = new ReadAlignmentStatsWritable();
			for (ReadAlignmentStatsWritable value : values) {
				finalStats.update(value);
				//stats.update(value);
				//value.
				if (value.pos > end) {
					// processes pending and updates start/end
				} else {
					if (value.pos < start) {
						long refPos = value.pos;
						long tmpPos = refPos - start;
						System.out.println("Error: 'start' at " + start + " and 'pos' at " + value.pos + ". 'pos' must be greater than 'start'");
						for (CigarUnit cu: value.cigar) {
							switch (cu.getOperation()) {
							case ALIGNMENT_MATCH:
								break;
							case CLIP_HARD:
							case CLIP_SOFT:
							case PAD:
							case SKIP:
							case DELETE:
							case INSERT:
								break;
							case SEQUENCE_MATCH:
								break;
							case SEQUENCE_MISMATCH:
								break;
							default:
								break;
							}
						}
					}
				}
			}
			context.write(new Text("\"name\": "), new Text("\"value\", "));
			//context.write(new Text(stats.toJSON()), NullWritable.get());
			 * 
			 */
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
		
		Job job = Job.getInstance(conf, "ReadAlignmentStatsMR");		
		job.setJarByClass(ReadAlignmentDepthMR.class);

		// input
		AvroJob.setInputKeySchema(job, ReadAlignment.getClassSchema());
		FileInputFormat.setInputPaths(job, new Path(input));
		job.setInputFormatClass(AvroKeyInputFormat.class);

		// output
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputKeyClass(ReadStatsWritable.class);
		//job.setOutputValueClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// mapper
		job.setMapperClass(ReadAlignmentStatsMapper.class);
		job.setMapOutputKeyClass(ReadAlignmentKey.class);
		job.setMapOutputValueClass(ReadAlignmentStatsWritable.class);

		// reducer
		job.setReducerClass(ReadAlignmentStatsReducer.class);
		job.setNumReduceTasks(1);

		job.waitForCompletion(true);
		//System.out.println("Output in " + output);
		//System.exit(-1);
		//return 0;
		 
		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
