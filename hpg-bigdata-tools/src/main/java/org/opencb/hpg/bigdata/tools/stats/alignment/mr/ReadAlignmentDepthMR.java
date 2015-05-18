package org.opencb.hpg.bigdata.tools.stats.alignment.mr;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.LineReader;
import htsjdk.samtools.util.StringLineReader;

import java.io.IOException;
import java.util.HashMap;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.opencb.hpg.bigdata.tools.converters.mr.ChunkKey;
import org.opencb.hpg.bigdata.tools.io.RegionDepthWritable;

public class ReadAlignmentDepthMR {

	public static class ReadAlignmentDepthMapper extends Mapper<AvroKey<ReadAlignment>, NullWritable, ChunkKey, RegionDepthWritable> {

		@Override
		public void map(AvroKey<ReadAlignment> key, NullWritable value, Context context) throws IOException, InterruptedException {
			ReadAlignment ra = (ReadAlignment) key.datum();
			LinearAlignment la = (LinearAlignment) ra.getAlignment();

			ChunkKey newKey;
			RegionDepthWritable newValue;

			if (la == null) {
				newKey = new ChunkKey(new String("*"), (long) 0);
				newValue = new RegionDepthWritable("null", 0, 0, 0);
			} else {
				long start_chunk = la.getPosition().getPosition() / RegionDepthWritable.CHUNK_SIZE;
				long end_chunk = (la.getPosition().getPosition() + ra.getAlignedSequence().length())  / RegionDepthWritable.CHUNK_SIZE;
				if (start_chunk != end_chunk) {
					//System.out.println("-----------> chunks (start, end) = (" + start_chunk + ", " + end_chunk + ")");
					//System.exit(-1);
				}
				newKey = new ChunkKey(la.getPosition().getReferenceName().toString(), start_chunk);

				newValue = new RegionDepthWritable(newKey.getName(), la.getPosition().getPosition(), start_chunk, ra.getAlignedSequence().length());
				newValue.update(la.getPosition().getPosition(), la.getCigar());

				//System.out.println("map : " + newKey.toString() + ", chrom. length = " + context.getConfiguration().get(newKey.getName()));
			}
			context.write(newKey, newValue);
		}
	}

	public static class ReadAlignmentDepthReducer extends Reducer<ChunkKey, RegionDepthWritable, Text, NullWritable> {

		public HashMap<String, HashMap<Long, RegionDepthWritable>> regions = null;
		public HashMap<String, Long> accDepth = null;

		public void setup(Context context) throws IOException, InterruptedException {
			regions = new HashMap<String, HashMap<Long, RegionDepthWritable>>();
			accDepth = new HashMap<String, Long>();
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			double accLen = 0, accDep = 0;
				
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataOutputStream out = fs.create(new Path(context.getConfiguration().get("summary.depth.json")));
			out.writeChars("{ \"chroms\": [");
			int size = accDepth.size();
			int i = 0;
			for(String name : accDepth.keySet()) {
				out.writeChars("{\"name\": \"" + name + "\", \"length\": " + context.getConfiguration().get(name) + ", \"acc\": " + accDepth.get(name) + ", \"depth\": " + (1.0f * accDepth.get(name) / Integer.parseInt(context.getConfiguration().get(name))) + "}");
				if (++i < size ) {
					out.writeChars(", ");
				}
				//out.writeChars(name + "\t" + context.getConfiguration().get(name) + "\t" + accDepth.get(name) + "\t" + (1.0f * accDepth.get(name) / Integer.parseInt(context.getConfiguration().get(name))) + "\n");
				//System.out.println("name : " + name + ", length : " + context.getConfiguration().get(name) + ", accDepth = " + accDepth.get(name) + ", depth = " + (1.0f * accDepth.get(name) / Integer.parseInt(context.getConfiguration().get(name))));
				accLen += Integer.parseInt(context.getConfiguration().get(name));
				accDep += accDepth.get(name);
			}
			out.writeChars("], \"depth\": " + (accDep / accLen));
			out.writeChars("}");
			out.close();

			//System.out.println("Depth = " + (accDep / accLen));
		}

		public void reduce(ChunkKey key, Iterable<RegionDepthWritable> values, Context context) throws IOException, InterruptedException {
			//System.out.println("reduce : " + key.toString());
			if (context.getConfiguration().get(key.getName()) == null) {
				System.out.println("skipping unknown key (name, chunk) = (" + key.getName() + ", " + key.getChunk() + ")");
				return;
			}

			int size = RegionDepthWritable.CHUNK_SIZE;
			if (Integer.parseInt(context.getConfiguration().get(key.getName())) / size == key.getChunk()) {
				size = Integer.parseInt(context.getConfiguration().get(key.getName())) % size;
			}
			RegionDepthWritable currRegionDepth = new RegionDepthWritable(key.getName(), key.getChunk() * RegionDepthWritable.CHUNK_SIZE, key.getChunk(), size);

			long chunk;
			RegionDepthWritable pending = null;
			HashMap<Long, RegionDepthWritable> map = null;

			for (RegionDepthWritable value: values) {
				if (value.size > 0) {

					currRegionDepth.merge(value);

					if (value.size > RegionDepthWritable.CHUNK_SIZE) {
						// we must split the current RegionDepth and add a pending RegionDepth
						long endChunk = (value.position + value.size) / RegionDepthWritable.CHUNK_SIZE;
						for (chunk = value.chunk + 1 ; chunk < endChunk ; chunk++) {
							if ((map = regions.get(currRegionDepth.chrom)) == null) {
								// no pending regions for this chrom, create it
								map = new HashMap<Long, RegionDepthWritable> ();
								regions.put(currRegionDepth.chrom, map);
							}
							if ((pending = map.get(chunk)) == null) {
								// there are not pending regions on this chunk
								pending = new RegionDepthWritable(key.getName(), chunk * RegionDepthWritable.CHUNK_SIZE, chunk, RegionDepthWritable.CHUNK_SIZE);
								map.put(chunk, pending);
							}
							pending.mergeChunk(value, chunk);
						}
					}
				}
			}

			// if there are pending regions, then merge them into the current region
			chunk = currRegionDepth.chunk;
			if ((map = regions.get(currRegionDepth.chrom)) != null) {
				if ((pending = map.get(chunk)) != null) {
					currRegionDepth.merge(pending);
					map.remove(chunk);
				}
			}

			long acc = 0;
			for (int i = 0; i < size; i++) {
				acc += currRegionDepth.array[i];
			}
			accDepth.put(key.getName(), (accDepth.get(key.getName()) == null ? acc : acc + accDepth.get(key.getName())));
			//System.out.println("name = " + key.getName() + " chunk = " + key.getChunk() + " -> acc. depth = " + accDepth.get(key.getName()) + ", lengh = " + context.getConfiguration().get(key.getName()));
			context.write(new Text(currRegionDepth.toString()), NullWritable.get());
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

		conf.set("summary.depth.json", output + ".summary.depth.json");
		
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
		job.setMapOutputKeyClass(ChunkKey.class);
		job.setMapOutputValueClass(RegionDepthWritable.class);

		// reducer
		job.setReducerClass(ReadAlignmentDepthReducer.class);
		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
