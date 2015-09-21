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

package org.opencb.hpg.bigdata.tools.alignment.stats;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.LineReader;
import htsjdk.samtools.util.StringLineReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

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
import org.opencb.biodata.tools.alignment.tasks.RegionDepth;
import org.opencb.biodata.tools.alignment.tasks.RegionDepthCalculator;
import org.opencb.hpg.bigdata.tools.utils.ChunkKey;
import org.opencb.hpg.bigdata.tools.alignment.RegionDepthWritable;

public class ReadAlignmentDepthMR {

    public static final String OUTPUT_SUMMARY_JSON = "summary.depth.json";

    public static class ReadAlignmentDepthMapper extends
            Mapper<AvroKey<ReadAlignment>, NullWritable, ChunkKey, RegionDepthWritable> {

        @Override
        public void map(AvroKey<ReadAlignment> key, NullWritable value, Context context) throws
                IOException, InterruptedException {
            ReadAlignment ra = key.datum();
            LinearAlignment la = (LinearAlignment) ra.getAlignment();

            ChunkKey newKey;
            RegionDepthWritable newValue;

            if (la == null) {
                newKey = new ChunkKey("*", 0L);
                // unmapped read
                newValue = new RegionDepthWritable(new RegionDepth("*", 0, 0, 0));
                context.write(newKey, newValue);
                return;
            }

            RegionDepthCalculator calculator = new RegionDepthCalculator();
            List<RegionDepth> regions = calculator.computeAsList(ra);

            for (RegionDepth region: regions) {
                newKey = new ChunkKey(region.chrom, region.chunk);
                newValue = new RegionDepthWritable(region);
                context.write(newKey, newValue);
            }
        }
    }

    public static class ReadAlignmentDepthCombiner extends
            Reducer<ChunkKey, RegionDepthWritable, ChunkKey, RegionDepthWritable> {

        @Override
        public void reduce(ChunkKey key, Iterable<RegionDepthWritable> values, Context context) throws
                IOException, InterruptedException {
            RegionDepth regionDepth;
            if (key.getName().equals("*")) {
                regionDepth = new RegionDepth("*", 0, 0, 0);
            } else {
                regionDepth = new RegionDepth(key.getName(), key.getChunk() * RegionDepth.CHUNK_SIZE, key.getChunk(),
                        RegionDepth.CHUNK_SIZE);
                RegionDepthCalculator calculator = new RegionDepthCalculator();
                for (RegionDepthWritable value : values) {
                    calculator.updateChunk(value.getRegionDepth(), key.getChunk(), regionDepth);
                }
            }
            context.write(key, new RegionDepthWritable(regionDepth));
        }
    }

    public static class ReadAlignmentDepthReducer extends Reducer<ChunkKey, RegionDepthWritable, Text, NullWritable> {

        private HashMap<String, Long> chromAccDepth = null;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            chromAccDepth = new HashMap<>();
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            double accLen = 0, accDep = 0;

            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path outPath = new Path(context.getConfiguration().get(OUTPUT_SUMMARY_JSON));
            FSDataOutputStream out = fs.create(outPath);
            out.writeChars("{ \"chroms\": [");
            int size = chromAccDepth.size();
            int i = 0;
            for (String name : chromAccDepth.keySet()) {
                out.writeChars("{\"name\": \"" + name + "\", \"length\": "
                        + context.getConfiguration().get(name) + ", \"acc\": "
                        + chromAccDepth.get(name) + ", \"depth\": "
                        + (1.0f * chromAccDepth.get(name) / Integer.parseInt(context.getConfiguration().get(name)))
                        + "}");

                if (++i < size) {
                    out.writeChars(", ");
                }
                accLen += Integer.parseInt(context.getConfiguration().get(name));
                accDep += chromAccDepth.get(name);
            }

            out.writeChars("], \"depth\": " + (accDep / accLen));
            out.writeChars("}");
            out.close();
        }

        @Override
        public void reduce(ChunkKey key, Iterable<RegionDepthWritable> values, Context context) throws
                IOException, InterruptedException {
            if (context.getConfiguration().get(key.getName()) == null) {
                System.out.println("Skip unknown key (name, chunk) = (" + key.getName() + ", " + key.getChunk() + ")");
                return;
            }

            RegionDepth regionDepth = new RegionDepth(key.getName(), key.getChunk() * RegionDepth.CHUNK_SIZE,
                    key.getChunk(), RegionDepth.CHUNK_SIZE);
            RegionDepthCalculator calculator = new RegionDepthCalculator();
            for (RegionDepthWritable value : values) {
                calculator.updateChunk(value.getRegionDepth(), key.getChunk(), regionDepth);
            }

            // accumulator to compute chromosome depth (further processing in cleanup)
            long acc = 0;
            for (int i = 0; i < RegionDepth.CHUNK_SIZE; i++) {
                acc += regionDepth.array[i];
            }
            chromAccDepth.put(key.getName(), (chromAccDepth.get(key.getName()) == null
                    ? acc
                    : acc + chromAccDepth.get(key.getName())));

            context.write(new Text(regionDepth.toFormat()), NullWritable.get());
        }
    }

    public static int run(String input, String output) throws Exception {
        return run(input, output, new Configuration());
    }

    public static int run(String input, String output, Configuration conf) throws Exception {
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

        conf.set(OUTPUT_SUMMARY_JSON, output + ".summary.json");

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

        // combiner
        job.setCombinerClass(ReadAlignmentDepthCombiner.class);

        // reducer
        job.setReducerClass(ReadAlignmentDepthReducer.class);
        job.setNumReduceTasks(1);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
