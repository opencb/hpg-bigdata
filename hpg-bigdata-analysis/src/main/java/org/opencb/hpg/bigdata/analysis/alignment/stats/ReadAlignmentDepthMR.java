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

package org.opencb.hpg.bigdata.analysis.alignment.stats;

//TODO: fix using the new RegionCoverage, JT

@Deprecated
public class ReadAlignmentDepthMR {

//    public static final String OUTPUT_SUMMARY_JSON = "summary.depth.json";
//    public static final String REGIONS_PARAM = "regions";
//    public static final String MIN_MAPQ_PARAM = "min_mapq";
//
//    public static class ReadAlignmentDepthMapper extends
//            Mapper<AvroKey<ReadAlignment>, NullWritable, ChunkKey, RegionCoverageWritable> {
//
//        private RegionFilter regionFilter = null;
//        private int minMapQ = 0;
//
//        @Override
//        public void setup(Mapper.Context context) throws IOException, InterruptedException {
//            String regs = context.getConfiguration().get(REGIONS_PARAM);
//
//            if (regs != null) {
//                System.err.println(">>>>>>> mapper, regs = " + regs);
//                regionFilter = new RegionFilter(regs);
//            }
//            minMapQ = context.getConfiguration().getInt(MIN_MAPQ_PARAM, 0);
//        }
//
//        @Override
//        public void map(AvroKey<ReadAlignment> key, NullWritable value, Context context) throws
//                IOException, InterruptedException {
//            ReadAlignment ra = key.datum();
//            LinearAlignment linearAlignment = (LinearAlignment) ra.getAlignment();
//
//            ChunkKey newKey;
//            RegionCoverageWritable newValue;
//
//            if (linearAlignment == null) {
//                //newKey = new ChunkKey("*", 0L);
//                // unmapped read
//                //newValue = new RegionCoverageWritable(new RegionDepth("*", 0, 0, 0));
//                //context.write(newKey, newValue);
//                return;
//            }
//
//            RegionCoverageCalculator calculator = new AvroRegionCoverageCalculator();
//
//            //TODO: fix using the new RegionCoverage, JT
//            String chrom = linearAlignment.getPosition().getReferenceName().toString();
//            int start = linearAlignment.getPosition().getPosition().intValue();
//            int end = start + calculator.computeSizeByCigar(linearAlignment.getCigar()) - 1;
//
//            //if ((regionFilter == null) || regionFilter.apply(chrom, start, end)) {
//            if ((minMapQ < linearAlignment.getMappingQuality()) && ((regionFilter == null) || regionFilter.apply(chrom, start, end))) {
//                List<RegionCoverage> regions = calculator.computeAsList(ra);
//
//                for (RegionCoverage region: regions) {
//                    newKey = new ChunkKey(region.chrom, region.chunk);
//                    newValue = new RegionCoverageWritable(region);
//                    context.write(newKey, newValue);
//                }
//            }
//        }
//    }
//
//    public static class ReadAlignmentDepthCombiner extends
//            Reducer<ChunkKey, RegionCoverageWritable, ChunkKey, RegionCoverageWritable> {
//
//        @Override
//        public void reduce(ChunkKey key, Iterable<RegionCoverageWritable> values, Context context) throws
//                IOException, InterruptedException {
//            RegionCoverage regionDepth;
//            //TODO: fix using the new RegionCoverage, JT
//            //if (key.getName().equals("*")) {
//            //    regionDepth = new RegionDepth("*", 0, 0, 0);
//            //} else {
//                regionDepth = new RegionCoverage(key.getName(), key.getChunk() * RegionCoverage.CHUNK_SIZE, key.getChunk(),
//                        RegionDepth.CHUNK_SIZE);
//                RegionCoverageCalculator calculator = new RegionCoverageCalculator();
//                for (RegionCoverageWritable value : values) {
//                    calculator.updateChunk(value.getRegionDepth(), key.getChunk(), regionDepth);
//                }
            //}
//            context.write(key, new RegionCoverageWritable(regionDepth));
//        }
//    }

//    public static class ReadAlignmentDepthReducer extends Reducer<ChunkKey, RegionCoverageWritable, Text, NullWritable> {
//
//        private RegionFilter regionFilter = null;
//        private HashMap<String, Long> chromAccDepth = null;
//
//        @Override
//        public void setup(Context context) throws IOException, InterruptedException {
//            String regs = context.getConfiguration().get(REGIONS_PARAM);
//
//            if (regs != null) {
//                System.err.println(">>>>>>> reducer, regs = " + regs);
//                regionFilter = new RegionFilter(regs);
//            }
//
//            chromAccDepth = new HashMap<>();
//        }
//
//        @Override
//        public void cleanup(Context context) throws IOException, InterruptedException {
//            double accLen = 0, accDep = 0;
//
//            FileSystem fs = FileSystem.get(context.getConfiguration());
//            Path outPath = new Path(context.getConfiguration().get(OUTPUT_SUMMARY_JSON));
//            FSDataOutputStream out = fs.create(outPath);
//            out.writeChars("{ \"chroms\": [");
//            int size = chromAccDepth.size();
//            int i = 0;
//            for (String name : chromAccDepth.keySet()) {
//                out.writeChars("{\"name\": \"" + name + "\", \"length\": "
//                        + context.getConfiguration().get(name) + ", \"acc\": "
//                        + chromAccDepth.get(name) + ", \"depth\": "
//                        + (1.0f * chromAccDepth.get(name) / Integer.parseInt(context.getConfiguration().get(name)))
//                        + "}");
//
//                if (++i < size) {
//                    out.writeChars(", ");
//                }
//                accLen += Integer.parseInt(context.getConfiguration().get(name));
//                accDep += chromAccDepth.get(name);
//            }
//
//            out.writeChars("], \"depth\": " + (accDep / accLen));
//            out.writeChars("}");
//            out.close();
//        }

//        @Override
//        public void reduce(ChunkKey key, Iterable<RegionCoverageWritable> values, Context context) throws
//                IOException, InterruptedException {
//            if (context.getConfiguration().get(key.getName()) == null) {
//                System.out.println("Skip unknown key (name, chunk) = (" + key.getName() + ", " + key.getChunk() + ")");
//                return;
//            }
//
//            RegionDepth regionDepth = new RegionDepth(key.getName(), key.getChunk() * RegionDepth.CHUNK_SIZE,
//                    key.getChunk(), RegionDepth.CHUNK_SIZE);
//            RegionCoverageCalculator calculator = new RegionCoverageCalculator();
//            for (RegionCoverageWritable value : values) {
//                calculator.updateChunk(value.getRegionDepth(), key.getChunk(), regionDepth);
//            }
//
//            // accumulator to compute chromosome depth (further processing in cleanup)
//            long acc = 0;
//            for (int i = 0; i < RegionDepth.CHUNK_SIZE; i++) {
//                acc += regionDepth.array[i];
//            }
//            chromAccDepth.put(key.getName(), (chromAccDepth.get(key.getName()) == null
//                    ? acc
//                    : acc + chromAccDepth.get(key.getName())));
//
//            if (regionFilter == null) {
//                context.write(new Text(regionDepth.toFormat()), NullWritable.get());
//            } else {
//                context.write(new Text(regionDepth.toFormat(regionFilter)), NullWritable.get());
//            }
//        }
//    }
//
//    public static int run(String input, String output, String regions, int minMapQ) throws Exception {
//        return run(input, output, regions, minMapQ, new Configuration());
//    }
//
//    public static int run(String input, String output, String regions, int minMapQ, Configuration conf) throws Exception {
//        // read header, and save sequence name/length in config
//        byte[] data = null;
//        Path headerPath = new Path(input + ".header");
//        FileSystem hdfs = FileSystem.get(conf);
//        FSDataInputStream dis = hdfs.open(headerPath);
//        FileStatus status = hdfs.getFileStatus(headerPath);
//        data = new byte[(int) status.getLen()];
//        dis.read(data, 0, (int) status.getLen());
//        dis.close();
//
//        String textHeader = new String(data);
//        LineReader lineReader = new StringLineReader(textHeader);
//        SAMFileHeader header = new SAMTextHeaderCodec().decode(lineReader, textHeader);
//        int i = 0;
//        SAMSequenceRecord sr;
//        while ((sr = header.getSequence(i++)) != null) {
//            conf.setInt(sr.getSequenceName(), sr.getSequenceLength());
//        }

//        // filters
//        conf.set(OUTPUT_SUMMARY_JSON, output + "summary.json");
//        if (regions != null) {
//            conf.set(REGIONS_PARAM, regions);
//        }
//        conf.set(MIN_MAPQ_PARAM, "" + minMapQ);

//        Job job = Job.getInstance(conf, "ReadAlignmentDepthMR");
//        job.setJarByClass(ReadAlignmentDepthMR.class);
//
//        // input
//        AvroJob.setInputKeySchema(job, ReadAlignment.SCHEMA$);
//        FileInputFormat.setInputPaths(job, new Path(input));
//        job.setInputFormatClass(AvroKeyInputFormat.class);
//
//        // output
//        FileOutputFormat.setOutputPath(job, new Path(output));
//        job.setOutputKeyClass(RegionCoverageWritable.class);
//        job.setOutputValueClass(NullWritable.class);
//
//        // mapper
////        job.setMapperClass(ReadAlignmentDepthMapper.class);
//        job.setMapOutputKeyClass(ChunkKey.class);
//        job.setMapOutputValueClass(RegionCoverageWritable.class);
//
//        // combiner
//        job.setCombinerClass(ReadAlignmentDepthCombiner.class);
//
//        // reducer
//        job.setReducerClass(ReadAlignmentDepthReducer.class);
//        job.setNumReduceTasks(1);
//
//        return (job.waitForCompletion(true) ? 0 : 1);
//    }
}
