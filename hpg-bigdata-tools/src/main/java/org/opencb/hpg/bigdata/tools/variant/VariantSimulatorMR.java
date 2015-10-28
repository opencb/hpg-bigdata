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

package org.opencb.hpg.bigdata.tools.variant;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.simulator.VariantSimulator;
import org.opencb.biodata.tools.variant.simulator.VariantSimulatorConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

//import org.apache.avro.mapred.AvroValue;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by kalyanreddyemani on 06/10/15.
 * @author pawan
 * @author kalyan
 */

public class VariantSimulatorMR extends Configured implements Tool{
    //private static Random rand = new Random();
    private static List<Region> regionsList;
    private static Map<String, Double> genotypeProbabilites;
    private Configuration conf = null;

    private VariantSimulator variantSimulator;
    private int numVariants;
    private int chunkSize;

    private VariantSimulatorConfiguration variantSimulatorConfiguration;

    public VariantSimulatorMR() {
    }

    public VariantSimulatorMR(VariantSimulatorConfiguration variantSimulatorConfiguration) {
        this.variantSimulatorConfiguration = variantSimulatorConfiguration;
    }

    public class SimulatorAvroMapper extends Mapper<LongWritable, Text, AvroKey<VariantAvro>, NullWritable> {

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valStr = value.toString().split("\t");
            List<Region> regionList = Arrays.asList(new Region(valStr[0], Integer.parseInt(valStr[1]), Integer.parseInt(valStr[2])));
            int numVariants = Integer.parseInt(valStr[3]);

            variantSimulator = new VariantSimulator(variantSimulatorConfiguration);
            List<Variant> variants = variantSimulator.simulate(numVariants, variantSimulatorConfiguration.getNumSamples(), regionList);

            for (Variant variant : variants) {
                context.write(new AvroKey<>(variant.getImpl()), NullWritable.get());
            }

//            int position = 0;
//            if (valStr[1] != null || !valStr[1].isEmpty()) {
//                position = Integer.parseInt(valStr[1].trim());
//            }
//
//            int start = position;
//            int end = position;
//            variant.setStart(start);
//            variant.setEnd(end);
//            VariantAvro variantAvro = variant.getImpl();
//            context.write(new AvroKey<>(variantAvro), NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: Data Gen " + "<inputPath>  <OutputPath>  <regions>  <genotypeProbabilites>");
            return -1;
        }

        String output = args[0];
        String regions = args[1];
        String genProb = args[2];

        conf = new Configuration();
//        conf.setInt(NLineInputFormat.LINES_PER_MAP, 500000);

        List<Region> regionList = variantSimulatorConfiguration.getRegions();
        long totalGenomeSize = 0;
        for (Region region : regionList) {
            totalGenomeSize += region.getEnd();
        }

        int numVariantsPerChunk = Math.round(numVariants / (totalGenomeSize / chunkSize));

        // create the input file for Hadoop MR
        PrintWriter printWriter = new PrintWriter(new File("/tmp/aaa"));
        for (Region region : regionList) {
            int start = 1;
            int end = chunkSize;
            int numChunks = region.getEnd() / chunkSize;
            for (int i = 0; i < numChunks; i++) {
                printWriter.println(region.getChromosome() + "\t" + start + "\t" + end + "\t" + numVariantsPerChunk);
                start += chunkSize;
                end += chunkSize;
            }
        }
        printWriter.close();

        Job job = Job.getInstance(conf, "Simulator");
        job.setJarByClass(VariantSimulatorMR.class);
        AvroJob.setMapOutputKeySchema(job, VariantAvro.getClassSchema());
        AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());

//        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

//        job.getConfiguration().set("regions", regions);
//        job.getConfiguration().set("genotypeProbabilites", genProb);

        job.setInputFormatClass(NLineInputFormat.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setMapperClass(SimulatorAvroMapper.class);
        return (job.waitForCompletion(true) ? 0 : 1);
    }


    public int getNumVariants() {
        return numVariants;
    }

    public void setNumVariants(int numVariants) {
        this.numVariants = numVariants;
    }


    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

}
