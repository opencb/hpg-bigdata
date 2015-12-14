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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.simulator.VariantSimulator;
import org.opencb.biodata.tools.variant.simulator.VariantSimulatorConfiguration;


/**
 * Created by kalyanreddyemani on 06/10/15.
 * @author pawan
 * @author kalyan
 */

public class VariantSimulatorMR extends Configured implements Tool{
    //private static Random rand = new Random();
    //    private static List<Region> regionsList;
    //    private static Map<String, Double> genotypeProbabilites;
    private Configuration conf = null;

    private static VariantSimulator variantSimulator;
    private static int numVariants;
    private int chunkSize;
    private List<Region> regionList;

    private static VariantSimulatorConfiguration variantSimulatorConfiguration;

    public VariantSimulatorMR() {
    }

    public VariantSimulatorMR(VariantSimulatorConfiguration variantSimulatorConfiguration) {
        VariantSimulatorMR.variantSimulatorConfiguration = variantSimulatorConfiguration;
    }

    public static class SimulatorAvroMapper extends Mapper<LongWritable, Text, AvroKey<VariantAvro>, NullWritable> {

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();

            String[] valStr = value.toString().split("\t");

            List<Region> regionList = Arrays.asList(new Region(valStr[0], Integer.parseInt(valStr[1]), Integer.parseInt(valStr[2])));

            int numVariants = Integer.parseInt(valStr[3].trim());

            String genotypeFreqs = config.get("genotypeProbabilites");
            Map<String, Double> genotypeFrequencies = new HashMap<>();
            if (genotypeFreqs != null) {
                String[] genotypeFreqsArray = genotypeFreqs.split(",");
                if (genotypeFreqsArray.length == 4) {
                    genotypeFrequencies.put("0/0", Double.parseDouble(genotypeFreqsArray[0]));
                    genotypeFrequencies.put("0/1", Double.parseDouble(genotypeFreqsArray[1]));
                    genotypeFrequencies.put("1/1", Double.parseDouble(genotypeFreqsArray[2]));
                    genotypeFrequencies.put("./.", Double.parseDouble(genotypeFreqsArray[3]));
                }
            }

            org.opencb.biodata.tools.variant.simulator.VariantSimulatorConfiguration variantSimulatorConfiguration =
                    new org.opencb.biodata.tools.variant.simulator.VariantSimulatorConfiguration(regionList, genotypeFrequencies);
            variantSimulatorConfiguration.setNumSamples(Integer.parseInt(config.get("numOfSamples")));
            variantSimulator = new VariantSimulator(variantSimulatorConfiguration);
            //variantSimulator = new VariantSimulator(variantSimulatorConfiguration);
            List<Variant> variants = variantSimulator.simulate(numVariants, variantSimulatorConfiguration.getNumSamples(),
                    regionList);

            for (Variant variant : variants) {
                context.write(new AvroKey<>(variant.getImpl()), NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        conf = new Configuration();
        //define how many numbers of lines need to processed by one mapper
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 200);

        //Set region value if null or if not null take default
        String regionData = args[1].replace("[", "").replace("]", "");
        if (regionData.isEmpty()) {
            regionList = variantSimulatorConfiguration.getRegions();
        } else {
            regionList = new ArrayList<>();
            String[] regionArray = regionData.split(",");
            for (int i = 0; i <= regionArray.length - 1; i++) {
                System.out.println("Entered regions are : " + regionArray[i]);
                regionList.add(Region.parseRegion(regionArray[i].trim()));
            }
        }

        //Calculate number of variants per chunk
        long totalGenomeSize = 0;
        for (Region region : regionList) {
            totalGenomeSize += region.getEnd();
        }
        int numVariantsPerChunk = Math.round(numVariants / (totalGenomeSize / chunkSize));

        // create the input file for Hadoop MR
        String fileName = "mrInput.txt";
        File tmpFile = new File("/tmp/" + fileName);

        //Check if file exist. Delete if exist
        if (tmpFile.exists() && !tmpFile.isDirectory()) {
            System.out.println("File " + tmpFile + " already exist. Deleting file...");
            tmpFile.delete();
            System.out.println("File " + tmpFile + " deleted");
        }

        //Write chromosome, start, end and number of variants per chunk
        PrintWriter printWriter = new PrintWriter(tmpFile);
        for (Region region : regionList) {
            int start = 1;
            int end = chunkSize;
            int numChunks = region.getEnd() / chunkSize;
            for (int i = 0; i < numChunks; i++) {
                printWriter.println(region.getChromosome() + "\t" + String.format("%09d", start) + "\t"
                        + String.format("%09d", end) + "\t" + numVariantsPerChunk);
                start += chunkSize;
                end += chunkSize;
            }
        }
        printWriter.close();

        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path("/tmp/" + fileName);

        //Check if HDFS destination path exist
        Path dstPath = new Path("/home/hpcpal1/simulatorInputFile");
        if (!fs.exists(dstPath)) {
            fs.mkdirs(dstPath);
        }

        //Check if simulator input file already exist, delete if exist
        Path hdfsPathWithFile = new Path("/home/hpcpal1/simulatorInputFile/" + fileName);
        if (fs.exists(hdfsPathWithFile)) {
            System.out.println("Inside hdfsPathWithFile");
            fs.delete(hdfsPathWithFile, true);
        }

        //Copy above generated input file to HSFS
        System.out.println("Source file path is : " + srcPath + ", Destination file path is : " + dstPath);
        fs.copyFromLocalFile(srcPath, dstPath);

        Job job = Job.getInstance(conf, "Simulator");
        job.setJarByClass(VariantSimulatorMR.class);
        AvroJob.setMapOutputKeySchema(job, VariantAvro.getClassSchema());
        AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());
        FileInputFormat.addInputPath(job, new Path(dstPath.toString()));
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.getConfiguration().set("regions", args[1]);
        job.getConfiguration().set("genotypeProbabilites", args[2]);
        job.getConfiguration().set("numOfSamples", args[3]);

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
