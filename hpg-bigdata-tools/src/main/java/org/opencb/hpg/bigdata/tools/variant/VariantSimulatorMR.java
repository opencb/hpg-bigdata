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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.AvroKey;
//import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.simulator.VariantSimulator;

/**
 * Created by kalyanreddyemani on 06/10/15.
 * @author pawan
 * @author kalyan
 */

public class VariantSimulatorMR extends Configured implements Tool{
    //private static Random rand = new Random();
    private static List<Region> regionsList;
    private static Map<String, Float> genotypeProbabilites;
    private Configuration conf = null;

    private static VariantSimulator variantSimulator;

    public static class SimulatorAvroMapper extends
    Mapper<LongWritable, Text, AvroKey<VariantAvro>, NullWritable> {

        @Override
        protected void setup(
                Context context)
                        throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            String regionsStr = configuration.get("regions");
            String genprobStr = configuration.get("genotypeProbabilites");

            String regionData = regionsStr.replace("[", "").replace("]", "");
            String[] regionArray = regionData.split(",");

            regionsList = new ArrayList<>();
            for (int i = 0; i <= regionArray.length - 1; i++) {
                regionsList.add(Region.parseRegion(regionArray[i]));
            }

            String genProbData = genprobStr.replace("{", "").replace("}", "");
            String[] genProbDataArray = genProbData.split(",");

            genotypeProbabilites = new HashMap<>();
            for (int i = 0; i <= genProbDataArray.length - 1; i++) {
                String[] keyValArray = genProbDataArray[i].split("=");
                String key = keyValArray[0].trim();
                float val = Float.valueOf(keyValArray[1].trim());
                genotypeProbabilites.put(key, val);
            }

            org.opencb.biodata.tools.variant.simulator.Configuration config =
                    new org.opencb.biodata.tools.variant.simulator.Configuration(regionsList, genotypeProbabilites);

            variantSimulator = new VariantSimulator(config);
            super.setup(context);
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws
        IOException, InterruptedException {

            Variant variant = variantSimulator.simulate();
            String[] valStr = value.toString().split(",");

            variant.setChromosome(valStr[0]);
            int position = 0;
            if (valStr[1] != null || !valStr[1].isEmpty()) {
                position = Integer.parseInt(valStr[1].trim());
            }

            int start = position;
            int end = position;
            variant.setStart(start);
            variant.setEnd(end);
            VariantAvro variantAvro = variant.getImpl();
            context.write(new AvroKey<>(variantAvro), NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.println("Usage: Data Gen " + "<inputPath>  <OutputPath>  <regions>  <genotypeProbabilites>");
            return -1;
        }

        String input = args[0];
        String output = args[1];
        String regions = args[2];
        String genProb = args[3];

        conf = new Configuration();
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 500000);
        Job job = Job.getInstance(conf, "Simulator");
        job.setJarByClass(VariantSimulatorMR.class);
        AvroJob.setMapOutputKeySchema(job, VariantAvro.getClassSchema());
        AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        //        List<Region> regionList = Arrays.asList(Region.parseRegion("1"), Region.parseRegion("2:40000-50000"),
        //                Region.parseRegion("3:1000000"));
        //
        //        Map<String, Float> genotypeProbabilites = new HashMap<>();
        //        genotypeProbabilites.put("0/0", 0.7f);
        //        genotypeProbabilites.put("0/1", 0.2f);
        //        genotypeProbabilites.put("1/1", 0.08f);
        //        genotypeProbabilites.put("./.", 0.02f);

        //        String regList = regionList.toString();
        //        String genProb = genotypeProbabilites.toString();

        job.getConfiguration().set("regions", regions);
        job.getConfiguration().set("genotypeProbabilites", genProb);

        job.setInputFormatClass(NLineInputFormat.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setMapperClass(SimulatorAvroMapper.class);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

}
