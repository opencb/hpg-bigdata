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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author Kalyan
 *
 */
public class DataSetGeneratorMR
extends Mapper<LongWritable, Text, NullWritable, Text> {
    private Configuration conf;
    private int noOfIterations;
    private Random rand;

    @Override
    protected void setup(
            Context context)
                    throws IOException, InterruptedException {
        rand = new Random();
        Configuration configuration = context.getConfiguration();

        String noOfVariantsStr = configuration.get("noOfVariants");
        noOfIterations = Integer.parseInt(noOfVariantsStr);
        super.setup(context);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        int posStratFrom = 1200320;

        for (int i = 1; i <= noOfIterations; i++) {
            int randInt = rand.nextInt(200 - 100 + 0) + 100;
            int position = posStratFrom + randInt;
            posStratFrom = position;
            context.write(NullWritable.get(), new Text(value + "," + position));
        }
    }


    /**
     * @param args inputPath OutputPath numOfVaraints
     * @return integer value
     * @throws Exception Exception
     */
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Data Gen " + "<inputPath>  <OutputPath>  <numOfVaraints>");
            return -1;
        }
        //Getting the parameters from the command line
        String input = args[0];
        String output = args[1];
        String numVariants = args[2];

        conf = new Configuration();
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 1);
        Job job = Job.getInstance(conf);

        job.getConfiguration().set("noOfVariants", numVariants);

        job.setJarByClass(DataSetGeneratorMR.class);
        job.setMapperClass(DataSetGeneratorMR.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        boolean result = job.waitForCompletion(true);
        if (result) {
            return 0;
        } else {
            return 1;
        }
    }
}
