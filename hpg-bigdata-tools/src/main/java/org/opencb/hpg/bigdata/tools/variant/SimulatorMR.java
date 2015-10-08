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
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;

import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Random;


/**
 * @author Kalyan
 *
 */
public class SimulatorMR
        extends Mapper<LongWritable, Text, Text, AvroValue<Variant>> {

    private int chromosomeSample = 0;
    @Override
    protected void setup(
            Context context)
            throws IOException, InterruptedException {
        chromosomeSample=(int)(Math.random() * 23);
        super.setup(context);
    }
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Variant variant=new Variant();
        variant.setChromosome(Integer.toString(chromosomeSample));
        context.write(value, new AvroValue<>(variant));
    }


     public int run(String[] args) throws Exception {
         System.out.println(args[0]);
         System.out.println(args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Simulator");
        job.setJarByClass(SimulatorMR.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(NLinesInputFormat.class);

         AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema());
         AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());
         job.setOutputFormatClass(AvroKeyOutputFormat.class);

         job.setMapperClass(SimulatorMR.class);
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(AvroValue.class);
       // job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return (job.waitForCompletion(true) ? 0 : 1);    }
}
