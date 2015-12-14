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

/**
 * Created by pawan on 19/11/15.
 */

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencb.biodata.models.variant.avro.VariantAvro;

import java.io.IOException;

public class VariantAvroSortCustomMR {

    private Configuration conf = null;

    private static class VariantAvroSortMapper
            extends Mapper<AvroKey<VariantAvro>, NullWritable, AvroKey<VariantAvro>, AvroValue<VariantAvro>> {
        @Override
        protected void map(AvroKey<VariantAvro> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(key, new AvroValue<>(key.datum()));
        }
    }

    private static class VariantAvroSortReducer
            extends Reducer<AvroKey<VariantAvro>, AvroValue<VariantAvro>, AvroKey<VariantAvro>, NullWritable> {
        @Override
        protected void reduce(AvroKey<VariantAvro> key, Iterable<AvroValue<VariantAvro>> ignore, Context context)
                throws IOException, InterruptedException {
            for (AvroValue<VariantAvro> variantAvro : ignore) {
                //WeatherNoIgnore.datum().setCounter(i++);
                context.write(new AvroKey<>(variantAvro.datum()), NullWritable.get());
            }
        }
    }

    public int run(final String[] args) throws Exception {

        String input = args[0];
        String output = args[1];
        System.out.println("input " + input);
        System.out.println("output " + output);

        conf = new Configuration();
        Job job = Job.getInstance(conf, "Avro Sort");

        FileInputFormat.setInputPaths(job, new Path(input));
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, VariantAvro.getClassSchema());

        job.setJarByClass(VariantAvroSortCustomMR.class);
        job.setMapperClass(VariantAvroSortMapper.class);
        job.setReducerClass(VariantAvroSortReducer.class);
        job.setNumReduceTasks(1);

        AvroJob.setMapOutputKeySchema(job, VariantAvro.getClassSchema());
        AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());
        AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema());

        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        AvroSort.builder()
                .setJob(job)
                .addPartitionField(VariantAvro.getClassSchema(), "chromosome", true)
                .addSortField(VariantAvro.getClassSchema(), "chromosome", true)
                .addSortField(VariantAvro.getClassSchema(), "start", true)
                //.addSortField(VariantAvro.getClassSchema(), "", true)
                //.addGroupField(VariantAvro.getClassSchema(), "", true)
                //.addGroupField(VariantAvro.getClassSchema(), "", true)
                .configure();

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
