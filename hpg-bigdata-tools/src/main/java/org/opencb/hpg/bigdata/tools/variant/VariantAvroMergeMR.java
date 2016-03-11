package org.opencb.hpg.bigdata.tools.variant;

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
import org.apache.hadoop.util.Tool;
import org.opencb.biodata.models.variant.avro.VariantAvro;

import java.io.IOException;

/**
 * Created by pawan on 17/11/15.
 */
public class VariantAvroMergeMR implements Tool {

    private Configuration conf = null;

    private static class VariantAvroMergeMapper
            extends Mapper<AvroKey<VariantAvro>, NullWritable, AvroKey<VariantAvro>, AvroValue<VariantAvro>> {
        @Override
        protected void map(AvroKey<VariantAvro> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(key, new AvroValue<>(key.datum()));
        }
    }

    private static class VariantAvroMergeReducer
            extends Reducer<AvroKey<VariantAvro>, AvroValue<VariantAvro>, AvroKey<VariantAvro>, NullWritable> {
        @Override
        protected void reduce(AvroKey<VariantAvro> key, Iterable<AvroValue<VariantAvro>> ignore, Context context)
                throws IOException, InterruptedException {
            for (AvroValue<VariantAvro> variantAvro : ignore) {
                context.write(new AvroKey<>(variantAvro.datum()), NullWritable.get());
            }
        }
    }


    @Override
    public int run(final String[] args) throws Exception {

        String input = args[0];
        String output = args[1];
        System.out.println("input " + input);
        System.out.println("output " + output);

        conf = new Configuration();
        Job job = Job.getInstance(conf, "Avro Sorting");

        FileInputFormat.setInputPaths(job, new Path(input));
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, VariantAvro.getClassSchema());

        job.setJarByClass(VariantAvroSortCustomMR.class);
        job.setMapperClass(VariantAvroMergeMapper.class);
        job.setReducerClass(VariantAvroMergeReducer.class);
        job.setNumReduceTasks(1);

        AvroJob.setMapOutputKeySchema(job, VariantAvro.getClassSchema());
        AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());
        AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema());

        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        AvroSort.builder()
                .setJob(job)
                .addPartitionField(VariantAvro.getClassSchema(), "chromosome", true)
                //.addSortField(VariantAvro.getClassSchema(), "chromosome", true)
                //.addSortField(VariantAvro.getClassSchema(), "start", true)
                //.addSortField(VariantAvro.getClassSchema(), "", true)
                //.addGroupField(VariantAvro.getClassSchema(), "", true)
                //.addGroupField(VariantAvro.getClassSchema(), "", true)
                .configure();

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    @Override
    public Configuration getConf() {
        return null;
    }

    @Override
    public void setConf(Configuration arg0) {
    }
}
