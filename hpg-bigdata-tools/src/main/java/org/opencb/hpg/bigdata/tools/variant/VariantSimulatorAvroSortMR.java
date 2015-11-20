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
 * @author pawan
 */
public class VariantSimulatorAvroSortMR implements Tool {

    private Configuration conf = null;


    private static class SortAvroMapper extends Mapper<AvroKey<VariantAvro>, NullWritable, AvroKey<VariantAvro>,
            AvroValue<VariantAvro>> {
        @Override
        public void map(AvroKey<VariantAvro> key, NullWritable value, Context context) throws
                IOException, InterruptedException {
            //VariantAvro variantAvro = avroKey.datum();
            context.write(key, new AvroValue<>(key.datum()));
        }
    }


    private static class SortAvroReducer extends Reducer<AvroKey<VariantAvro>, AvroValue<VariantAvro>,
            AvroKey<VariantAvro>, NullWritable> {
        @Override
        public void reduce(AvroKey<VariantAvro> key, Iterable<AvroValue<VariantAvro>> values, Context context)
                throws IOException, InterruptedException {

            for (AvroValue<VariantAvro> variantAvro : values) {
                context.write(new AvroKey<>(variantAvro.datum()), NullWritable.get());
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception {

        /*if (args.length != 2) {
            System.err.println("Usage: Data Gen " + "<inputPath>  <OutputPath>");
            return -1;
        }*/

        String input = args[0];
        String output = args[1];
        System.out.println("input " + input);
        System.out.println("output " + output);

        conf = new Configuration();
        //conf.setInt(NLineInputFormat.LINES_PER_MAP, 4000);

        //System.out.println("input 1");
        Job job = Job.getInstance(conf, "Avro Sorting");

        job.setJarByClass(VariantSimulatorAvroSortMR.class);
        job.setMapperClass(SortAvroMapper.class);
        job.setReducerClass(SortAvroReducer.class);

        AvroJob.setInputKeySchema(job, VariantAvro.getClassSchema());
        FileInputFormat.setInputPaths(job, new Path(input));
        job.setInputFormatClass(AvroKeyInputFormat.class);

        AvroJob.setMapOutputKeySchema(job, VariantAvro.getClassSchema());
        AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());

        job.setNumReduceTasks(1);

        //job.setInputFormatClass(NLineInputFormat.class);

        AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());

        AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema());
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

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
