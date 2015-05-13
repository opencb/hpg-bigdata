package org.opencb.hpg.bigdata.core.io.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.opencb.hpg.bigdata.core.io.ReadAlignment2ParquetMapper;
import org.opencb.hpg.bigdata.core.utils.CompressionUtils;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;

/**
 * Created by hpccoll1 on 05/05/15.
 */
public class ParquetMR {

    private final Schema schema;

    public ParquetMR(Schema schema) {
        this.schema = schema;
    }

    public int run(String input, String output, String codecName) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "ParquetMR");
        job.setJarByClass(this.getClass());

        // point to input data
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, schema);

        // set the output format
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, new Path(output));
        AvroParquetOutputFormat.setSchema(job, schema);
        AvroParquetOutputFormat.setCompression(job, CompressionUtils.getParquetCodec(codecName));
        AvroParquetOutputFormat.setCompressOutput(job, true);

        // set a large block size to ensure a single row group
        AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

        job.setMapperClass(ParquetMapper.class);
        job.setNumReduceTasks(0);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
