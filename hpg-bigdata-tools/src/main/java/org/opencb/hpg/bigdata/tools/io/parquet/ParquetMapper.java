package org.opencb.hpg.bigdata.tools.io.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by hpccoll1 on 05/05/15.
 */
public class ParquetMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Void, GenericRecord> {
    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(null, key.datum());
    }
}