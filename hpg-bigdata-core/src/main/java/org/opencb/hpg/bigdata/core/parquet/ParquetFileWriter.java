package org.opencb.hpg.bigdata.core.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.opencb.commons.io.DataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Created by jtarraga on 13/10/16.
 */
public class ParquetFileWriter<T extends GenericRecord> implements DataWriter<T> {

    private AvroParquetWriter parquetWriter;

    protected Logger logger = LoggerFactory.getLogger(this.getClass().toString());

    public ParquetFileWriter(String outputFilename, Schema schema,
                             CompressionCodecName compression, int rowGroupSize, int pageSize) {
        try {
            parquetWriter = new AvroParquetWriter(new Path(outputFilename), schema, compression, rowGroupSize, pageSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public boolean close() {
        try {
            parquetWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean pre() {
        return true;
    }

    @Override
    public boolean post() {
        return true;
    }

    @Override
    public boolean write(T item) {
        return write(Collections.singletonList(item));
    }

    @Override
    public boolean write(List<T> batch) {
        if (batch == null) {
            return true;
        }

        for (T item: batch) {
            try {
                parquetWriter.write(item);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }
}
