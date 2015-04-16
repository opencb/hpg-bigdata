package org.opencb.hpg.bigdata.core.io;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.opencb.commons.io.DataWriter;
import org.opencb.hpg.bigdata.core.utils.CompressionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by hpccoll1 on 02/04/15.
 */
public class AvroFileWriter <T> implements DataWriter<ByteBuffer> {
    protected Logger logger = LoggerFactory.getLogger(this.getClass().toString());

    private final String codecName;
    private final Schema schema;
    private final OutputStream outputStream;
    private final DataFileWriter<T> writer;
    private final DatumWriter<T> datumWriter;
    private int numWrites = 0;

    public AvroFileWriter(Schema schema, String codecName, OutputStream outputStream) {
        this.schema = schema;
        this.outputStream = outputStream;
        this.codecName = codecName;

        datumWriter = new SpecificDatumWriter<>();
        writer = new DataFileWriter<>(datumWriter);
        writer.setCodec(CompressionUtils.getAvroCodec(this.codecName));
    }

    @Override
    public boolean open() {
        try {
            writer.create(schema, outputStream);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }


    @Override
    public boolean write(List<ByteBuffer> batch) {
        for (ByteBuffer byteBuffer : batch) {
            if (numWrites++%1000 == 0) {
                logger.info("Written {} elements", numWrites);
            }
            try {
                writer.appendEncoded(byteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }
        logger.debug("[" + Thread.currentThread().getName() + "] Written " + batch.size());
        return true;
    }

    @Override
    public boolean close(){
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
