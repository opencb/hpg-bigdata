package org.opencb.hpg.bigdata.core.io;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.ga4gh.models.Variant;
import org.opencb.commons.io.DataWriter;
import org.opencb.hpg.bigdata.core.utils.CompressionUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by hpccoll1 on 02/04/15.
 */
public class AvroFileWriter <T> implements DataWriter<ByteBuffer> {

    private final String codecName;
    private final Schema schema;
    private final OutputStream outputStream;
    private final DataFileWriter<T> writer;
    private final DatumWriter<T> datumWriter;

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
    public boolean pre() {
        return true;
    }

    @Override
    public boolean write(List<ByteBuffer> batch) {
        System.out.println("[" + Thread.currentThread().getName() + "] Writing " + batch.size());
        for (ByteBuffer byteBuffer : batch) {
            try {
                writer.appendEncoded(byteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }
        System.out.println("[" + Thread.currentThread().getName() + "] Written " + batch.size());
        return true;
    }

    @Override
    public boolean post() {
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
