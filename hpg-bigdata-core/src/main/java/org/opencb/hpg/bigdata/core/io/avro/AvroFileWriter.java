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

package org.opencb.hpg.bigdata.core.io.avro;


import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.opencb.commons.io.DataWriter;
import org.opencb.hpg.bigdata.core.utils.AvroUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hpccoll1 on 02/04/15.
 */
public class AvroFileWriter<T> implements DataWriter<ByteBuffer> {

    private final String codecName;
    private final Schema schema;
    private final OutputStream outputStream;
    private final DataFileWriter<T> writer;
    private final DatumWriter<T> datumWriter;
    private int numWrites = 0;

    protected Logger logger = LoggerFactory.getLogger(this.getClass().toString());

    public AvroFileWriter(Schema schema, String codecName, OutputStream outputStream) {
        this.schema = schema;
        this.outputStream = outputStream;
        this.codecName = codecName;

        datumWriter = new SpecificDatumWriter<>();
        writer = new DataFileWriter<>(datumWriter);
        writer.setCodec(AvroUtils.getCodec(this.codecName));
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
            if (numWrites++ % 1000 == 0) {
                logger.debug("Written {} elements", numWrites);
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
    public boolean close() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
