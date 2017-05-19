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

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Move to java-common-libs
 *
 * Created by hpccoll1 on 02/04/15.
 */
public class AvroEncoder<T> {

    private final DatumWriter<T> datumWriter;
    private final Encoder encoder;
    private final ByteArrayOutputStream byteArrayOutputStream;
    private int encodeFails = 0;
    private boolean abortOnFail = false;

    protected Logger logger = LoggerFactory.getLogger(AvroEncoder.class);
    private static final int SIZE = 1000000;

    public AvroEncoder(Schema schema) {
        this(schema, true);
    }

    public AvroEncoder(Schema schema, boolean abortOnFail) {
        this.abortOnFail = abortOnFail;
        this.datumWriter = new SpecificDatumWriter<>(schema);
        this.byteArrayOutputStream = new ByteArrayOutputStream(SIZE);    //Initialize with 1MB
        this.encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
    }

    public List<ByteBuffer> encode(List<T> batch) throws IOException {
        List<ByteBuffer> encoded = new ArrayList<>(batch.size());
        for (T elem : batch) {
            try {
                datumWriter.write(elem, encoder);
            } catch (Exception e) {
                if (abortOnFail) {
                    throw e;
                }
                encodeFails++;
                logger.warn("Error encoding element", e);
                encoder.flush();
                byteArrayOutputStream.reset();
                continue;
            }
            encoder.flush();
            encoded.add(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
            byteArrayOutputStream.reset();
        }
        return encoded;
    }

    public int getEncodeFails() {
        return encodeFails;
    }

    public boolean isAbortOnFail() {
        return abortOnFail;
    }
}
