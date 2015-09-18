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

package org.opencb.hpg.bigdata.tools.io.parquet;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by hpccoll1 on 05/05/15.
 */
public class ParquetConverter<T extends IndexedRecord> {

    private Schema schema;

    public ParquetConverter(Schema schema) {
        this.schema = schema;
    }

    public void toParquet(InputStream inputStream, String outputFile) throws IOException {
        DatumReader<T> datumReader = new GenericDatumReader<>(schema);
        DataFileStream<T> dataFileStream = new DataFileStream<>(inputStream, datumReader);


        // load your Avro schema
        Schema avroSchema = dataFileStream.getSchema();

        // generate the corresponding parquet schema
        MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);

        // create a WriteSupport object to serialize your Avro objects
        WriteSupport<IndexedRecord> writeSupport = new AvroWriteSupport(parquetSchema, avroSchema);

        // choose compression scheme
        CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;

        // set parquet file block size and page size values
        int blockSize = 256 * 1024 * 1024;
        int pageSize = 64 * 1024;

        // the ParquetWriter object that will consume Avro GenericRecords
//        ParquetWriter parquetWriter = new ParquetWriter(new org.apache.hadoop.fs.Path(outputFile),
//                writeSupport, compressionCodecName, blockSize, pageSize);

        ParquetWriter<IndexedRecord> parquetWriter = new ParquetWriter<>(new org.apache.hadoop.fs.Path(outputFile),
                writeSupport, compressionCodecName, blockSize, pageSize);


        int numRecords = 0;
        T resuse = null;

        while (dataFileStream.hasNext()) {
            resuse = dataFileStream.next(resuse);
            parquetWriter.write(resuse);
            if (numRecords%1000 == 0) {
                System.out.println(numRecords);
            }
            numRecords++;
        }
        parquetWriter.close();

    }
}
