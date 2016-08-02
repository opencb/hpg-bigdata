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

package org.opencb.hpg.bigdata.core.parquet;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by hpccoll1 on 05/05/15.
 */
public abstract class ParquetConverter<T extends IndexedRecord> {

    protected CompressionCodecName compressionCodecName;
    protected int rowGroupSize;
    protected int pageSize;

    protected Schema schema;

    public ParquetConverter() {
        compressionCodecName = CompressionCodecName.GZIP;
        rowGroupSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
        pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;
    }

    public ParquetConverter(Schema schema) {
        this();

        this.schema = schema;
    }

    public void toParquet(InputStream inputStream, String outputFile) throws IOException {
        DatumReader<T> datumReader = new GenericDatumReader<>(schema);
        DataFileStream<T> dataFileStream = new DataFileStream<>(inputStream, datumReader);

        ParquetWriter<Object> parquetWriter = AvroParquetWriter.builder(new Path(outputFile))
                .withSchema(schema)
                .withCompressionCodec(compressionCodecName)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(pageSize)
                .build();

        int numRecords = 0;
        T record = null;
        while (dataFileStream.hasNext()) {
            record = dataFileStream.next(record);
                parquetWriter.write(record);

            if (numRecords % 10000 == 0) {
                System.out.println(numRecords);
            }
            numRecords++;
        }

        parquetWriter.close();
    }

}
