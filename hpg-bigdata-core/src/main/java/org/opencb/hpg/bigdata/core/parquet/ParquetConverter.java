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
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.hpg.bigdata.core.io.avro.AvroReader;
import org.opencb.hpg.bigdata.core.utils.FilterTask;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Created by hpccoll1 on 05/05/15.
 */
public abstract class ParquetConverter<T extends IndexedRecord> {

    protected CompressionCodecName compressionCodecName;
    protected int rowGroupSize;
    protected int pageSize;

    protected Schema schema;

    protected List<List<Predicate<T>>> filters;

    public ParquetConverter() {
        this(CompressionCodecName.GZIP, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);
    }

    public ParquetConverter(CompressionCodecName compressionCodecName, int rowGroupSize, int pageSize) {
        this.compressionCodecName = compressionCodecName;
        this.rowGroupSize = rowGroupSize;
        this.pageSize = pageSize;

        filters = new ArrayList<>();
    }

    public boolean filter(T record) {
        for (List<Predicate<T>> list: filters) {
            if (list.size() == 1) {
                if (!list.get(0).test(record)) {
                    return false;
                }
            } else if (list.size() > 1) {
                boolean or = false;
                for (Predicate<T> filter: list) {
                    if (filter.test(record)) {
                        or = true;
                        break;
                    }
                }
                if (!or) {
                    return false;
                }
            }
        }
        return true;
    }

    public ParquetConverter addFilter(Predicate<T> predicate) {
        List<Predicate<T>> list = new ArrayList<>();
        list.add(predicate);
        getFilters().add(list);
        return this;
    }

    public ParquetConverter addFilter(List<Predicate<T>> predicates) {
        return addFilter(predicates, false);
    }

    public ParquetConverter addFilter(List<Predicate<T>> predicates, boolean and) {
        if (and) {
            predicates.forEach(p -> addFilter(p));
        } else {
            getFilters().add(predicates);
        }
        return this;
    }

    public void toParquetFromAvro(InputStream inputStream, String outputFilename) throws IOException {
        DatumReader<T> datumReader = new SpecificDatumReader<>(schema);
        DataFileStream<T> dataFileStream = new DataFileStream<>(inputStream, datumReader);

        AvroParquetWriter parquetWriter =
                new AvroParquetWriter(new Path(outputFilename), schema, compressionCodecName, rowGroupSize, pageSize);

        int numRecords = 0;
        T record = null;
        while (dataFileStream.hasNext()) {
            record = dataFileStream.next(record);

            if (filter(record)) {
                parquetWriter.write(record);

                if (++numRecords % 10000 == 0) {
                    System.out.println("Number of processed records: " + numRecords);
                }
            }
        }

        parquetWriter.close();
        dataFileStream.close();
    }

    public void toParquetFromAvro(InputStream inputStream, String outputFilename, int numThreads) throws IOException {

        // config
        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(numThreads)
                .setBatchSize(100)
                .setSorted(true)
                .build();

        // reader
        DataReader dataReader = new AvroReader<T>(inputStream, schema);

        // writer
        DataWriter dataWriter = new ParquetFileWriter(outputFilename, schema, compressionCodecName,
                rowGroupSize, pageSize);

        // filtering
        Supplier<FilterTask<T>> taskSupplier;
        taskSupplier = () -> new FilterTask<>(filters);

        // parallel task runner
        ParallelTaskRunner<T, T> ptr;
        try {
            ptr = new ParallelTaskRunner<>(dataReader, taskSupplier, dataWriter, config);
        } catch (Exception e) {
            throw new IOException("Error while creating ParallelTaskRunner", e);
        }
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new IOException("Error while converting Avro to Parquet in ParallelTaskRunner", e);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ParquetConverter{");
        sb.append("compressionCodecName=").append(compressionCodecName);
        sb.append(", rowGroupSize=").append(rowGroupSize);
        sb.append(", pageSize=").append(pageSize);
        sb.append(", schema=").append(schema);
        sb.append(", filters=").append(filters);
        sb.append('}');
        return sb.toString();
    }

    public List<List<Predicate<T>>> getFilters() {
        return filters;
    }

    public ParquetConverter setFilters(List<List<Predicate<T>>> filters) {
        this.filters = filters;
        return this;
    }
}
