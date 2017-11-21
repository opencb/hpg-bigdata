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

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.converters.SAMRecordToAvroReadAlignmentBiConverter;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;


/**
 * Created by imedina on 02/08/16.
 */
public class AlignmentParquetConverter extends ParquetConverter<ReadAlignment> {

    private boolean binQualities;

    public AlignmentParquetConverter() {
        this(CompressionCodecName.GZIP, true, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);
    }

    public AlignmentParquetConverter(CompressionCodecName compressionCodecName, boolean binQualities,
                                     int rowGroupSize, int pageSize) {
        super(compressionCodecName, rowGroupSize, pageSize);

        this.binQualities = binQualities;
        this.schema = ReadAlignment.SCHEMA$;
    }

    public void toParquetFromBam(String inputFilename, String outputFilename) throws IOException {

        // reader
        SamReader reader = SamReaderFactory.makeDefault().open(new File(inputFilename));

        // writer
        AvroParquetWriter parquetFileWriter =
                new AvroParquetWriter(new Path(outputFilename), schema, compressionCodecName, rowGroupSize, pageSize);

        // converter
        SAMRecordToAvroReadAlignmentBiConverter converter = new SAMRecordToAvroReadAlignmentBiConverter(binQualities);

        // main loop
        long counter = 0;
        SAMRecordIterator iterator = reader.iterator();
        while (iterator.hasNext()) {
            SAMRecord record = iterator.next();
            ReadAlignment readAlignment = converter.to(record);
            if (filter(readAlignment)) {
                parquetFileWriter.write(readAlignment);
                counter++;
            }
        }
        System.out.println("Number of processed records: " + counter);

        // save the SAM header in a separated file
        PrintWriter pwriter = new PrintWriter(new FileWriter(outputFilename + ".header"));
        pwriter.write(reader.getFileHeader().getTextHeader());
        pwriter.close();

        // close
        reader.close();
        parquetFileWriter.close();
    }

    public AlignmentParquetConverter addRegionFilter(Region region) {
        addFilter(a -> a.getAlignment() != null
                && a.getAlignment().getPosition() != null
                && a.getAlignment().getPosition().getReferenceName().equals(region.getChromosome())
                && a.getAlignment().getPosition().getPosition() <= region.getEnd()
                && (a.getAlignment().getPosition().getPosition() + a.getAlignedSequence().length())
                >= region.getStart());
        return this;
    }

    public AlignmentParquetConverter addRegionFilter(List<Region> regions, boolean and) {
        List<Predicate<ReadAlignment>> predicates = new ArrayList<>();
        regions.forEach(r -> predicates.add(a -> a.getAlignment() != null
                && a.getAlignment().getPosition() != null
                && a.getAlignment().getPosition().getReferenceName().equals(r.getChromosome())
                && a.getAlignment().getPosition().getPosition() <= r.getEnd()
                && (a.getAlignment().getPosition().getPosition() + a.getAlignedSequence().length())
                >= r.getStart()));
        addFilter(predicates, and);
        return this;
    }

    public AlignmentParquetConverter addMinMapQFilter(int quality) {
        addFilter(a -> a.getAlignment() != null
                && a.getAlignment().getMappingQuality() >= quality);
        return this;
    }

}
